package service

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/robfig/cron/v3"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

// TransmuteScheduler polls cdc_internal.transmute_schedule every `interval`
// and enqueues due rows by publishing cdc.cmd.transmute. Concurrency-safe
// across multiple instances via:
//
//  1. SELECT ... FOR UPDATE SKIP LOCKED inside a transaction → at most
//     one scheduler instance claims a given row per tick.
//  2. Fencing: each tick SET LOCAL cdc_internal session vars with the
//     scheduler's current machine_id + fencing_token. If the DB says
//     a newer token exists → the cdc_internal fencing triggers RAISE
//     EXCEPTION and the whole tick rolls back (lesson #73).
//
// The cron parser is robfig/cron/v3 with 5-field classic crontab syntax.
type TransmuteScheduler struct {
	db           *gorm.DB
	nats         *nats.Conn
	logger       *zap.Logger
	parser       cron.Parser
	interval     time.Duration
	machineID    int
	fencingToken int64
	stopCh       chan struct{}
}

func NewTransmuteScheduler(
	db *gorm.DB,
	nats *nats.Conn,
	logger *zap.Logger,
	machineID int,
	fencingToken int64,
) *TransmuteScheduler {
	return &TransmuteScheduler{
		db:     db,
		nats:   nats,
		logger: logger,
		parser: cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow),
		interval:     60 * time.Second,
		machineID:    machineID,
		fencingToken: fencingToken,
		stopCh:       make(chan struct{}),
	}
}

// Start drives the poll loop until ctx is cancelled or Stop is called.
func (s *TransmuteScheduler) Start(ctx context.Context) {
	s.logger.Info("transmute scheduler started",
		zap.Duration("interval", s.interval),
		zap.Int("machine_id", s.machineID),
		zap.Int64("fencing_token", s.fencingToken))

	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()

	s.tick(ctx)

	for {
		select {
		case <-ticker.C:
			s.tick(ctx)
		case <-ctx.Done():
			s.logger.Info("transmute scheduler stopped (ctx done)")
			return
		case <-s.stopCh:
			s.logger.Info("transmute scheduler stopped")
			return
		}
	}
}

// Stop signals the loop to exit on next tick.
func (s *TransmuteScheduler) Stop() {
	select {
	case <-s.stopCh:
	default:
		close(s.stopCh)
	}
}

// tick claims due rows atomically, publishes cdc.cmd.transmute for each,
// then recomputes next_run_at from cron_expr.
func (s *TransmuteScheduler) tick(ctx context.Context) {
	claimed := 0
	start := time.Now()
	err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		// Plant fencing session vars so any cdc_internal trigger attached
		// to the future master_table_registry row can verify this
		// scheduler instance still owns machine_id.
		if err := tx.Exec(
			`SELECT set_config('app.fencing_machine_id', ?, true), set_config('app.fencing_token', ?, true)`,
			fmt.Sprintf("%d", s.machineID),
			fmt.Sprintf("%d", s.fencingToken),
		).Error; err != nil {
			return fmt.Errorf("set fencing: %w", err)
		}

		rows, err := tx.Raw(
			`SELECT id, master_table, cron_expr
			   FROM cdc_internal.transmute_schedule
			  WHERE is_enabled = true
			    AND mode = 'cron'
			    AND (next_run_at IS NULL OR next_run_at <= NOW())
			  FOR UPDATE SKIP LOCKED
			  LIMIT 10`,
		).Rows()
		if err != nil {
			return fmt.Errorf("claim due: %w", err)
		}
		defer rows.Close()

		type due struct {
			id       int64
			master   string
			cronExpr string
		}
		dues := make([]due, 0, 10)
		for rows.Next() {
			var d due
			if err := rows.Scan(&d.id, &d.master, &d.cronExpr); err != nil {
				return fmt.Errorf("scan: %w", err)
			}
			dues = append(dues, d)
		}

		now := time.Now().UTC()
		for _, d := range dues {
			// Mark running + fire.
			_ = tx.Exec(
				`UPDATE cdc_internal.transmute_schedule
				   SET last_status='running', last_run_at=?, updated_at=NOW()
				 WHERE id=?`, now, d.id).Error

			payload, _ := json.Marshal(map[string]any{
				"master_table":   d.master,
				"triggered_by":   "scheduler",
				"correlation_id": fmt.Sprintf("sched-%d-%d", d.id, now.UnixNano()),
			})
			if s.nats != nil {
				if err := s.nats.Publish("cdc.cmd.transmute", payload); err != nil {
					s.logger.Warn("scheduler publish failed",
						zap.Int64("id", d.id),
						zap.String("master", d.master),
						zap.Error(err))
					_ = tx.Exec(
						`UPDATE cdc_internal.transmute_schedule
						   SET last_status='failed', last_error=?, updated_at=NOW()
						 WHERE id=?`, err.Error(), d.id).Error
					continue
				}
			}

			// Compute next_run_at.
			schedule, perr := s.parser.Parse(d.cronExpr)
			if perr != nil {
				s.logger.Warn("scheduler parse cron failed",
					zap.Int64("id", d.id), zap.String("expr", d.cronExpr), zap.Error(perr))
				_ = tx.Exec(
					`UPDATE cdc_internal.transmute_schedule
					   SET last_status='failed', last_error=?, updated_at=NOW()
					 WHERE id=?`, "cron parse: "+perr.Error(), d.id).Error
				continue
			}
			next := schedule.Next(now)
			if err := tx.Exec(
				`UPDATE cdc_internal.transmute_schedule
				   SET next_run_at=?, updated_at=NOW()
				 WHERE id=?`, next, d.id).Error; err != nil {
				return fmt.Errorf("update next_run_at: %w", err)
			}
			claimed++
		}
		return nil
	})

	if err != nil {
		s.logger.Warn("scheduler tick failed", zap.Error(err), zap.Duration("elapsed", time.Since(start)))
		return
	}
	if claimed > 0 {
		s.logger.Info("scheduler tick dispatched",
			zap.Int("count", claimed),
			zap.Duration("elapsed", time.Since(start)))
	}
}
