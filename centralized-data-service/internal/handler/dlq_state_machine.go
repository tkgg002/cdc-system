package handler

import (
	"context"
	"fmt"
	"time"

	"centralized-data-service/internal/model"
	"centralized-data-service/pkgs/natsconn"

	"go.uber.org/zap"
	"gorm.io/gorm"
)

type DLQStateMachineConfig struct {
	PollInterval time.Duration
	BatchSize    int
	MaxRetries   int
	QueryTimeout time.Duration
}

func (c *DLQStateMachineConfig) applyDefaults() {
	if c.PollInterval <= 0 {
		c.PollInterval = 5 * time.Minute
	}
	if c.BatchSize <= 0 {
		c.BatchSize = 100
	}
	if c.MaxRetries <= 0 {
		c.MaxRetries = MaxRetries
	}
	if c.QueryTimeout <= 0 {
		c.QueryTimeout = 30 * time.Second
	}
}

// DLQStateMachine polls failed_sync_logs through idx_fsl_retry_poll and
// replays the original payload back to its original subject.
type DLQStateMachine struct {
	db     *gorm.DB
	nats   *natsconn.NatsClient
	logger *zap.Logger
	cfg    DLQStateMachineConfig
}

func NewDLQStateMachine(
	db *gorm.DB,
	nats *natsconn.NatsClient,
	cfg DLQStateMachineConfig,
	logger *zap.Logger,
) *DLQStateMachine {
	cfg.applyDefaults()
	return &DLQStateMachine{
		db:     db,
		nats:   nats,
		logger: logger,
		cfg:    cfg,
	}
}

func (sm *DLQStateMachine) Start(ctx context.Context) {
	sm.logInfo("dlq state machine started",
		zap.Duration("poll_interval", sm.cfg.PollInterval),
		zap.Int("batch_size", sm.cfg.BatchSize),
		zap.Int("max_retries", sm.cfg.MaxRetries),
	)

	sm.RunOnce(ctx)

	ticker := time.NewTicker(sm.cfg.PollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			sm.logInfo("dlq state machine stopped")
			return
		case <-ticker.C:
			sm.RunOnce(ctx)
		}
	}
}

func (sm *DLQStateMachine) RunOnce(ctx context.Context) {
	if sm.db == nil || sm.nats == nil || sm.nats.Conn == nil {
		sm.logWarn("dlq state machine skipped due to missing dependency")
		return
	}

	cycleCtx, cancel := context.WithTimeout(ctx, sm.cfg.QueryTimeout)
	defer cancel()

	var rows []model.FailedSyncLog
	err := sm.db.WithContext(cycleCtx).
		Raw(`SELECT * FROM failed_sync_logs
			 WHERE status IN ('pending','failed','retrying')
			   AND (next_retry_at IS NULL OR next_retry_at <= NOW())
			   AND retry_count < ?
			 ORDER BY next_retry_at NULLS FIRST, id
			 LIMIT ?`,
			sm.cfg.MaxRetries, sm.cfg.BatchSize,
		).Scan(&rows).Error
	if err != nil {
		sm.logWarn("dlq state machine poll failed", zap.Error(err))
		return
	}

	for _, row := range rows {
		if ctx.Err() != nil {
			return
		}
		sm.retryOne(ctx, row)
	}
}

func (sm *DLQStateMachine) retryOne(ctx context.Context, row model.FailedSyncLog) {
	now := time.Now().UTC()
	if err := sm.db.WithContext(ctx).Exec(
		`UPDATE failed_sync_logs
		    SET status='retrying', last_retry_at=?
		  WHERE id=?`,
		now, row.ID,
	).Error; err != nil {
		sm.logWarn("dlq state machine mark retrying failed",
			zap.Uint64("id", row.ID),
			zap.Error(err),
		)
		return
	}

	retryCount := row.RetryCount + 1
	subject := row.KafkaTopic
	if subject == "" {
		subject = DLQSubject
	}

	publishErr := sm.nats.Conn.Publish(subject, row.RawJSON)
	if publishErr == nil {
		if err := sm.db.WithContext(ctx).Exec(
			`UPDATE failed_sync_logs
			    SET status='resolved', retry_count=?, resolved_at=?, next_retry_at=NULL, last_error=NULL
			  WHERE id=?`,
			retryCount, now, row.ID,
		).Error; err != nil {
			sm.logWarn("dlq state machine resolve update failed",
				zap.Uint64("id", row.ID),
				zap.Error(err),
			)
		}
		sm.logInfo("dlq state machine replayed message",
			zap.Uint64("id", row.ID),
			zap.String("subject", subject),
			zap.Int("retry_count", retryCount),
		)
		return
	}

	errMsg := truncateDLQError(publishErr.Error(), 2000)
	if retryCount >= sm.cfg.MaxRetries {
		if err := sm.db.WithContext(ctx).Exec(
			`UPDATE failed_sync_logs
			    SET status='dead_letter', retry_count=?, next_retry_at=NULL, last_error=?
			  WHERE id=?`,
			retryCount, errMsg, row.ID,
		).Error; err != nil {
			sm.logWarn("dlq state machine dead_letter update failed",
				zap.Uint64("id", row.ID),
				zap.Error(err),
			)
		}
		sm.logWarn("dlq state machine replay exhausted",
			zap.Uint64("id", row.ID),
			zap.String("subject", subject),
			zap.Error(publishErr),
		)
		return
	}

	nextRetryAt := now.Add(nextReplayDelay(retryCount))
	if err := sm.db.WithContext(ctx).Exec(
		`UPDATE failed_sync_logs
		    SET status='retrying', retry_count=?, next_retry_at=?, last_error=?
		  WHERE id=?`,
		retryCount, nextRetryAt, errMsg, row.ID,
	).Error; err != nil {
		sm.logWarn("dlq state machine retry schedule update failed",
			zap.Uint64("id", row.ID),
			zap.Error(err),
		)
	}
	sm.logWarn("dlq state machine scheduled replay retry",
		zap.Uint64("id", row.ID),
		zap.String("subject", subject),
		zap.Int("retry_count", retryCount),
		zap.Time("next_retry_at", nextRetryAt),
		zap.Error(publishErr),
	)
}

func nextReplayDelay(retryCount int) time.Duration {
	switch retryCount {
	case 1:
		return 1 * time.Minute
	case 2:
		return 5 * time.Minute
	case 3:
		return 30 * time.Minute
	case 4:
		return 2 * time.Hour
	default:
		return 6 * time.Hour
	}
}

func (sm *DLQStateMachine) logInfo(msg string, fields ...zap.Field) {
	if sm.logger != nil {
		sm.logger.Info(msg, fields...)
	}
}

func (sm *DLQStateMachine) logWarn(msg string, fields ...zap.Field) {
	if sm.logger != nil {
		sm.logger.Warn(msg, fields...)
	}
}

func (sm *DLQStateMachine) ReplayFailedLog(ctx context.Context, id uint64) error {
	if sm.db == nil {
		return fmt.Errorf("database not configured")
	}
	var row model.FailedSyncLog
	if err := sm.db.WithContext(ctx).Where("id = ?", id).First(&row).Error; err != nil {
		return err
	}
	sm.retryOne(ctx, row)
	return nil
}
