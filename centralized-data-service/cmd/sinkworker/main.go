// Command sinkworker runs the v1.25 Debezium -> cdc_internal pipeline in a
// standalone process. It is launched in PARALLEL with the legacy worker
// binary (cmd/worker) and does not share any runtime state. See plan v7.2
// §10 (Parallel Independence Contract).
package main

import (
	"context"
	"errors"
	"log"
	"os"
	"os/signal"
	"regexp"
	"strings"
	"syscall"
	"time"

	"centralized-data-service/config"
	"centralized-data-service/internal/sinkworker"
	"centralized-data-service/pkgs/database"
	"centralized-data-service/pkgs/idgen"
	"centralized-data-service/pkgs/natsconn"

	"github.com/nats-io/nats.go"
	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

const (
	// group ("cdc-worker-group") so we never compete for partitions.
	consumerGroup = "cdc-v125-sink-worker"
	// topicPattern matches all Debezium MongoDB topics emitted by the
	// goopay-mongodb-cdc connector. kafka-go supports regex subscription
	// via GroupTopicsRegex since v0.4.42.
	topicPattern = `^cdc\.goopay\..*`
)

func main() {
	cfg, err := config.NewConfig()
	if err != nil {
		log.Fatalf("load config: %v", err)
	}

	logger, _ := zap.NewProduction()
	if cfg.Server.Mode == "debug" {
		logger, _ = zap.NewDevelopment()
	}
	defer logger.Sync()

	logger.Info("sinkworker starting",
		zap.Strings("brokers", cfg.Kafka.Brokers),
		zap.String("group", consumerGroup),
		zap.String("topicPattern", topicPattern),
	)

	db, err := database.NewPostgresConnection(cfg)
	if err != nil {
		logger.Fatal("postgres connect", zap.Error(err))
	}

	// Claim a machine_id and fencing_token via Phase 0's Postgres function.
	// The returned machine_id feeds Sonyflake so the identities produced
	// by this pod never collide with any other pod's IDs.
	hostname, _ := os.Hostname()
	var claim struct {
		OutMachineID    int   `gorm:"column:out_machine_id"`
		OutFencingToken int64 `gorm:"column:out_fencing_token"`
	}
	if err := db.Raw(`SELECT * FROM cdc_internal.claim_machine_id(?, ?)`,
		hostname, os.Getpid(),
	).Scan(&claim).Error; err != nil {
		logger.Fatal("claim_machine_id", zap.Error(err))
	}
	logger.Info("claimed machine_id",
		zap.Int("machineID", claim.OutMachineID),
		zap.Int64("fencingToken", claim.OutFencingToken),
		zap.String("hostname", hostname),
		zap.Int("pid", os.Getpid()),
	)

	if err := idgen.InitWithMachineID(uint16(claim.OutMachineID)); err != nil {
		logger.Fatal("idgen init", zap.Error(err))
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Heartbeat: every 30s refresh claim — if the DB says our fencing
	// token has been reclaimed, we *must* self-terminate (lesson #72).
	go runHeartbeat(ctx, db, claim.OutMachineID, claim.OutFencingToken, logger, cancel)

	// NATS client so the post-ingest hook can fire Transmute triggers.
	// Failure to connect is non-fatal — sink continues, Transmuter kicks in
	// only on schedule + manual trigger.
	var natsConn *nats.Conn
	if nc, err := natsconn.NewNatsClient(cfg, logger); err == nil {
		natsConn = nc.Conn
		logger.Info("sinkworker nats connected — post-ingest Transmute hook armed")
	} else {
		logger.Warn("sinkworker nats connect failed — post-ingest hook disabled", zap.Error(err))
	}

	schemaMgr := sinkworker.NewSchemaManager(db, logger)
	sw := sinkworker.New(sinkworker.Config{
		DB:                db,
		SchemaManager:     schemaMgr,
		SchemaRegistryURL: cfg.Kafka.SchemaRegistryURL,
		MachineID:         claim.OutMachineID,
		FencingToken:      claim.OutFencingToken,
		Logger:            logger,
		NATSConn:          natsConn,
	})

	// kafka-go v0.4 only supports explicit topic lists for consumer groups
	// (no regex), so we resolve the pattern once at startup via cluster
	// metadata. A restart is required if new topics appear later — acceptable
	// trade-off for Phase 1 (Debezium adds topics only when a new Mongo
	// collection is enabled, which is infrequent).
	topics, err := discoverTopics(ctx, cfg.Kafka.Brokers, topicPattern)
	if err != nil {
		logger.Fatal("discover topics", zap.Error(err))
	}
	if len(topics) == 0 {
		logger.Warn("no topics match pattern yet — sinkworker will idle until they appear",
			zap.String("pattern", topicPattern))
	}
	logger.Info("subscribed topics", zap.Strings("topics", topics))

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:           cfg.Kafka.Brokers,
		GroupID:           consumerGroup,
		GroupTopics:       topics,
		MinBytes:          1,
		MaxBytes:          10 << 20, // 10 MiB
		MaxWait:           500 * time.Millisecond,
		CommitInterval:    0, // manual commits after successful handle
		HeartbeatInterval: 3 * time.Second,
		SessionTimeout:    30 * time.Second,
	})
	defer reader.Close()

	// Graceful shutdown plumbing.
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	go func() {
		s := <-sig
		logger.Info("shutdown signal received", zap.String("signal", s.String()))
		cancel()
	}()

	logger.Info("sinkworker ready — consuming", zap.String("group", consumerGroup))

	for {
		msg, err := reader.FetchMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				logger.Info("fetch stopped by ctx cancel")
				return
			}
			if strings.Contains(err.Error(), "reader closed") {
				return
			}
			logger.Error("kafka fetch error", zap.Error(err))
			time.Sleep(1 * time.Second)
			continue
		}

		if err := sw.HandleMessage(ctx, msg); err != nil {
			// Skip-poison-pill: log and DO NOT commit, so the message is
			// re-delivered on next poll. The DLQ migration is Phase 2
			// territory — for Phase 1 we stay loud in logs.
			logger.Error("handle message failed — not committing",
				zap.String("topic", msg.Topic),
				zap.Int("partition", msg.Partition),
				zap.Int64("offset", msg.Offset),
				zap.Error(err),
			)
			// Back off a touch to avoid hot loop when error is persistent.
			time.Sleep(250 * time.Millisecond)
			continue
		}

		if err := reader.CommitMessages(ctx, msg); err != nil {
			logger.Error("commit offset failed",
				zap.String("topic", msg.Topic),
				zap.Int64("offset", msg.Offset),
				zap.Error(err),
			)
		}
	}
}

// discoverTopics asks the Kafka cluster for its full topic list and
// filters to the caller's regex. We use a short-lived kafka.Client (not
// the reader) because the reader only sees topics we hand it up-front.
func discoverTopics(ctx context.Context, brokers []string, pattern string) ([]string, error) {
	if len(brokers) == 0 {
		return nil, errors.New("no brokers configured")
	}
	re, err := regexp.Compile(pattern)
	if err != nil {
		return nil, err
	}
	client := &kafka.Client{Addr: kafka.TCP(brokers...), Timeout: 5 * time.Second}
	resp, err := client.Metadata(ctx, &kafka.MetadataRequest{})
	if err != nil {
		return nil, err
	}
	seen := make(map[string]struct{})
	var out []string
	for _, t := range resp.Topics {
		if t.Error != nil {
			continue
		}
		if strings.HasPrefix(t.Name, "_") {
			continue // internal topics (__consumer_offsets, _schemas, ...)
		}
		if _, dup := seen[t.Name]; dup {
			continue
		}
		if re.MatchString(t.Name) {
			seen[t.Name] = struct{}{}
			out = append(out, t.Name)
		}
	}
	return out, nil
}

// runHeartbeat keeps cdc_internal.worker_registry fresh. The Postgres
// function returns FALSE if our fencing_token has been reclaimed by
// another pod — we translate that into fail-stop behaviour via cancel().
func runHeartbeat(
	ctx context.Context,
	db *gorm.DB,
	machineID int,
	fencingToken int64,
	logger *zap.Logger,
	cancel context.CancelFunc,
) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			var alive bool
			if err := db.Raw(
				`SELECT cdc_internal.heartbeat_machine_id(?, ?)`,
				machineID, fencingToken,
			).Scan(&alive).Error; err != nil {
				logger.Warn("heartbeat query error", zap.Error(err))
				continue
			}
			if !alive {
				logger.Error("FENCING: token reclaimed by another pod — self-terminating",
					zap.Int("machineID", machineID),
					zap.Int64("fencingToken", fencingToken),
				)
				cancel()
				return
			}
		}
	}
}
