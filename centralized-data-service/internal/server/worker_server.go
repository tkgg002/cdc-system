package server

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"centralized-data-service/config"
	"centralized-data-service/internal/handler"
	"centralized-data-service/internal/model"
	"centralized-data-service/internal/repository"
	"centralized-data-service/internal/service"
	"centralized-data-service/pkgs/database"
	"centralized-data-service/pkgs/mongodb"
	"centralized-data-service/pkgs/natsconn"

	"go.mongodb.org/mongo-driver/mongo"
	"centralized-data-service/pkgs/rediscache"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/adaptor"
	"github.com/gofiber/fiber/v2/middleware/cors"
	"github.com/nats-io/nats.go"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

type WorkerServer struct {
	cfg              *config.AppConfig
	logger           *zap.Logger
	db               *gorm.DB
	dbReplica        *gorm.DB // may == db when no replica DSN configured
	nats             *natsconn.NatsClient
	redis            *rediscache.RedisCache
	consumerPool     *handler.ConsumerPool
	batchBuffer      *handler.BatchBuffer
	eventHandler     *handler.EventHandler
	registrySvc      *service.RegistryService
	registryRepo     *repository.RegistryRepo
	activityLogger   *service.ActivityLogger
	reconCore        *service.ReconCore
	reconHealer      *service.ReconHealer
	schemaValidator  *service.SchemaValidator
	dlqWorker        *service.DLQWorker
	partitionDropper *service.PartitionDropper
	tsDetector       *service.TimestampDetector     // Migration 017 — auto-detect
	fullCountAgg     *service.FullCountAggregator   // Migration 017 — full-count daily
	mongoClient      *mongo.Client
	app              *fiber.App
}

func NewWorkerServer(cfg *config.AppConfig, logger *zap.Logger) (*WorkerServer, error) {
	// 1. Connect PostgreSQL (primary — owns writes).
	db, err := database.NewPostgresConnection(cfg)
	if err != nil {
		return nil, fmt.Errorf("postgres: %w", err)
	}
	logger.Info("PostgreSQL connected")

	// 1b. Optional read-replica pool (plan WORKER task #2). When the
	// replica DSN is not set we reuse the primary connection but the
	// Recon dest agent still wraps every read in SET TRANSACTION
	// READ ONLY as defence-in-depth.
	dbReplica, err := database.NewPostgresReadReplica(cfg)
	if err != nil {
		logger.Warn("postgres read replica init failed, reusing primary",
			zap.Error(err),
		)
		dbReplica = nil
	}
	if dbReplica == nil {
		dbReplica = db
		logger.Info("postgres read-replica not configured, reusing primary with SET TRANSACTION READ ONLY")
	} else {
		logger.Info("postgres read-replica connected")
	}

	// Schema managed via SQL migrations (migrations/001-013) — NOT auto-migrated.
	// Reason: GORM AutoMigrate conflicts with partitioned tables (e.g. cdc_activity_log
	// has composite PRIMARY KEY (created_at, id) for RANGE partitioning — GORM tries
	// to DROP NOT NULL on created_at which Postgres rejects with SQLSTATE 42P16).
	// Tables & owning migrations:
	//   - cdc_table_registry      -> 001_init_schema.sql, 013_table_registry_expected_fields.sql
	//   - cdc_mapping_rules       -> 001_init_schema.sql
	//   - cdc_activity_log        -> 006_activity_log.sql, 010_partitioning.sql
	//   - cdc_worker_schedule     -> 007_worker_schedule.sql
	//   - cdc_reconciliation_*    -> 008_reconciliation.sql
	//   - cdc_failed_sync_logs    -> 008_reconciliation.sql, 012_dlq_state_machine.sql
	//   - cdc_recon_runs          -> 011_recon_runs.sql

	// 2. Connect NATS
	natsClient, err := natsconn.NewNatsClient(cfg, logger)
	if err != nil {
		return nil, fmt.Errorf("nats: %w", err)
	}

	// Ensure streams exist
	if err := natsClient.EnsureStreams(); err != nil {
		return nil, fmt.Errorf("nats streams: %w", err)
	}
	logger.Info("NATS streams ready")

	// 3. Connect Redis
	redisCache, err := rediscache.NewRedisCache(cfg, logger)
	if err != nil {
		return nil, fmt.Errorf("redis: %w", err)
	}

	// 4. Repositories
	registryRepo := repository.NewRegistryRepo(db)
	mappingRepo := repository.NewMappingRuleRepo(db)
	pendingRepo := repository.NewPendingFieldRepo(db)

	// 5. Services
	registrySvc := service.NewRegistryService(registryRepo, mappingRepo, logger)
	schemaInspector := service.NewSchemaInspector(pendingRepo, redisCache, natsClient, logger)

	// 6. Batch buffer
	schemaAdapter := service.NewSchemaAdapter(db, logger)
	batchBuffer := handler.NewBatchBuffer(cfg.Worker.BatchSize, cfg.Worker.BatchTimeout, db, schemaAdapter, logger)

	// 6b. MongoDB + Reconciliation Core (plan WORKER tasks #2, #3).
	// - ReconDestAgent uses the dedicated replica pool when configured.
	// - ReconCoreWithConfig + Redis client enables leader election so
	//   scheduled CheckAll runs on exactly one worker instance.
	var reconCore *service.ReconCore
	var mongoClientShared *mongo.Client
	if cfg.MongoDB.URL != "" {
		mc, err := mongodb.NewClient(context.Background(), mongodb.MongoConfig{URL: cfg.MongoDB.URL}, logger)
		if err != nil {
			logger.Warn("MongoDB connection failed, reconciliation disabled", zap.Error(err))
		} else {
			mongoClientShared = mc
			sourceAgent := service.NewReconSourceAgent(mc, logger)
			destAgent := service.NewReconDestAgentWithConfig(
				db,
				dbReplica,
				service.ReconDestAgentConfig{ReadReplicaDSN: cfg.DB.ReadReplicaDSN},
				logger,
			)
			reconCore = service.NewReconCoreWithConfig(
				sourceAgent, destAgent, db, mc, schemaAdapter, registryRepo,
				redisCache,
				service.ReconCoreConfig{}, // defaults; InstanceID derived from hostname+uuid
				logger,
			)
			logger.Info("Reconciliation Core initialized (replica + leader election)")
		}
	}

	// 6c. Schema validator (Phase A — JSON converter). Runs inside the
	// Kafka consumer BEFORE message processing so drift rejects reach
	// the DLQ via the write-before-ACK path.
	schemaValidator := service.NewSchemaValidator(db, logger)

	// 6d. DLQ retry worker. Runs alongside the consumer, polling
	// failed_sync_logs every 5m and applying exponential backoff.
	dlqWorker := service.NewDLQWorker(db, mongoClientShared, schemaAdapter, service.DLQWorkerConfig{}, logger)
	// 7. Dynamic Mapper + Event handler
	dynamicMapper := service.NewDynamicMapper(registrySvc, logger)
	eventHandler := handler.NewEventHandler(db, registrySvc, dynamicMapper, schemaInspector, batchBuffer, logger)

	// 8. Consumer pool
	consumerPool, err := handler.NewConsumerPool(
		natsClient.JS,
		"cdc.goopay.>",
		"cdc-worker-group",
		eventHandler.Handle,
		cfg.Worker.PoolSize,
		logger,
	)
	if err != nil {
		return nil, fmt.Errorf("consumer pool: %w", err)
	}

	// 9. Config reload listener — parse user context from CMS
	natsClient.Conn.Subscribe("schema.config.reload", func(msg *nats.Msg) {
		var reloadEvent struct {
			Table     string `json:"table"`
			UserID    string `json:"user_id"`
			Action    string `json:"action"`
			Field     string `json:"field"`
			Timestamp string `json:"timestamp"`
		}
		if err := json.Unmarshal(msg.Data, &reloadEvent); err != nil {
			// Backward compat: plain table name string
			logger.Info("config reload triggered", zap.String("table", string(msg.Data)))
		} else {
			logger.Info("config reload triggered by user",
				zap.String("table", reloadEvent.Table),
				zap.String("user_id", reloadEvent.UserID),
				zap.String("action", reloadEvent.Action),
				zap.String("field", reloadEvent.Field),
			)
		}
		registrySvc.ReloadAll(context.Background())
		redisCache.DeletePattern(context.Background(), "schema:*")
	})

	// 10a. pgx pool for high-throughput operations
	pgxPool, err := database.NewPgxPool(context.Background(), cfg)
	if err != nil {
		logger.Warn("pgx pool init failed, batch bridge disabled", zap.Error(err))
	}

	// 10b. Command handler — handles DW operations relayed via NATS from the API.
	cmdHandler := handler.NewCommandHandler(db, mappingRepo, registryRepo, pendingRepo, logger)
	// Inject Kafka Connect URL so boundary-refactor handlers (restart,
	// sync-state) can call the Connect REST API without leaking through
	// the CMS. Empty string keeps those handlers idle.
	cmdHandler.SetKafkaConnectURL(cfg.Debezium.KafkaConnectURL)
	cmdHandler.SetNATSConn(natsClient.Conn)
	natsClient.Conn.Subscribe("cdc.cmd.standardize", cmdHandler.HandleStandardize)
	natsClient.Conn.Subscribe("cdc.cmd.discover", cmdHandler.HandleDiscover)
	natsClient.Conn.Subscribe("cdc.cmd.backfill", cmdHandler.HandleBackfill)
	natsClient.Conn.Subscribe("cdc.cmd.scan-raw-data", cmdHandler.HandleScanRawData)
	natsClient.Conn.Subscribe("cdc.cmd.batch-transform", cmdHandler.HandleBatchTransform)
	natsClient.Conn.Subscribe("cdc.cmd.periodic-scan", cmdHandler.HandlePeriodicScan)
	natsClient.Conn.Subscribe("cdc.cmd.drop-gin-index", cmdHandler.HandleDropGINIndex)
	natsClient.Conn.Subscribe("cdc.cmd.create-default-columns", cmdHandler.HandleCreateDefaultColumns)
	// Boundary-refactor handlers — kept for Debezium-native flows.
	natsClient.Conn.Subscribe("cdc.cmd.scan-fields", cmdHandler.HandleScanFields)
	natsClient.Conn.Subscribe("cdc.cmd.sync-register", cmdHandler.HandleSyncRegister)
	natsClient.Conn.Subscribe("cdc.cmd.sync-state", cmdHandler.HandleSyncState)
	natsClient.Conn.Subscribe("cdc.cmd.restart-debezium", cmdHandler.HandleRestartDebezium)
	natsClient.Conn.Subscribe("cdc.cmd.alter-column", cmdHandler.HandleAlterColumn)

	// Transmuter wiring (plan v2 §R6). TypeResolver + TransmuterModule +
	// NATS handler. Subject cdc.cmd.transmute materialises 1 master;
	// cdc.cmd.transmute-shadow fans out from post-ingest hook.
	typeResolver := service.NewTypeResolver(db)
	transmuter := service.NewTransmuterModule(db, typeResolver, logger)
	transmuteHandler := handler.NewTransmuteHandler(transmuter, db, natsClient.Conn, logger)
	natsClient.Conn.Subscribe("cdc.cmd.transmute", transmuteHandler.HandleTransmute)
	natsClient.Conn.Subscribe("cdc.cmd.transmute-shadow", transmuteHandler.HandleTransmuteShadow)
	logger.Info("transmute handler registered",
		zap.String("subject_run", "cdc.cmd.transmute"),
		zap.String("subject_shadow", "cdc.cmd.transmute-shadow"))

	// Transmute scheduler (plan v2 §R7). Cron-driven + fencing-guarded.
	// machineID/fencingToken shared with sinkworker not strictly required
	// here — worker binary claims its own pair via worker_registry. For
	// local dev we reuse a 0/0 pair; production should wire distinct claim.
	transmuteScheduler := service.NewTransmuteScheduler(db, natsClient.Conn, logger, 0, 0)
	go transmuteScheduler.Start(context.Background())
	logger.Info("transmute scheduler started (60s poll, cron + FOR UPDATE SKIP LOCKED + fencing)")

	// Master DDL generator (Sprint 5 R8) — consumes cdc.cmd.master-create.
	masterDDLGen := service.NewMasterDDLGenerator(db, logger)
	masterDDLHandler := handler.NewMasterDDLHandler(masterDDLGen, natsClient.Conn, logger)
	natsClient.Conn.Subscribe("cdc.cmd.master-create", masterDDLHandler.HandleMasterCreate)
	logger.Info("master DDL handler registered", zap.String("subject", "cdc.cmd.master-create"))

	// Sprint 4 4A.1. pgxPool remains available for future high-throughput
	// Debezium-only consumers.
	_ = pgxPool

	// 10d. Reconciliation handlers (recon-check, recon-heal, retry-failed, debezium signals)
	var reconHealerShared *service.ReconHealer
	var tsDetectorShared *service.TimestampDetector
	var fullCountAggShared *service.FullCountAggregator
	if reconCore != nil {
		var mongoClientForRecon *mongo.Client
		if cfg.MongoDB.URL != "" {
			mongoClientForRecon, _ = mongodb.NewClient(context.Background(), mongodb.MongoConfig{URL: cfg.MongoDB.URL}, logger)
		}

		// Plan WORKER task #9 — NATS cmd cdc.cmd.recon-heal now routes
		// through ReconHealer.HealWindow (Phase 2/3) instead of the
		// legacy ReconCore.Heal. Signal client optional — nil falls
		// back to the direct $in path.
		var signalClient *service.DebeziumSignalClient
		if mongoClientShared != nil {
			signalClient = service.NewDebeziumSignalClient(
				mongoClientShared,
				service.DebeziumSignalConfig{
					SignalCollection:     cfg.Debezium.SignalCollection,
					ConnectorStatusURL:   cfg.Debezium.ConnectorStatusURL,
					IncrementalChunkSize: cfg.Debezium.IncrementalChunkSize,
				},
				logger,
			)
		}
		reconHealerShared = service.NewReconHealer(
			db, mongoClientShared, schemaAdapter, signalClient,
			service.ReconHealerConfig{}, // per-table masks loaded from registry
			logger,
		)

		reconHandler := handler.NewReconHandler(reconCore, db, mongoClientForRecon, schemaAdapter, logger).
			WithHealer(reconHealerShared)

		// Backfill (_source_ts) service — tier 4 runs. Requires Mongo
		// client + registry to resolve source → dest pairs.
		backfillSvc := service.NewBackfillSourceTsService(
			db, mongoClientShared, registryRepo,
			service.BackfillSourceTsConfig{}, logger,
		)
		reconHandler = reconHandler.WithBackfill(backfillSvc, natsClient.Conn)

		// Migration 017 — auto-detect timestamp field. Exposed via
		// cdc.cmd.detect-timestamp-field NATS subject so admins (and
		// the systematic bootstrap loop in this server) can re-run
		// detection on demand without restarting worker.
		tsDetectorShared = service.NewTimestampDetector(mongoClientShared, logger)
		reconHandler = reconHandler.WithTimestampDetector(tsDetectorShared)

		natsClient.Conn.Subscribe("cdc.cmd.recon-check", reconHandler.HandleReconCheck)
		natsClient.Conn.Subscribe("cdc.cmd.recon-heal", reconHandler.HandleReconHeal)
		natsClient.Conn.Subscribe("cdc.cmd.retry-failed", reconHandler.HandleRetryFailed)
		natsClient.Conn.Subscribe("cdc.cmd.debezium-signal", reconHandler.HandleDebeziumSignal)
		natsClient.Conn.Subscribe("cdc.cmd.debezium-snapshot", reconHandler.HandleDebeziumSignal)
		natsClient.Conn.Subscribe("cdc.cmd.recon-backfill-source-ts", reconHandler.HandleBackfillSourceTs)
		natsClient.Conn.Subscribe("cdc.cmd.detect-timestamp-field", reconHandler.HandleDetectTimestampField)
		logger.Info("reconciliation handlers registered (7 commands)")

		// Migration 017 §2.7 — daily full-count aggregator. Captures
		// Mongo EstimatedDocumentCount + PG COUNT(*) (or reltuples for
		// tables >10M rows) into cdc_table_registry.full_*_count.
		fullCountAggShared = service.NewFullCountAggregator(
			db, dbReplica, mongoClientShared, registryRepo,
			service.FullCountAggregatorConfig{}, logger,
		)
	} else {
		logger.Warn("reconciliation handlers NOT registered (MongoDB not configured)")
	}

	logger.Info("command listeners registered", zap.Strings("subjects", []string{
		"cdc.cmd.standardize", "cdc.cmd.discover", "cdc.cmd.backfill",
		"cdc.cmd.scan-raw-data", "cdc.cmd.batch-transform",
		"cdc.cmd.recon-check", "cdc.cmd.recon-heal",
		"cdc.cmd.retry-failed", "cdc.cmd.debezium-signal", "cdc.cmd.debezium-snapshot",
		"cdc.cmd.scan-fields", "cdc.cmd.sync-register", "cdc.cmd.sync-state",
		"cdc.cmd.restart-debezium", "cdc.cmd.alter-column",
		"cdc.cmd.master-create",
		"cdc.cmd.transmute", "cdc.cmd.transmute-shadow",
	}))

	// 11. HTTP server (health + metrics)
	app := fiber.New(fiber.Config{DisableStartupMessage: true})
	app.Use(cors.New())

	app.Get("/health", func(c *fiber.Ctx) error {
		return c.JSON(fiber.Map{"status": "ok", "service": "cdc-worker"})
	})
	app.Get("/ready", func(c *fiber.Ctx) error {
		sqlDB, _ := db.DB()
		if err := sqlDB.Ping(); err != nil {
			return c.Status(503).JSON(fiber.Map{"status": "not ready", "error": err.Error()})
		}
		return c.JSON(fiber.Map{"status": "ready"})
	})
	app.Get("/metrics", adaptor.HTTPHandler(promhttp.Handler()))

	// Internal monitoring API for CMS
	app.Get("/api/v1/internal/stats", func(c *fiber.Ctx) error {
		return c.JSON(fiber.Map{
			"queue":  consumerPool.GetStats(),
			"buffer": batchBuffer.GetStatus(),
			"config": fiber.Map{
				"pool_size":  cfg.Worker.PoolSize,
				"batch_size": cfg.Worker.BatchSize,
			},
		})
	})

	activityLogger := service.NewActivityLogger(db, logger)

	// 10e. Partition retention job (plan WORKER task #5). Runs on a
	// daily ticker; advisory-locked so multiple worker instances don't
	// fight over the same DROP TABLE calls.
	partitionDropper := service.NewPartitionDropper(db, service.PartitionDropperConfig{}, logger)

	return &WorkerServer{
		cfg:              cfg,
		logger:           logger,
		db:               db,
		dbReplica:        dbReplica,
		nats:             natsClient,
		redis:            redisCache,
		consumerPool:     consumerPool,
		batchBuffer:      batchBuffer,
		eventHandler:     eventHandler,
		registrySvc:      registrySvc,
		registryRepo:     registryRepo,
		activityLogger:   activityLogger,
		reconCore:        reconCore,
		reconHealer:      reconHealerShared,
		schemaValidator:  schemaValidator,
		dlqWorker:        dlqWorker,
		partitionDropper: partitionDropper,
		tsDetector:       tsDetectorShared,
		fullCountAgg:     fullCountAggShared,
		mongoClient:      mongoClientShared,
		app:              app,
	}, nil
}

func (s *WorkerServer) Start() error {
	// NATS consumer pool (for legacy CDC events, will phase out)
	s.consumerPool.Start()

	// Kafka consumer (primary CDC event source — Debezium → Kafka → Worker)
	if s.cfg.Kafka.Enabled && len(s.cfg.Kafka.Brokers) > 0 {
		kafkaConsumer := handler.NewKafkaConsumer(
			handler.KafkaConsumerConfig{
				Brokers:           s.cfg.Kafka.Brokers,
				GroupID:           s.cfg.Kafka.GroupID,
				TopicPrefix:       s.cfg.Kafka.TopicPrefix,
				SchemaRegistryURL: s.cfg.Kafka.SchemaRegistryURL,
			},
			s.eventHandler,
			s.registrySvc,
			s.db,
			s.logger,
		)
		kafkaConsumer.SetSchemaValidator(s.schemaValidator)
		go kafkaConsumer.Start(context.Background())
		s.logger.Info("kafka consumer started",
			zap.Strings("brokers", s.cfg.Kafka.Brokers),
			zap.String("group", s.cfg.Kafka.GroupID),
		)
	}

	// DLQ retry worker — polls failed_sync_logs every 5 minutes and
	// applies exponential backoff. Runs regardless of Kafka/NATS mode.
	if s.dlqWorker != nil {
		go s.dlqWorker.Start(context.Background())
	}

	// Partition retention job (plan WORKER task #5). Runs once at boot
	// + every 24h to DROP TABLE IF EXISTS on monthly partitions past
	// their configured retention window.
	if s.partitionDropper != nil {
		go s.partitionDropper.Start(context.Background())
	}

	// Migration 017 §2.7 — daily full-count aggregator. Fires at the
	// configured RunAt (default 03:00 UTC). Safe to run alongside
	// recon because Mongo EstimatedDocumentCount is a metadata read.
	if s.fullCountAgg != nil {
		go s.fullCountAgg.Start(context.Background())
	}

	// Schedule-driven periodic executor — reads cdc_worker_schedule from DB
	// Checks every 1 minute which operations are due
	go func() {
		time.Sleep(30 * time.Second) // Wait for services to initialize

		// Seed default schedules if empty
		var count int64
		s.db.Model(&model.WorkerSchedule{}).Count(&count)
		if count == 0 {
			defaults := []model.WorkerSchedule{
				{Operation: "transform", IntervalMinutes: 5, IsEnabled: true},
				{Operation: "field-scan", IntervalMinutes: 60, IsEnabled: true},
				{Operation: "partition-check", IntervalMinutes: 1440, IsEnabled: true},
				{Operation: "reconcile", IntervalMinutes: 30, IsEnabled: true},
			}
			for _, d := range defaults {
				s.db.Create(&d)
			}
			s.logger.Info("default schedules seeded", zap.Int("count", len(defaults)))
		} else {
			// Ensure "reconcile" exists even if schedules were seeded before this feature
			var reconCount int64
			s.db.Model(&model.WorkerSchedule{}).Where("operation = ?", "reconcile").Count(&reconCount)
			if reconCount == 0 {
				s.db.Create(&model.WorkerSchedule{Operation: "reconcile", IntervalMinutes: 30, IsEnabled: true})
				s.logger.Info("reconcile schedule added to existing schedules")
			}
		}

		// Startup visibility — count enabled schedules so operators know
		// the ticker is live and what it will do (Bug A diagnostic log).
		var enabledSchedules []model.WorkerSchedule
		s.db.Where("is_enabled = ?", true).Find(&enabledSchedules)
		scheduleSummary := make([]string, 0, len(enabledSchedules))
		for _, sc := range enabledSchedules {
			scheduleSummary = append(scheduleSummary,
				fmt.Sprintf("%s=%dm", sc.Operation, sc.IntervalMinutes))
		}
		s.logger.Info("schedule poller started",
			zap.Int("enabled_count", len(enabledSchedules)),
			zap.Strings("registered", scheduleSummary),
			zap.Duration("tick_interval", 60*time.Second),
			zap.Bool("recon_core_available", s.reconCore != nil),
		)

		ticker := time.NewTicker(60 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			var schedules []model.WorkerSchedule
			s.db.Where("is_enabled = ?", true).Find(&schedules)

			now := time.Now()
			for _, sched := range schedules {
				// Check if due. NULL last_run_at = first run since enable,
				// fire immediately (no wait for interval cycle). This is
				// intentional: user-visible "enable + expect run within 1
				// minute" SLA.
				intervalDur := time.Duration(sched.IntervalMinutes) * time.Minute
				if sched.LastRunAt != nil && now.Sub(*sched.LastRunAt) < intervalDur {
					continue // Not due yet
				}

				firstRun := sched.LastRunAt == nil
				s.logger.Info("executing scheduled operation",
					zap.String("operation", sched.Operation),
					zap.Int("interval_min", sched.IntervalMinutes),
					zap.Bool("first_run", firstRun),
				)

				// target_table: nil = all, specific = only that table
				targetTable := ""
				if sched.TargetTable != nil {
					targetTable = *sched.TargetTable
				}

				switch sched.Operation {
				case "bridge":
					s.runBridgeCycle(now, targetTable)
				case "transform":
					s.runTransformCycle(now, targetTable)
				case "field-scan":
					if targetTable != "" {
						s.nats.Conn.Publish("cdc.cmd.scan-raw-data", []byte(targetTable))
						s.activityLogger.Quick("field-scan", targetTable, "scheduler", "success", 0, nil, "")
					} else {
						s.nats.Conn.Publish("cdc.cmd.periodic-scan", []byte("auto"))
						s.activityLogger.Quick("field-scan", "*", "scheduler", "success", 0, nil, "")
					}
				case "partition-check":
					s.runPartitionCheck(now)
				case "reconcile":
					s.runReconcileCycle(now)
				}

				// Update last_run_at + run_count
				s.db.Model(&model.WorkerSchedule{}).Where("id = ?", sched.ID).Updates(map[string]interface{}{
					"last_run_at": now,
					"next_run_at": now.Add(intervalDur),
					"run_count":   gorm.Expr("run_count + 1"),
				})
			}
		}
	}()

	s.logger.Info("CDC Worker started", zap.String("port", s.cfg.Server.Port))
	return s.app.Listen(s.cfg.Server.Port)
}

// runBridgeCycle is a no-op after Sprint 4 4A.1 retired the legacy bridge
// pipeline. Kept so WorkerSchedule rows with operation="bridge" don't error
// until an admin deletes them from cdc_worker_schedule. Future work: replace
// with Transmuter trigger dispatch.
func (s *WorkerServer) runBridgeCycle(_ time.Time, targetTable string) {
	logTarget := targetTable
	if logTarget == "" {
		logTarget = "*"
	}
	s.activityLogger.Quick("bridge", logTarget, "scheduler", "skipped", 0,
		map[string]interface{}{"reason": "legacy bridge retired"}, "")
}

// runTransformCycle dispatches transform. targetTable="" = all, specific = only that table.
func (s *WorkerServer) runTransformCycle(now time.Time, targetTable string) {
	entries, err := s.registryRepo.GetAllActive(context.Background())
	if err != nil {
		s.activityLogger.Quick("transform", targetTable, "scheduler", "error", 0, nil, err.Error())
		return
	}
	dispatched := 0
	for _, entry := range entries {
		if targetTable != "" && entry.TargetTable != targetTable {
			continue
		}
		s.nats.Conn.Publish("cdc.cmd.batch-transform", []byte(entry.TargetTable))
		dispatched++
	}
	logTarget := targetTable
	if logTarget == "" {
		logTarget = "*"
	}
	s.activityLogger.Quick("transform", logTarget, "scheduler", "success", int64(dispatched), map[string]interface{}{
		"tables": dispatched,
	}, "")
}

// runPartitionCheck ensures next month's partitions exist for partitioned tables
func (s *WorkerServer) runPartitionCheck(now time.Time) {
	entries, err := s.registryRepo.GetAllActive(context.Background())
	if err != nil {
		return
	}
	nextMonth := now.AddDate(0, 1, 0)
	checked := 0
	for _, entry := range entries {
		if entry.IsPartitioned == nil || !*entry.IsPartitioned {
			continue
		}
		s.db.Exec("SELECT ensure_cdc_partition($1, $2)", entry.TargetTable, nextMonth)
		checked++
	}
	s.activityLogger.Quick("partition-check", "*", "scheduler", "success", int64(checked), map[string]interface{}{
		"tables_checked": checked,
	}, "")
}

// runReconcileCycle runs reconciliation CheckAll via reconCore.
//
// Bug A fix (2026-04-20): when reconCore is nil the previous code wrote
// a single "skipped" row to activity_log and returned silently — operators
// monitoring worker.log never saw why their enabled schedule was effectively
// dead. We now WARN-log on every skipped tick so the condition is visible
// in the log stream, and keep writing the activity row for the UI.
func (s *WorkerServer) runReconcileCycle(now time.Time) {
	if s.reconCore == nil {
		s.logger.Warn("reconcile schedule tick SKIPPED — reconCore is nil",
			zap.String("reason", "MongoDB not configured or Mongo connection failed at startup"),
			zap.String("fix_hint", "set MONGODB_URL env + restart worker; check worker startup logs for 'MongoDB connection failed'"),
		)
		s.activityLogger.Quick("reconcile", "*", "scheduler", "skipped", 0, nil, "reconCore not initialized (MongoDB not configured)")
		return
	}
	s.logger.Info("reconcile cycle started", zap.Time("started_at", now))
	ctx := context.Background()
	reports := s.reconCore.CheckAll(ctx)

	driftCount := 0
	errorCount := 0
	for _, r := range reports {
		switch r.Status {
		case "drift":
			driftCount++
		case "error":
			errorCount++
		}
	}
	s.activityLogger.Quick("reconcile", "*", "scheduler", "success", int64(len(reports)), map[string]interface{}{
		"tables_checked": len(reports),
		"drift_count":    driftCount,
		"error_count":    errorCount,
	}, "")
	s.logger.Info("reconcile cycle completed",
		zap.Int("tables_checked", len(reports)),
		zap.Int("drift_detected", driftCount),
		zap.Int("error_count", errorCount),
		zap.Duration("elapsed", time.Since(now)),
	)
}

func (s *WorkerServer) Shutdown() {
	s.logger.Info("shutting down CDC Worker...")
	s.consumerPool.Stop()
	s.batchBuffer.Stop()
	s.app.Shutdown()
	s.nats.Close()
	s.redis.Close()
	sqlDB, _ := s.db.DB()
	sqlDB.Close()
	s.logger.Info("CDC Worker stopped")
}
