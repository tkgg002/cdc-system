package server

import (
	"context"
	"fmt"
	"time"

	"cdc-cms-service/config"
	"cdc-cms-service/internal/api"
	"cdc-cms-service/internal/app/commands"
	"cdc-cms-service/internal/app/queries"
	infrahttp "cdc-cms-service/internal/infra/http"
	"cdc-cms-service/internal/infra/messaging"
	"cdc-cms-service/internal/infra/persistence"
	"cdc-cms-service/internal/middleware"
	"cdc-cms-service/internal/repository"
	"cdc-cms-service/internal/router"
	"cdc-cms-service/internal/service"
	"cdc-cms-service/pkgs/database"
	"cdc-cms-service/pkgs/natsconn"
	"cdc-cms-service/pkgs/rediscache"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/cors"
	fiberlogger "github.com/gofiber/fiber/v2/middleware/logger"
	"github.com/gofiber/swagger"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

type Server struct {
	cfg             *config.AppConfig
	logger          *zap.Logger
	db              *gorm.DB
	nats            *natsconn.NatsClient
	redis           *rediscache.RedisCache
	app             *fiber.App
	reconSvc        *service.ReconciliationService
	healthCollector *service.Collector
	collectorCancel context.CancelFunc
	auditLogger     *middleware.AuditLogger
	auditCancel     context.CancelFunc
	// Phase 6 — alert state machine + background resolver.
	alertMgr            *service.AlertManager
	alertResolverCancel context.CancelFunc
	// Phase 2 v2 / P3.T3.12 — stuck-job reaper (per-type timeout).
	stuckJobReaper       *service.StuckJobReaper
	stuckJobReaperCancel context.CancelFunc
}

func New(cfg *config.AppConfig, logger *zap.Logger) (*Server, error) {
	db, err := database.NewPostgresConnection(cfg)
	if err != nil {
		return nil, fmt.Errorf("postgres: %w", err)
	}
	logger.Info("PostgreSQL connected")

	// Schema managed via SQL migrations (centralized-data-service/migrations/001-014
	// + cdc-cms-service/migrations/003-013) — NOT auto-migrated.
	// Reason: GORM AutoMigrate conflicts with partitioned tables (e.g. cdc_activity_log
	// has composite PRIMARY KEY (created_at, id) for RANGE partitioning — GORM tries
	// to DROP NOT NULL on created_at which Postgres rejects with SQLSTATE 42P16).
	// Tables & owning migrations:
	//   - cdc_table_registry         -> 001_init_schema.sql, 013_table_registry_expected_fields.sql
	//   - cdc_mapping_rules          -> 001_init_schema.sql, cms/003_add_mapping_rule_status.sql
	//   - cdc_activity_log           -> 006_activity_log.sql, 010_partitioning.sql (PARTITIONED)
	//   - cdc_worker_schedule        -> 007_worker_schedule.sql
	//   - cdc_reconciliation_reports -> 008_reconciliation.sql
	//   - cdc_failed_sync_logs       -> 008_reconciliation.sql, 012_dlq_state_machine.sql
	//   - cdc_alerts / cdc_silences  -> cms/013_alerts.sql

	natsClient, err := natsconn.NewNatsClient(cfg, logger)
	if err != nil {
		return nil, fmt.Errorf("nats: %w", err)
	}

	redisCache, err := rediscache.NewRedisCache(cfg, logger)
	if err != nil {
		return nil, fmt.Errorf("redis: %w", err)
	}

	// Repositories
	registryRepo := repository.NewRegistryRepo(db)
	mappingRepo := repository.NewMappingRuleRepo(db)
	pendingRepo := repository.NewPendingFieldRepo(db)
	schemaLogRepo := repository.NewSchemaLogRepo(db)
	sourceRepo := repository.NewSourceRepo(db)
	wizardRepo := repository.NewWizardRepo(db)

	// Phase 2 v2 / P2 — CQRS Q-side adapters. New ports-backed repos
	// live alongside the legacy `internal/repository/` ones; each
	// migration moves one handler method (here: GET /api/mapping-rules
	// list path, GET /api/v1/source-objects, GET /api/v1/source-objects/
	// registry/:id) onto the new stack. Phase 2 v2 / P4 will retire the
	// legacy repo for these aggregates.
	mappingRuleRepoV2 := persistence.NewMappingRuleRepo(db)
	listMappingRulesH := queries.NewListMappingRulesHandler(mappingRuleRepoV2)

	sourceObjectReader := persistence.NewSourceObjectReadRepo(db)
	listSourceObjectsH := queries.NewListSourceObjectsHandler(sourceObjectReader)
	getSourceMappingContextH := queries.NewGetSourceObjectMappingContextHandler(sourceObjectReader)

	masterReader := persistence.NewMasterReadRepo(db)
	listMastersH := queries.NewListMastersHandler(masterReader)

	// Phase 2 v2 / P2.T2.4 — Reconciliation Q-side. One reader powers
	// 3 list endpoints (LatestReport, TableHistory, ListFailedLogs).
	reconReader := persistence.NewReconReadRepo(db)
	listLatestReportsH := queries.NewListLatestReportsHandler(reconReader)
	getTableHistoryH := queries.NewGetTableHistoryHandler(reconReader)
	listFailedLogsH := queries.NewListFailedLogsHandler(reconReader)

	// Phase 2 v2 / P2.T2.5 — SyncHealth Q-side. SystemHealth Snapshot
	// (Redis cache reader) skipped per CLAUDE.md §6 elegance — it has
	// no SQL surface to migrate.
	syncHealthReader := persistence.NewSyncHealthReadRepo(db)
	getSyncHealthH := queries.NewGetSyncHealthHandler(syncHealthReader)

	// Phase 2 v2 / P2.T2.6 — Connectors Q-side. One Kafka Connect
	// client backs all 8 connector endpoints (3 reads via query
	// handlers, 5 writes still on the legacy handler path until P3
	// moves them to commands).
	kafkaConnectClient := infrahttp.NewKafkaConnectClient(cfg.System.KafkaConnectURL)
	listConnectorsH := queries.NewListConnectorsHandler(kafkaConnectClient)
	getConnectorH := queries.NewGetConnectorHandler(kafkaConnectClient)
	listConnectorPluginsH := queries.NewListConnectorPluginsHandler(kafkaConnectClient)

	// Phase 2 v2 / P2.T2.7 — ActivityLog Q-side. Heavy SQL with
	// LATERAL joins on shadow_binding + source_object_registry,
	// extracted from the legacy handler.
	activityLogReader := persistence.NewActivityLogReadRepo(db)
	listActivityLogsH := queries.NewListActivityLogsHandler(activityLogReader)
	getActivityStatsH := queries.NewGetActivityStatsHandler(activityLogReader)

	// Phase 2 v2 / P2.T2.7 — TransmuteSchedule List Q-side.
	transmuteScheduleReader := persistence.NewTransmuteScheduleReadRepo(db)
	listTransmuteSchedulesH := queries.NewListTransmuteSchedulesHandler(transmuteScheduleReader)

	// Phase 2 v2 / P2.T2.7 — Sources + Wizard reads. Existing
	// `*SourceRepo` and `*WizardRepo` already satisfy the reader
	// ports — no new persistence adapter needed (Strangler Fig:
	// defer adapter rewrite to P4).
	listSourcesH := queries.NewListSourcesHandler(sourceRepo)
	getSourceH := queries.NewGetSourceHandler(sourceRepo)
	getWizardSessionH := queries.NewGetWizardSessionHandler(wizardRepo)
	getWizardProgressH := queries.NewGetWizardProgressHandler(wizardRepo)

	// Phase 2 v2 / P2.T2.7 — WorkerSchedule List Q-side. Reader is
	// shared between List (Q-side) and Create/Update (post-write
	// projection) — P3 will fold the latter into command handlers.
	workerScheduleReader := persistence.NewWorkerScheduleReadRepo(db)
	listWorkerSchedulesH := queries.NewListWorkerSchedulesHandler(workerScheduleReader)

	// Phase 2 v2 / P3.T3.10 — Job tracker Q-side. The same `*jobRepoGorm`
	// the CommandBus writes through satisfies the read port (it has
	// GetByID). One adapter, two consumers.
	jobRepo := persistence.NewJobRepo(db)
	getJobH := queries.NewGetJobHandler(jobRepo)

	// Phase 2 v2 / P3.T3.3 — NATSCommandBus. One bus is wired here and
	// shared by every API handler that mutates state. Sync handlers run
	// in-process; subject mappings publish onto JetStream-retained
	// subjects the worker subscribes.
	cmdBus := messaging.NewNATSCommandBus(natsClient.Conn, jobRepo, logger)

	// P3.T3.4 — sync metadata commands. Each line wires one command
	// type to its in-process handler. The bus closes the cdc_jobs row
	// on success/failure and surfaces the result inline so the API
	// handler can respond 200 + body without a polling round-trip.
	//
	// Sync registrations that depend on services not yet built are
	// deferred to just below the corresponding service init (e.g.
	// alert.ack waits for alertMgr).

	// P3.T3.5 — async NATS subjects. Adding a new async command is one
	// RegisterSubject call here + a Command struct under
	// `internal/app/commands/`. The worker repo owns the consumer side.
	cmdBus.RegisterSubject("recon.check", "cdc.cmd.recon-check")
	cmdBus.RegisterSubject("recon.heal", "cdc.cmd.recon-heal")
	cmdBus.RegisterSubject("recon.retry-failed", "cdc.cmd.retry-failed")
	cmdBus.RegisterSubject("recon.backfill-source-ts", "cdc.cmd.recon-backfill-source-ts")
	cmdBus.RegisterSubject("debezium.signal", "cdc.cmd.debezium-signal")
	cmdBus.RegisterSubject("debezium.snapshot", "cdc.cmd.debezium-snapshot")
	cmdBus.RegisterSubject("debezium.restart", "cdc.cmd.restart-debezium")
	cmdBus.RegisterSubject("source.create-default-columns", "cdc.cmd.create-default-columns")
	cmdBus.RegisterSubject("source.standardize", "cdc.cmd.standardize")
	cmdBus.RegisterSubject("source.scan-fields", "cdc.cmd.scan-fields")
	cmdBus.RegisterSubject("source.detect-timestamp-field", "cdc.cmd.detect-timestamp-field")
	cmdBus.RegisterSubject("mapping.backfill", "cdc.cmd.backfill")
	cmdBus.RegisterSubject("mapping.alter-column", "cdc.cmd.alter-column")
	cmdBus.RegisterSubject("transmute.run", "cdc.cmd.transmute")
	cmdBus.RegisterSubject("master.create", "cdc.cmd.master-create")

	// Services
	approvalSvc := service.NewApprovalService(db, pendingRepo, mappingRepo, schemaLogRepo, registryRepo, natsClient, logger)
	reconSvc := service.NewReconciliationService(registryRepo, mappingRepo, db, logger)
	shadowAutomator := service.NewShadowAutomator(db, logger)
	sourceObjectV2Sync := service.NewSourceObjectV2SyncService(db, logger)
	masterSwap := service.NewMasterSwap(db, jobRepo, logger)
	// Phase 2 T13 — single owner of cdc_activity_log writes/reads. Shared
	// across registry, source-object actions, and reconciliation handlers.
	activityLogger := service.NewActivityLogger(db, logger)

	// Handlers
	healthHandler := api.NewHealthHandler(db)
	schemaHandler := api.NewSchemaChangeHandler(pendingRepo, schemaLogRepo, approvalSvc)
	registryHandler := api.NewRegistryHandler(registryRepo, mappingRepo, db, natsClient, cmdBus, shadowAutomator, sourceObjectV2Sync, activityLogger, logger, getSyncHealthH)
	sourceObjectsHandler := api.NewSourceObjectsHandler(db, logger, listSourceObjectsH, getSourceMappingContextH)
	sourceObjectActionsHandler := api.NewSourceObjectActionsHandler(db, cmdBus, activityLogger, logger)
	systemConnectorsHandler := api.NewSystemConnectorsHandler(kafkaConnectClient, sourceRepo, cmdBus, logger, listConnectorsH, getConnectorH, listConnectorPluginsH)
	sourcesHandler := api.NewSourcesHandler(logger, listSourcesH, getSourceH)
	wizardHandler := api.NewWizardHandler(wizardRepo, logger, getWizardSessionH, getWizardProgressH, cmdBus)
	masterRegistryHandler := api.NewMasterRegistryHandler(db, natsClient, masterSwap, logger, listMastersH, cmdBus)
	schemaProposalHandler := api.NewSchemaProposalHandler(db, cmdBus, logger)
	scheduleHandler2 := api.NewTransmuteScheduleHandler(db, natsClient, cmdBus, logger, listTransmuteSchedulesH)
	mappingPreviewHandler := api.NewMappingPreviewHandler(db, logger)
	mappingHandler := api.NewMappingRuleHandler(mappingRepo, registryRepo, natsClient, cmdBus, listMappingRulesH, db)
	introspectionHandler := api.NewIntrospectionHandler(natsClient)
	activityLogHandler := api.NewActivityLogHandler(listActivityLogsH, getActivityStatsH)
	scheduleHandler := api.NewScheduleHandler(db, workerScheduleReader, listWorkerSchedulesH, cmdBus)
	reconHandler := api.NewReconciliationHandler(reconReader, natsClient, cmdBus, listLatestReportsH, getTableHistoryH, listFailedLogsH, activityLogger)
	jobHandler := api.NewJobHandler(getJobH)
	// Phase 0 — System Health Background Collector.
	// Builds a Prometheus client (path A + fallback) and a Collector that
	// writes a cached snapshot to Redis every 15s. The handler just reads
	// that cache, which keeps p99 under 50ms even during a cascading outage.
	promClient, err := service.NewPromClient(service.PromClientConfig{
		PrometheusURL: cfg.System.PrometheusURL,
		WorkerURL:     cfg.System.WorkerURL,
	}, logger)
	if err != nil {
		return nil, fmt.Errorf("prom client: %w", err)
	}
	healthCollector := service.NewCollector(
		service.CollectorConfig{
			WorkerURL:        cfg.System.WorkerURL,
			KafkaConnectURL:  cfg.System.KafkaConnectURL,
			NATSMonitorURL:   cfg.System.NatsMonitorURL,
			KafkaExporterURL: cfg.System.KafkaExporterURL,
			CacheKey:         cfg.System.HealthCacheKey,
			DebeziumName:     cfg.System.DebeziumConnector,
		},
		db, redisCache, promClient, logger,
	)
	systemHealthHandler := api.NewSystemHealthHandler(
		redisCache,
		natsClient,
		cmdBus,
		cfg.System.KafkaConnectURL,
		cfg.System.HealthCacheKey,
		cfg.System.DebeziumConnector,
		logger,
	)

	// Phase 6 — Alert state machine.
	// AlertManager owns Fire/Resolve/Ack/Silence + the BG resolver goroutine.
	// It is wired into the health collector so each tick persists the
	// currently-firing conditions; the HTTP handler exposes the read/write
	// surface.
	alertMgr := service.NewAlertManager(db, redisCache, logger)
	healthCollector.SetAlertManager(alertMgr)
	// P3.T3.4 — sync metadata commands. Each handler runs in-process via
	// bus.Execute. Bus persists a cdc_jobs row for audit + idempotency.
	cmdBus.RegisterSync("alert.ack", commands.NewAckAlertHandler(alertMgr))
	cmdBus.RegisterSync("alert.silence", commands.NewSilenceAlertHandler(alertMgr))
	cmdBus.RegisterSync("mapping.update-status", commands.NewUpdateMappingRuleHandler(db, natsClient, logger))
	cmdBus.RegisterSync("mapping.create", commands.NewCreateMappingRuleHandler(db, logger))
	cmdBus.RegisterSync("master.reject", commands.NewRejectMasterHandler(db, logger))
	cmdBus.RegisterSync("master.create", commands.NewCreateMasterHandler(db, logger))
	cmdBus.RegisterSync("master.approve", commands.NewApproveMasterHandler(db, natsClient, logger))
	cmdBus.RegisterSync("wizard.create", commands.NewCreateWizardHandler(wizardRepo, logger))
	cmdBus.RegisterSync("wizard.patch", commands.NewPatchWizardHandler(wizardRepo, logger))
	cmdBus.RegisterSync("wizard.execute", commands.NewWizardExecuteHandler(wizardRepo, logger))
	cmdBus.RegisterSync("source.update-v2", commands.NewUpdateSourceObjectV2Handler(db, logger))
	cmdBus.RegisterSync("schedule.update", commands.NewUpdateScheduleHandler(db, logger))
	cmdBus.RegisterSync("recon.failed-log-mark-retrying", commands.NewMarkFailedLogRetryingHandler(db))
	cmdBus.RegisterSync("registry.update", commands.NewUpdateRegistryHandler(db, natsClient, logger))
	cmdBus.RegisterSync("schedule.create", commands.NewCreateTransmuteScheduleHandler(db))
	cmdBus.RegisterSync("schedule.toggle", commands.NewToggleTransmuteScheduleHandler(db))
	cmdBus.RegisterSync("registry.register", commands.NewRegisterRegistryHandler(db, shadowAutomator, natsClient, logger))
	cmdBus.RegisterSync("registry.bulk-register", commands.NewBulkRegisterRegistryHandler(db, natsClient, logger))
	cmdBus.RegisterSync("source.v2-sync", commands.NewV2SyncHandler(sourceObjectV2Sync))
	cmdBus.RegisterSync("master.toggle-active", commands.NewToggleMasterActiveHandler(db))
	cmdBus.RegisterSync("worker-schedule.create", commands.NewCreateWorkerScheduleHandler(db))
	cmdBus.RegisterSync("schema-proposal.reject", commands.NewRejectSchemaProposalHandler(db))
	cmdBus.RegisterSync("schema-proposal.approve", commands.NewApproveSchemaProposalHandler(db))
	cmdBus.RegisterSync("system-connector.create", commands.NewCreateSystemConnectorHandler(kafkaConnectClient, sourceRepo, logger))
	cmdBus.RegisterSync("system-connector.delete", commands.NewDeleteSystemConnectorHandler(kafkaConnectClient, sourceRepo, logger))
	cmdBus.RegisterSync("system-connector.lifecycle", commands.NewLifecycleSystemConnectorHandler(kafkaConnectClient, logger))
	alertsHandler := api.NewAlertsHandler(alertMgr, cmdBus, logger)

	// Source Provisioning Mode (workspace feature-cdc-integration / phase
	// provisioning_mode). CMS owns the synchronous trigger surface;
	// worker owns RecoveryLoop + step_completed handling. Both share
	// the DB and rely on D6 CAS for race safety.
	provOrch := service.NewProvisioningOrchestrator(db, natsClient.Conn, logger)
	provisioningHandler := api.NewProvisioningHandler(provOrch, logger)

	// Phase 4 — Security stack.
	//
	// Audit logger: async writer into the partitioned admin_actions
	// table. Start goroutine in Start(); stop via Shutdown().
	auditLogger := middleware.NewAuditLogger(db, logger, nil)
	destructiveMW := router.NewDestructiveMiddleware(redisCache, auditLogger)

	// Fiber app
	app := fiber.New(fiber.Config{DisableStartupMessage: true})
	app.Use(fiberlogger.New())
	app.Use(cors.New())

	// Swagger UI
	app.Get("/swagger/*", swagger.HandlerDefault)

	// Routes
	router.SetupRoutes(app, cfg, healthHandler, schemaHandler, registryHandler, sourceObjectsHandler, sourceObjectActionsHandler, systemConnectorsHandler, sourcesHandler, wizardHandler, masterRegistryHandler, schemaProposalHandler, scheduleHandler2, mappingPreviewHandler, mappingHandler, introspectionHandler, activityLogHandler, scheduleHandler, reconHandler, systemHealthHandler, alertsHandler, provisioningHandler, jobHandler, destructiveMW)

	stuckJobReaper := service.NewStuckJobReaper(db, logger, 30*time.Second, nil)

	return &Server{
		cfg: cfg, logger: logger, db: db,
		nats: natsClient, redis: redisCache, app: app,
		reconSvc:        reconSvc,
		healthCollector: healthCollector,
		auditLogger:     auditLogger,
		alertMgr:        alertMgr,
		stuckJobReaper:  stuckJobReaper,
	}, nil
}

func (s *Server) Start() error {
	s.logger.Info("CMS Service started", zap.String("port", s.cfg.Server.Port))

	// Start background workers
	go s.reconSvc.Start(context.Background())

	// Phase 0 — system health collector (writes Redis snapshot every 15s).
	if s.healthCollector != nil {
		ctx, cancel := context.WithCancel(context.Background())
		s.collectorCancel = cancel
		go s.healthCollector.Run(ctx)
		s.logger.Info("system health collector started")
	}

	// Phase 4 — audit log async writer.
	if s.auditLogger != nil {
		ctx, cancel := context.WithCancel(context.Background())
		s.auditCancel = cancel
		go s.auditLogger.Run(ctx)
		s.logger.Info("audit logger started")
	}

	// Phase 6 — alert background resolver (reopen expired silences + auto-resolve stale firing rows).
	if s.alertMgr != nil {
		ctx, cancel := context.WithCancel(context.Background())
		s.alertResolverCancel = cancel
		go s.alertMgr.RunBackgroundResolver(ctx)
		s.logger.Info("alert background resolver started")
	}

	// Phase 2 v2 / P3.T3.12 — stuck job reaper (per-type timeout).
	if s.stuckJobReaper != nil {
		ctx, cancel := context.WithCancel(context.Background())
		s.stuckJobReaperCancel = cancel
		go s.stuckJobReaper.Run(ctx)
	}

	return s.app.Listen(s.cfg.Server.Port)
}

func (s *Server) Shutdown() {
	s.logger.Info("shutting down CMS Service...")
	if s.collectorCancel != nil {
		s.collectorCancel()
	}
	if s.auditCancel != nil {
		s.auditCancel()
	}
	if s.alertResolverCancel != nil {
		s.alertResolverCancel()
	}
	if s.stuckJobReaperCancel != nil {
		s.stuckJobReaperCancel()
	}
	s.app.Shutdown()
	s.nats.Close()
	s.redis.Close()
	sqlDB, _ := s.db.DB()
	sqlDB.Close()
	s.logger.Info("CMS Service stopped")
}
