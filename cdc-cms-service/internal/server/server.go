package server

import (
	"context"
	"fmt"

	"cdc-cms-service/config"
	"cdc-cms-service/internal/api"
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
	cfg    *config.AppConfig
	logger *zap.Logger
	db     *gorm.DB
	nats   *natsconn.NatsClient
	redis  *rediscache.RedisCache
	app    *fiber.App
	reconSvc         *service.ReconciliationService
	healthCollector  *service.Collector
	collectorCancel  context.CancelFunc
	auditLogger      *middleware.AuditLogger
	auditCancel      context.CancelFunc
	// Phase 6 — alert state machine + background resolver.
	alertMgr      *service.AlertManager
	alertResolverCancel context.CancelFunc
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

	// No external client wiring required.

	// Services
	approvalSvc := service.NewApprovalService(db, pendingRepo, mappingRepo, schemaLogRepo, registryRepo, natsClient, logger)
	reconSvc := service.NewReconciliationService(registryRepo, mappingRepo, db, logger)

	// Handlers
	healthHandler := api.NewHealthHandler(db)
	schemaHandler := api.NewSchemaChangeHandler(pendingRepo, schemaLogRepo, approvalSvc)
	registryHandler := api.NewRegistryHandler(registryRepo, mappingRepo, db, natsClient, logger)
	cdcInternalRegistryHandler := api.NewCDCInternalRegistryHandler(db, logger)
	systemConnectorsHandler := api.NewSystemConnectorsHandler(cfg.System.KafkaConnectURL, logger)
	masterRegistryHandler := api.NewMasterRegistryHandler(db, natsClient, logger)
	schemaProposalHandler := api.NewSchemaProposalHandler(db, logger)
	scheduleHandler2 := api.NewTransmuteScheduleHandler(db, natsClient, logger)
	mappingPreviewHandler := api.NewMappingPreviewHandler(db, logger)
	mappingHandler := api.NewMappingRuleHandler(mappingRepo, registryRepo, natsClient, db)
	introspectionHandler := api.NewIntrospectionHandler(natsClient)
	activityLogHandler := api.NewActivityLogHandler(db)
	scheduleHandler := api.NewScheduleHandler(db)
	reconHandler := api.NewReconciliationHandler(db, natsClient)
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
	alertsHandler := api.NewAlertsHandler(alertMgr, logger)

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
	router.SetupRoutes(app, cfg, healthHandler, schemaHandler, registryHandler, cdcInternalRegistryHandler, systemConnectorsHandler, masterRegistryHandler, schemaProposalHandler, scheduleHandler2, mappingPreviewHandler, mappingHandler, introspectionHandler, activityLogHandler, scheduleHandler, reconHandler, systemHealthHandler, alertsHandler, destructiveMW)

	return &Server{
		cfg: cfg, logger: logger, db: db,
		nats: natsClient, redis: redisCache, app: app,
		reconSvc:        reconSvc,
		healthCollector: healthCollector,
		auditLogger:     auditLogger,
		alertMgr:        alertMgr,
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
	s.app.Shutdown()
	s.nats.Close()
	s.redis.Close()
	sqlDB, _ := s.db.DB()
	sqlDB.Close()
	s.logger.Info("CMS Service stopped")
}
