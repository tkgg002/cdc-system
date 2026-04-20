package router

import (
	"time"

	"cdc-cms-service/config"
	"cdc-cms-service/internal/api"
	"cdc-cms-service/internal/middleware"
	"cdc-cms-service/pkgs/rediscache"

	"github.com/gofiber/fiber/v2"
)

// DestructiveMiddleware bundles the Phase-4 security stack. Kept as a
// struct so the server bootstrap can wire it once and pass it in,
// avoiding a long parameter list on SetupRoutes.
type DestructiveMiddleware struct {
	Idempotency fiber.Handler
	Audit       fiber.Handler
	RateRestart fiber.Handler // extra layer for connector restart (3/hour)
}

// NewDestructiveMiddleware builds the bundle. audit may be nil during
// very-early bootstrap (no DB yet) — in that case audit is skipped.
func NewDestructiveMiddleware(redis *rediscache.RedisCache, auditLogger *middleware.AuditLogger) DestructiveMiddleware {
	bundle := DestructiveMiddleware{}
	if redis != nil {
		bundle.Idempotency = middleware.NewIdempotency(
			middleware.NewIdempotencyFromRedisClient(redis.Client()),
		)
		bundle.RateRestart = middleware.NewRateLimit(middleware.RateLimitConfig{
			Redis:  redis,
			Scope:  "restart",
			Max:    3,
			Window: time.Hour,
		})
	}
	if auditLogger != nil {
		bundle.Audit = auditLogger.Middleware()
	}
	return bundle
}

// chain applies the optional middlewares in order, skipping any nil.
func chain(mws ...fiber.Handler) []fiber.Handler {
	out := make([]fiber.Handler, 0, len(mws))
	for _, m := range mws {
		if m != nil {
			out = append(out, m)
		}
	}
	return out
}

func SetupRoutes(
	app *fiber.App,
	cfg *config.AppConfig,
	healthHandler *api.HealthHandler,
	schemaHandler *api.SchemaChangeHandler,
	registryHandler *api.RegistryHandler,
	mappingHandler *api.MappingRuleHandler,
	introspectionHandler *api.IntrospectionHandler,
	airbyteHandler *api.AirbyteHandler,
	activityLogHandler *api.ActivityLogHandler,
	scheduleHandler *api.ScheduleHandler,
	reconHandler *api.ReconciliationHandler,
	systemHealthHandler *api.SystemHealthHandler,
	alertsHandler *api.AlertsHandler,
	destructive DestructiveMiddleware,
) {
	app.Get("/health", healthHandler.Health)
	app.Get("/api/system/health", systemHealthHandler.Health)
	app.Get("/ready", healthHandler.Ready)

	// API Group
	apiGroup := app.Group("/api")

	// Airbyte routes
	airbyteGroup := apiGroup.Group("/airbyte")
	airbyteGroup.Get("/sources", airbyteHandler.ListSources)
	airbyteGroup.Get("/destinations", airbyteHandler.ListDestinations)
	airbyteGroup.Get("/connections", airbyteHandler.ListConnectionDetails)
	airbyteGroup.Get("/connections/:id/streams", airbyteHandler.GetConnectionStreams)
	airbyteGroup.Get("/jobs", airbyteHandler.ListJobs)
	airbyteGroup.Get("/sync-audit", airbyteHandler.GetSyncAudit)
	airbyteGroup.Get("/import/list", airbyteHandler.ListImportableStreams)
	airbyteGroup.Post("/import/execute", airbyteHandler.ExecuteImport)
	apiGroup.Post("/registry/:id/refresh-catalog-unauth", registryHandler.RefreshCatalog)

	// All API routes require JWT auth
	apiGroup.Use(middleware.JWTAuth(cfg))

	// ------------------------------------------------------------
	// Phase 4 — Destructive admin endpoints (MUST come before the
	// `shared` / `admin` Groups below).
	//
	// Fiber quirk: `Group("", mw)` installs `Use` middleware on the
	// parent group, affecting *subsequent* routes too. If we mounted
	// destructive routes AFTER the shared/admin Groups, our routes
	// would inherit their `RequireRole("admin|operator")` gate and
	// ops-admin tokens would be rejected. Registering destructive
	// first insulates them from the read-only groups.
	//
	// Every route below MUST pass the full security stack:
	//   JWTAuth  (already applied upstream on apiGroup)
	//     → RequireOpsAdmin()
	//     → [optional] RateLimit (restart only, 3/hour/user)
	//     → Idempotency (Redis TTL 1h)
	//     → Audit (async INSERT into admin_actions)
	//     → handler
	//
	// Plan ref: 02_plan_data_integrity_v3.md §13.
	// ------------------------------------------------------------
	destructiveChain := chain(
		middleware.RequireOpsAdmin(),
		destructive.Idempotency,
		destructive.Audit,
	)
	destructiveRestartChain := chain(
		middleware.RequireOpsAdmin(),
		destructive.RateRestart,
		destructive.Idempotency,
		destructive.Audit,
	)

	// Register destructive routes at the per-route level (not via
	// Group-with-Use) so they do NOT leak middleware onto other
	// subsequent handlers.
	registerDestructive := func(path string, h fiber.Handler) {
		handlers := append([]fiber.Handler{}, destructiveChain...)
		handlers = append(handlers, h)
		apiGroup.Post(path, handlers...)
	}
	registerDestructiveRestart := func(path string, h fiber.Handler) {
		handlers := append([]fiber.Handler{}, destructiveRestartChain...)
		handlers = append(handlers, h)
		apiGroup.Post(path, handlers...)
	}

	registerDestructive("/reconciliation/check", reconHandler.TriggerCheckAll)
	registerDestructive("/reconciliation/check/:table", reconHandler.TriggerCheck)
	registerDestructive("/reconciliation/heal/:table", reconHandler.TriggerHeal)
	registerDestructive("/failed-sync-logs/:id/retry", reconHandler.RetryFailedLog)
	registerDestructive("/tools/reset-debezium-offset", reconHandler.ResetDebeziumOffset)
	registerDestructive("/tools/trigger-snapshot/:table", reconHandler.TriggerSnapshot)
	registerDestructiveRestart("/tools/restart-debezium", systemHealthHandler.RestartDebezium)

	// Backfill `_source_ts` — not strictly destructive (data enrichment
	// only, parameterised UPDATE guarded by `_source_ts IS NULL`), but
	// we still require ops-admin + idempotency + audit so every run is
	// attributable. Runs as a background job; returns 202 immediately.
	registerDestructive("/recon/backfill-source-ts", reconHandler.TriggerBackfillSourceTs)

	// Phase 6 — alert write operations piggyback on the destructive
	// chain so audit+idempotency are automatic.
	if alertsHandler != nil {
		registerDestructive("/alerts/:fingerprint/ack", alertsHandler.Ack)
		registerDestructive("/alerts/:fingerprint/silence", alertsHandler.Silence)
	}

	// TODO(phase-4): routes below are mentioned in the plan but their
	// handlers do not exist in this service yet. When they land, mount
	// via registerDestructive / registerDestructiveRestart:
	//   POST /api/recon/heal            → generic heal (currently /:table)
	//   POST /api/connectors/:name/restart   → generic connector restart
	//   POST /api/debezium/signal       → arbitrary signal (currently
	//                                     modeled as reset-debezium-offset)
	//   POST /api/kafka/reset-offset    → kafka-side offset reset

	// --- Shared routes (admin + operator) ---
	shared := apiGroup.Group("", middleware.RequireRole("admin", "operator"))
	shared.Get("/schema-changes/pending", schemaHandler.GetPending)
	shared.Get("/schema-changes/history", schemaHandler.GetHistory)
	shared.Get("/sync/health", registryHandler.SyncHealth)
	shared.Get("/activity-log", activityLogHandler.List)
	shared.Get("/activity-log/stats", activityLogHandler.Stats)
	shared.Get("/worker-schedule", scheduleHandler.List)
	shared.Get("/sync/reconciliation", registryHandler.Reconciliation)
	shared.Get("/registry", registryHandler.List)
	shared.Get("/registry/stats", registryHandler.GetStats)
	shared.Get("/registry/:id/status", registryHandler.GetStatus)
	shared.Get("/registry/:id/dispatch-status", registryHandler.DispatchStatus)
	shared.Get("/mapping-rules", mappingHandler.List)
	shared.Get("/introspection/scan/:table", introspectionHandler.Scan)
	shared.Get("/introspection/scan-raw/:table", introspectionHandler.ScanRawData)

	// --- Admin only routes ---
	admin := apiGroup.Group("", middleware.RequireRole("admin"))
	admin.Post("/schema-changes/:id/approve", schemaHandler.Approve)
	admin.Post("/schema-changes/:id/reject", schemaHandler.Reject)
	admin.Post("/registry", registryHandler.Register)
	admin.Patch("/registry/:id", registryHandler.Update)
	admin.Post("/registry/batch", registryHandler.BulkRegister)
	admin.Post("/registry/scan-source", registryHandler.ScanSource)
	admin.Post("/registry/sync-from-airbyte", registryHandler.SyncFromAirbyte)
	admin.Post("/registry/:id/sync", registryHandler.Sync)
	admin.Get("/registry/:id/jobs", registryHandler.GetJobs)
	admin.Post("/registry/:id/standardize", registryHandler.Standardize)
	admin.Post("/registry/:id/discover", registryHandler.Discover)
	admin.Post("/registry/:id/refresh-catalog", registryHandler.RefreshCatalog)
	admin.Post("/registry/:id/scan-fields", registryHandler.ScanFields)
	admin.Post("/registry/:id/bridge", registryHandler.Bridge)
	admin.Post("/registry/:id/transform", registryHandler.Transform)
	admin.Post("/registry/:id/drop-gin-index", registryHandler.DropGINIndex)
	admin.Post("/registry/:id/create-default-columns", registryHandler.CreateDefaultColumns)
	shared.Get("/registry/:id/transform-status", registryHandler.TransformStatus)
	admin.Post("/mapping-rules", mappingHandler.Create)
	admin.Patch("/mapping-rules/batch", mappingHandler.BatchUpdate)
	admin.Patch("/mapping-rules/:id", mappingHandler.UpdateStatus)
	admin.Post("/mapping-rules/reload", mappingHandler.Reload)
	admin.Post("/mapping-rules/:id/backfill", mappingHandler.Backfill)
	admin.Patch("/worker-schedule/:id", scheduleHandler.Update)
	admin.Post("/worker-schedule", scheduleHandler.Create)

	// Reconciliation + Data Integrity (read-only)
	shared.Get("/reconciliation/report", reconHandler.LatestReport)
	shared.Get("/reconciliation/report/:table", reconHandler.TableHistory)
	shared.Get("/failed-sync-logs", reconHandler.ListFailedLogs)
	shared.Get("/recon/backfill-source-ts/status", reconHandler.BackfillSourceTsStatus)

	// ------------------------------------------------------------
	// Phase 6 — Alert state machine.
	//
	// Reads (active / silenced / history) are shared (admin + operator)
	// because even read-only operators benefit from the runbook.
	// Writes (ack / silence) go through the destructive chain so they
	// inherit auth+idempotency+audit for free when Phase 4 is live.
	// ------------------------------------------------------------
	if alertsHandler != nil {
		shared.Get("/alerts/active", alertsHandler.Active)
		shared.Get("/alerts/silenced", alertsHandler.Silenced)
		shared.Get("/alerts/history", alertsHandler.History)
	}

}
