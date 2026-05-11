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
	sourceObjectsHandler *api.SourceObjectsHandler,
	sourceObjectActionsHandler *api.SourceObjectActionsHandler,
	systemConnectorsHandler *api.SystemConnectorsHandler,
	sourcesHandler *api.SourcesHandler,
	wizardHandler *api.WizardHandler,
	masterRegistryHandler *api.MasterRegistryHandler,
	schemaProposalHandler *api.SchemaProposalHandler,
	scheduleV1Handler *api.TransmuteScheduleHandler,
	mappingPreviewHandler *api.MappingPreviewHandler,
	mappingHandler *api.MappingRuleHandler,
	introspectionHandler *api.IntrospectionHandler,
	activityLogHandler *api.ActivityLogHandler,
	scheduleHandler *api.ScheduleHandler,
	reconHandler *api.ReconciliationHandler,
	systemHealthHandler *api.SystemHealthHandler,
	alertsHandler *api.AlertsHandler,
	provisioningHandler *api.ProvisioningHandler,
	destructive DestructiveMiddleware,
) {
	app.Get("/health", healthHandler.Health)
	app.Get("/api/system/health", systemHealthHandler.Health)
	app.Get("/ready", healthHandler.Ready)

	// API Group
	apiGroup := app.Group("/api")

	// Debezium Command Center at /api/v1/system/connectors/*.

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
	registerDestructive("/reconciliation/heal", reconHandler.TriggerHeal)
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

	// Debezium Command Center — Kafka Connect REST proxy. Replaces the
	registerDestructive("/v1/system/connectors", systemConnectorsHandler.Create)
	registerDestructive("/v1/system/connectors/:name/restart", systemConnectorsHandler.Restart)
	registerDestructive("/v1/system/connectors/:name/tasks/:taskId/restart", systemConnectorsHandler.RestartTask)
	registerDestructive("/v1/system/connectors/:name/pause", systemConnectorsHandler.Pause)
	registerDestructive("/v1/system/connectors/:name/resume", systemConnectorsHandler.Resume)
	// Destructive DELETE — removes connector entirely. Registered manually
	// (registerDestructive only wraps POST).
	{
		deleteHandlers := append([]fiber.Handler{}, destructiveChain...)
		deleteHandlers = append(deleteHandlers, systemConnectorsHandler.Delete)
		apiGroup.Delete("/v1/system/connectors/:name", deleteHandlers...)
	}

	// Master Table Registry (Sprint 5 §R8) — admin plane for warehouse
	// masters. Approve dispatches cdc.cmd.master-create → worker runs DDL.
	registerDestructive("/v1/masters", masterRegistryHandler.Create)
	registerDestructive("/v1/masters/:name/approve", masterRegistryHandler.Approve)
	registerDestructive("/v1/masters/:name/reject", masterRegistryHandler.Reject)
	registerDestructive("/v1/masters/:name/toggle-active", masterRegistryHandler.ToggleActive)
	// Systematic Flow F-4.1 — atomic master swap (BEGIN+RENAME+COMMIT).
	registerDestructive("/v1/masters/:name/swap", masterRegistryHandler.Swap)

	// Systematic Flow F-3 — Wizard state machine.
	//
	// Tier split (Apr 2026):
	//   - Create (draft) + Patch (draft field edits)  → admin tier only.
	//     Zero infra side-effect, zero DDL, zero fan-out. Forcing the
	//     destructive chain (Idempotency-Key + reason ≥ 10) here would
	//     generate audit noise on every keystroke + burden the FE.
	//   - Execute (commit → triggers pipeline, NATS, Debezium, DDL)
	//     stays destructive — it is the real side-effect boundary.
	// See lessons.md [2026-04-24] "Route classification".
	registerDestructive("/v1/wizard/sessions/:id/execute", wizardHandler.Execute)

	// Schema Proposal Workflow (Sprint 5 §R9) — approve applies ALTER +
	// inserts mapping_rule in a single transaction.
	registerDestructive("/v1/schema-proposals/:id/approve", schemaProposalHandler.Approve)
	registerDestructive("/v1/schema-proposals/:id/reject", schemaProposalHandler.Reject)

	// Transmute Schedules (Sprint 5 Dashboard.2)
	registerDestructive("/v1/schedules", scheduleV1Handler.Create)
	registerDestructive("/v1/schedules/:id/run-now", scheduleV1Handler.RunNow)
	{
		handlers := append([]fiber.Handler{}, destructiveChain...)
		handlers = append(handlers, scheduleV1Handler.Toggle)
		apiGroup.Patch("/v1/schedules/:id", handlers...)
	}

	// Mapping-rule JsonPath preview (Sprint 5 Dashboard.1). Read-only
	// eval against live shadow rows — admin convenience before saving.
	{
		handlers := append([]fiber.Handler{}, destructiveChain...)
		handlers = append(handlers, mappingPreviewHandler.Preview)
		apiGroup.Post("/v1/mapping-rules/preview", handlers...)
	}

	// Phase 6 — alert write operations piggyback on the destructive
	// chain so audit+idempotency are automatic.
	if alertsHandler != nil {
		registerDestructive("/alerts/:fingerprint/ack", alertsHandler.Ack)
		registerDestructive("/alerts/:fingerprint/silence", alertsHandler.Silence)
	}

	// ------------------------------------------------------------
	// Source Provisioning Mode (workspace feature-cdc-integration /
	// phase provisioning_mode). Architect ruling D5: path scope
	// /api/v1/cms/sources/:id/provisioning/*. ALL 7 endpoints (incl.
	// GET) gated by RequireOpsAdmin per architect Phase C ruling.
	// Mutating endpoints additionally pick up the destructive chain
	// (Idempotency + Audit) so a Manager retrying with the same
	// Idempotency-Key doesn't double-fire commands.
	//
	// Error mapping (api/provisioning_handler.go):
	//   ErrProvisioningSourceNotFound      → 404
	//   ErrProvisioningInvalidTransition   → 422
	//   ErrProvisioningConflict            → 409
	// ------------------------------------------------------------
	if provisioningHandler != nil {
		// GET — ops-admin alone, no idempotency/audit (read-only).
		{
			handlers := []fiber.Handler{middleware.RequireOpsAdmin()}
			handlers = append(handlers, provisioningHandler.GetState)
			apiGroup.Get("/v1/cms/sources/:id/provisioning", handlers...)
		}
		// POST writes — full destructive chain (ops-admin + idempotency + audit).
		registerDestructive("/v1/cms/sources/:id/provisioning/advance", provisioningHandler.Advance)
		registerDestructive("/v1/cms/sources/:id/provisioning/pause", provisioningHandler.Pause)
		registerDestructive("/v1/cms/sources/:id/provisioning/resume", provisioningHandler.Resume)
		registerDestructive("/v1/cms/sources/:id/provisioning/retry", provisioningHandler.Retry)
		registerDestructive("/v1/cms/sources/:id/provisioning/archive", provisioningHandler.Archive)
		registerDestructive("/v1/cms/sources/:id/provisioning/mode", provisioningHandler.SetMode)
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
	shared.Get("/v1/source-objects/stats", sourceObjectsHandler.GetStats)
	shared.Get("/v1/source-objects", sourceObjectsHandler.List)
	shared.Get("/v1/source-objects/registry/:registry_id", sourceObjectsHandler.GetMappingContext)
	shared.Get("/v1/shadow-bindings", sourceObjectsHandler.ListShadowBindings)
	shared.Get("/v1/source-objects/:id/dispatch-status", sourceObjectActionsHandler.DispatchStatusV2)
	shared.Get("/v1/source-objects/:id/transform-status", sourceObjectActionsHandler.TransformStatusV2)
	shared.Get("/v1/source-objects/registry/:id/dispatch-status", sourceObjectActionsHandler.DispatchStatus)
	shared.Get("/v1/source-objects/registry/:id/transform-status", sourceObjectActionsHandler.TransformStatus)
	shared.Get("/mapping-rules", mappingHandler.List)
	shared.Get("/introspection/scan/:table", introspectionHandler.Scan)
	shared.Get("/introspection/scan-raw/:table", introspectionHandler.ScanRawData)
	shared.Get("/v1/system/connectors", systemConnectorsHandler.List)
	shared.Get("/v1/system/connectors/:name", systemConnectorsHandler.Get)
	shared.Get("/v1/system/connector-plugins", systemConnectorsHandler.Plugins)
	// Systematic Flow F-1.2/1.3 — Sources registry reads.
	shared.Get("/v1/sources", sourcesHandler.List)
	shared.Get("/v1/sources/:id", sourcesHandler.Get)
	// Systematic Flow F-3.2/3.5 — Wizard state machine reads.
	shared.Get("/v1/wizard/sessions/:id", wizardHandler.Get)
	shared.Get("/v1/wizard/sessions/:id/progress", wizardHandler.Progress)
	shared.Get("/v1/masters", masterRegistryHandler.List)
	shared.Get("/v1/schema-proposals", schemaProposalHandler.List)
	shared.Get("/v1/schema-proposals/:id", schemaProposalHandler.Get)
	shared.Get("/v1/schedules", scheduleV1Handler.List)

	// --- Admin only routes ---
	admin := apiGroup.Group("", middleware.RequireRole("admin"))
	admin.Post("/schema-changes/:id/approve", schemaHandler.Approve)
	admin.Post("/schema-changes/:id/reject", schemaHandler.Reject)
	admin.Post("/v1/source-objects/register", sourceObjectActionsHandler.Register)
	admin.Patch("/v1/source-objects/:id", sourceObjectActionsHandler.UpdateV2)
	admin.Post("/v1/source-objects/:id/create-default-columns", sourceObjectActionsHandler.CreateDefaultColumnsV2)
	admin.Post("/v1/source-objects/:id/scan-fields", sourceObjectActionsHandler.ScanFieldsV2)
	admin.Post("/v1/source-objects/:id/standardize", sourceObjectActionsHandler.StandardizeV2)
	admin.Patch("/v1/source-objects/registry/:id", sourceObjectActionsHandler.UpdateBridge)
	admin.Post("/v1/source-objects/register-batch", sourceObjectActionsHandler.BulkRegister)
	admin.Post("/v1/source-objects/registry/:id/standardize", sourceObjectActionsHandler.Standardize)
	admin.Post("/v1/source-objects/registry/:id/scan-fields", sourceObjectActionsHandler.ScanFields)
	admin.Post("/v1/source-objects/registry/:id/transform", sourceObjectActionsHandler.Transform)
	admin.Post("/v1/source-objects/registry/:id/create-default-columns", sourceObjectActionsHandler.CreateDefaultColumns)
	admin.Post("/v1/source-objects/:id/detect-timestamp-field", sourceObjectActionsHandler.DetectTimestampFieldV2)
	admin.Post("/v1/source-objects/registry/:id/detect-timestamp-field", sourceObjectActionsHandler.DetectTimestampField)
	admin.Post("/mapping-rules", mappingHandler.Create)
	admin.Patch("/mapping-rules/batch", mappingHandler.BatchUpdate)
	admin.Patch("/mapping-rules/:id", mappingHandler.UpdateStatus)
	admin.Post("/mapping-rules/reload", mappingHandler.Reload)
	admin.Post("/mapping-rules/:id/backfill", mappingHandler.Backfill)
	admin.Patch("/worker-schedule/:id", scheduleHandler.Update)
	admin.Post("/worker-schedule", scheduleHandler.Create)

	// Systematic Flow F-3 — Wizard draft endpoints (non-destructive tier).
	// See block above for the tier rationale.
	admin.Post("/v1/wizard/sessions", wizardHandler.Create)
	admin.Patch("/v1/wizard/sessions/:id", wizardHandler.Patch)

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
