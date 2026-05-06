package api

import (
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"time"

	"cdc-cms-service/internal/app/commands"
	"cdc-cms-service/internal/app/ports"
	"cdc-cms-service/internal/app/queries"
	"cdc-cms-service/internal/infra/messaging"
	"cdc-cms-service/internal/middleware"
	"cdc-cms-service/internal/model"
	"cdc-cms-service/internal/repository"
	"cdc-cms-service/internal/service"
	"cdc-cms-service/pkgs/natsconn"

	"github.com/gofiber/fiber/v2"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

type RegistryHandler struct {
	repo           *repository.RegistryRepo
	mappingRepo    *repository.MappingRuleRepo
	db             *gorm.DB
	natsClient     *natsconn.NatsClient
	bus            ports.CommandBus
	automator      *service.ShadowAutomator
	v2sync         *service.SourceObjectV2SyncService
	activityLogger *service.ActivityLogger
	logger         *zap.Logger
	syncHealthQ    *queries.GetSyncHealthHandler
	bridgeReader   queries.BridgeStatusReader
}

func NewRegistryHandler(
	repo *repository.RegistryRepo,
	mappingRepo *repository.MappingRuleRepo,
	db *gorm.DB,
	nats *natsconn.NatsClient,
	bus ports.CommandBus,
	automator *service.ShadowAutomator,
	v2sync *service.SourceObjectV2SyncService,
	activityLogger *service.ActivityLogger,
	logger *zap.Logger,
	syncHealthQ *queries.GetSyncHealthHandler,
	bridgeReader queries.BridgeStatusReader,
) *RegistryHandler {
	return &RegistryHandler{
		repo:           repo,
		mappingRepo:    mappingRepo,
		db:             db,
		natsClient:     nats,
		bus:            bus,
		automator:      automator,
		v2sync:         v2sync,
		activityLogger: activityLogger,
		logger:         logger,
		syncHealthQ:    syncHealthQ,
		bridgeReader:   bridgeReader,
	}
}

// List is kept as a compatibility delegate for V2 read models and internal
// operator-flow bridges. It is intentionally no longer mounted directly.
func (h *RegistryHandler) List(c *fiber.Ctx) error {
	filter := repository.RegistryFilter{
		Page:     intQuery(c, "page", 1),
		PageSize: intQuery(c, "page_size", 20),
	}
	if v := c.Query("source_db"); v != "" {
		filter.SourceDB = &v
	}
	if v := c.Query("sync_engine"); v != "" {
		filter.SyncEngine = &v
	}
	if v := c.Query("priority"); v != "" {
		filter.Priority = &v
	}
	if v := c.Query("is_active"); v != "" {
		b := v == "true"
		filter.IsActive = &b
	}
	if v := c.Query("destination_id"); v != "" {
		filter.DestinationID = &v
	}

	entries, total, err := h.repo.GetAll(c.Context(), filter)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": "failed to fetch registry"})
	}

	return c.JSON(fiber.Map{"data": entries, "total": total, "page": filter.Page})
}

// Register is kept as a compatibility delegate behind the V2
// /api/v1/source-objects/register facade.
func (h *RegistryHandler) Register(c *fiber.Ctx) error {
	var entry model.TableRegistry
	if err := c.BodyParser(&entry); err != nil {
		return c.Status(400).JSON(fiber.Map{"error": err.Error()})
	}

	if h.bus == nil {
		return c.Status(503).JSON(fiber.Map{"error": "command bus not ready"})
	}

	user := middleware.GetUsername(c)
	registerCtx := messaging.WithMetadata(c.UserContext(), user, c.Get("X-Correlation-Id"), c.Get("Idempotency-Key"))
	registerCmd := commands.RegisterRegistryCommand{Entry: entry, CreatedBy: user}
	res, err := h.bus.Execute(registerCtx, registerCmd)
	if err != nil {
		if errors.Is(err, commands.ErrShadowDDLFailed) {
			return c.Status(500).JSON(fiber.Map{"error": "shadow DDL failed: " + err.Error()})
		}
		return c.Status(500).JSON(fiber.Map{"error": "failed to register table: " + err.Error()})
	}

	var body struct {
		Message string              `json:"message"`
		Entry   model.TableRegistry `json:"entry"`
	}
	_ = json.Unmarshal(res.ResultBody, &body)
	created := body.Entry

	dispatched := []string{}
	dispatchIdem := c.Get("Idempotency-Key")
	if dispatchIdem != "" {
		dispatchIdem += ":cdc"
	}
	dispatchCtx := messaging.WithMetadata(c.UserContext(), user, c.Get("X-Correlation-Id"), dispatchIdem)
	createCmd := commands.CreateDefaultColumnsCommand{
		RegistryID:      created.ID,
		TargetTable:     created.TargetTable,
		SourceTable:     created.SourceTable,
		PrimaryKeyField: created.PrimaryKeyField,
		PrimaryKeyType:  created.PrimaryKeyType,
	}
	if _, derr := h.bus.Dispatch(dispatchCtx, createCmd); derr != nil {
		h.logger.Warn("publish create-default-columns failed", zap.Error(derr))
	} else {
		dispatched = append(dispatched, "cdc.cmd.create-default-columns")
	}

	if h.bus != nil {
		syncCmd := commands.V2SyncCommand{Entry: &created}
		if _, err := h.bus.Execute(dispatchCtx, syncCmd); err != nil {
			h.logger.Error("post-register v2 sync failed", zap.Uint("registry_id", created.ID), zap.Error(err))
		}
	}

	return c.Status(202).JSON(fiber.Map{
		"message":    "table registered — external sync dispatched",
		"entry":      created,
		"dispatched": dispatched,
	})
}

// Update is kept as a compatibility delegate behind the V2
// /api/v1/source-objects/registry/:id facade.
func (h *RegistryHandler) Update(c *fiber.Ctx) error {
	id, err := strconv.ParseUint(c.Params("id"), 10, 32)
	if err != nil {
		return c.Status(400).JSON(fiber.Map{"error": "invalid id"})
	}

	existing, err := h.repo.GetByID(c.Context(), uint(id))
	if err != nil {
		return c.Status(404).JSON(fiber.Map{"error": "not found"})
	}

	var update struct {
		SyncEngine     *string `json:"sync_engine"`
		SyncInterval   *string `json:"sync_interval"`
		Priority       *string `json:"priority"`
		IsActive       *bool   `json:"is_active"`
		Notes          *string `json:"notes"`
		TimestampField *string `json:"timestamp_field"`
	}
	if err := c.BodyParser(&update); err != nil {
		return c.Status(400).JSON(fiber.Map{"error": err.Error()})
	}

	// Selective update — only changed fields to avoid column mismatch
	updates := map[string]interface{}{}
	if update.SyncEngine != nil {
		updates["sync_engine"] = *update.SyncEngine
		existing.SyncEngine = *update.SyncEngine
	}
	if update.SyncInterval != nil {
		updates["sync_interval"] = *update.SyncInterval
		existing.SyncInterval = *update.SyncInterval
	}
	if update.Priority != nil {
		updates["priority"] = *update.Priority
		existing.Priority = *update.Priority
	}
	if update.IsActive != nil {
		updates["is_active"] = *update.IsActive
		existing.IsActive = *update.IsActive
	}
	if update.Notes != nil {
		updates["notes"] = *update.Notes
		existing.Notes = update.Notes
	}
	// Bug B fix (2026-04-20): allow CMS to update Mongo timestamp field so
	// recon source agent can filter the right field (updated_at vs
	// lastUpdatedAt vs createdAt). Whitelist regexp guards against DB-level
	// mischief even though recon_source_agent.go re-validates on use.
	if update.TimestampField != nil {
		tsf := *update.TimestampField
		if !isValidTimestampField(tsf) {
			return c.Status(400).JSON(fiber.Map{
				"error": "invalid timestamp_field: must match [A-Za-z_][A-Za-z0-9_]{0,63}",
			})
		}
		updates["timestamp_field"] = tsf
		existing.TimestampField = &tsf
	}

	if len(updates) == 0 {
		return c.Status(400).JSON(fiber.Map{"error": "no fields to update"})
	}

	if h.bus == nil {
		return c.Status(503).JSON(fiber.Map{"error": "command bus not ready"})
	}

	user := middleware.GetUsername(c)
	cmd := commands.UpdateRegistryCommand{
		ID:             existing.ID,
		SyncEngine:     update.SyncEngine,
		SyncInterval:   update.SyncInterval,
		Priority:       update.Priority,
		IsActive:       update.IsActive,
		Notes:          update.Notes,
		TimestampField: update.TimestampField,
		UpdatedBy:      user,
	}
	ctx := messaging.WithMetadata(c.UserContext(), user, c.Get("X-Correlation-Id"), c.Get("Idempotency-Key"))
	res, err := h.bus.Execute(ctx, cmd)
	if err != nil {
		switch {
		case errors.Is(err, commands.ErrRegistryNotFound):
			return c.Status(404).JSON(fiber.Map{"error": "not found"})
		case errors.Is(err, commands.ErrRegistryNoFields):
			return c.Status(400).JSON(fiber.Map{"error": "no fields to update"})
		case errors.Is(err, commands.ErrRegistryInvalidTSField):
			return c.Status(400).JSON(fiber.Map{
				"error": "invalid timestamp_field: must match [A-Za-z_][A-Za-z0-9_]{0,63}",
			})
		default:
			return c.Status(500).JSON(fiber.Map{"error": "failed to update: " + err.Error()})
		}
	}

	if h.bus != nil {
		// Re-fetch so the v2 sync sees the post-update row state. Cheap
		// — single PK lookup; the alternative would be threading the
		// updated entry out of the bus result body which couples this
		// API to the command's wire shape.
		if updated, getErr := h.repo.GetByID(c.Context(), existing.ID); getErr == nil {
			syncCmd := commands.V2SyncCommand{Entry: updated}
			if _, syncErr := h.bus.Execute(ctx, syncCmd); syncErr != nil {
				h.logger.Error("post-update v2 sync failed", zap.Uint("registry_id", existing.ID), zap.Error(syncErr))
			}
		}
	}

	c.Set("Content-Type", "application/json")
	return c.Status(202).Send(res.ResultBody)
}

// BulkRegister is kept as a compatibility delegate behind the V2
// /api/v1/source-objects/register-batch facade.
func (h *RegistryHandler) BulkRegister(c *fiber.Ctx) error {
	var entries []model.TableRegistry
	if err := c.BodyParser(&entries); err != nil {
		return c.Status(400).JSON(fiber.Map{"error": err.Error()})
	}

	if h.bus == nil {
		return c.Status(503).JSON(fiber.Map{"error": "command bus not ready"})
	}

	user := middleware.GetUsername(c)
	registerCtx := messaging.WithMetadata(c.UserContext(), user, c.Get("X-Correlation-Id"), c.Get("Idempotency-Key"))
	registerCmd := commands.BulkRegisterRegistryCommand{Entries: entries, CreatedBy: user}
	res, err := h.bus.Execute(registerCtx, registerCmd)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": "bulk register failed: " + err.Error()})
	}

	var body struct {
		Message string                `json:"message"`
		Created int                   `json:"created"`
		Entries []model.TableRegistry `json:"entries"`
	}
	_ = json.Unmarshal(res.ResultBody, &body)

	dispatched := 0
	baseIdem := c.Get("Idempotency-Key")
	for _, e := range body.Entries {
		entryIdem := ""
		if baseIdem != "" {
			entryIdem = baseIdem + ":cdc:" + strconv.FormatUint(uint64(e.ID), 10)
		}
		dispatchCtx := messaging.WithMetadata(c.UserContext(), user, c.Get("X-Correlation-Id"), entryIdem)
		cmd := commands.CreateDefaultColumnsCommand{
			RegistryID:      e.ID,
			TargetTable:     e.TargetTable,
			SourceTable:     e.SourceTable,
			PrimaryKeyField: e.PrimaryKeyField,
			PrimaryKeyType:  e.PrimaryKeyType,
		}
		if _, derr := h.bus.Dispatch(dispatchCtx, cmd); derr != nil {
			h.logger.Warn("publish create-default-columns failed", zap.Error(derr), zap.String("table", e.TargetTable))
			continue
		}
		dispatched++
		if h.bus != nil {
			eCopy := e
			syncCmd := commands.V2SyncCommand{Entry: &eCopy}
			if _, err := h.bus.Execute(dispatchCtx, syncCmd); err != nil {
				h.logger.Error("bulk register v2 sync failed", zap.Uint("registry_id", e.ID), zap.Error(err))
			}
		}
	}

	return c.Status(202).JSON(fiber.Map{
		"message":    "tables registered — create-default-columns dispatched per entry",
		"created":    body.Created,
		"dispatched": dispatched,
	})
}

// GetStats is kept as a compatibility delegate for internal callers; the
// public CMS read surface now uses /api/v1/source-objects/stats.
func (h *RegistryHandler) GetStats(c *fiber.Ctx) error {
	stats, err := h.repo.GetStats(c.Context())
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": "failed to get stats"})
	}
	return c.JSON(stats)
}

// Standardize is kept as a compatibility delegate behind the V2
// /api/v1/source-objects/registry/:id/standardize facade.
func (h *RegistryHandler) Standardize(c *fiber.Ctx) error {
	id, err := strconv.ParseUint(c.Params("id"), 10, 32)
	if err != nil {
		return c.Status(400).JSON(fiber.Map{"error": "invalid id"})
	}

	entry, err := h.repo.GetByID(c.Context(), uint(id))
	if err != nil {
		return c.Status(404).JSON(fiber.Map{"error": "registry entry not found"})
	}

	user := middleware.GetUsername(c)
	ctx := messaging.WithMetadata(c.UserContext(), user, c.Get("X-Correlation-Id"), c.Get("Idempotency-Key"))
	cmd := commands.StandardizeCommand{
		RegistryID:  entry.ID,
		TargetTable: entry.TargetTable,
	}
	if _, derr := h.bus.Dispatch(ctx, cmd); derr != nil {
		h.activityLogger.LogAsync(service.ActivityEntry{
			Operation: "standardize", TargetTable: entry.TargetTable, Status: "error", ErrorMsg: derr.Error(),
		})
		return c.Status(500).JSON(fiber.Map{"error": "failed to dispatch standardize command: " + derr.Error()})
	}

	h.activityLogger.LogAsync(service.ActivityEntry{
		Operation: "standardize", TargetTable: entry.TargetTable, Status: "success",
		Details: map[string]any{"user": user},
	})
	return c.Status(202).JSON(fiber.Map{
		"message":      "standardize command accepted",
		"target_table": entry.TargetTable,
	})
}

// RefreshCatalog retired.
//
// Route intentionally unmounted in Debezium-only mode. Keep this marker so
// future maintainers know the old Swagger entry was removed on purpose.

// ScanFields is kept as a compatibility delegate behind the V2
// /api/v1/source-objects/registry/:id/scan-fields facade.
func (h *RegistryHandler) ScanFields(c *fiber.Ctx) error {
	id, err := strconv.ParseUint(c.Params("id"), 10, 32)
	if err != nil {
		return c.Status(400).JSON(fiber.Map{"error": "invalid id"})
	}

	entry, err := h.repo.GetByID(c.Context(), uint(id))
	if err != nil {
		return c.Status(404).JSON(fiber.Map{"error": "not found"})
	}

	// Debezium-native scan: worker looks up Mongo source via source_db +
	user := middleware.GetUsername(c)
	ctx := messaging.WithMetadata(c.UserContext(), user, c.Get("X-Correlation-Id"), c.Get("Idempotency-Key"))
	cmd := commands.ScanFieldsCommand{
		RegistryID:  entry.ID,
		SyncEngine:  entry.SyncEngine,
		SourceType:  entry.SourceType,
		SourceDB:    entry.SourceDB,
		SourceTable: entry.SourceTable,
		TargetTable: entry.TargetTable,
	}
	if _, derr := h.bus.Dispatch(ctx, cmd); derr != nil {
		h.activityLogger.LogAsync(service.ActivityEntry{
			Operation: "scan-fields", TargetTable: entry.TargetTable, Status: "error", ErrorMsg: derr.Error(),
		})
		return c.Status(500).JSON(fiber.Map{"error": "dispatch failed: " + derr.Error()})
	}

	h.activityLogger.LogAsync(service.ActivityEntry{
		Operation: "scan-fields", TargetTable: entry.TargetTable, Status: "accepted",
		Details: map[string]any{"user": user, "sync_engine": entry.SyncEngine},
	})

	return c.Status(202).JSON(fiber.Map{
		"message":      "scan-fields command accepted",
		"target_table": entry.TargetTable,
		"sync_engine":  entry.SyncEngine,
	})
}

// (removed: inferSQLType) — schema inference now lives in Worker

// SyncHealth returns overall sync health summary. Delegates the
// 5 aggregate counts to queries.GetSyncHealthHandler. The JSON
// surface stays byte-identical because queries.SyncHealthSnapshot
// owns the wire tags.
func (h *RegistryHandler) SyncHealth(c *fiber.Ctx) error {
	res, err := h.syncHealthQ.Handle(c.UserContext(), queries.GetSyncHealthQuery{})
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}
	return c.JSON(res.Snapshot)
}

func intQuery(c *fiber.Ctx, key string, defaultVal int) int {
	v, err := strconv.Atoi(c.Query(key))
	if err != nil || v <= 0 {
		return defaultVal
	}
	return v
}

// (removed: createMappingRulesFromSchema) — moved to Worker

// (removed: inferDataTypeFromSchema) — moved to Worker

// Transform triggers batch transformation of _raw_data → typed columns
func (h *RegistryHandler) Transform(c *fiber.Ctx) error {
	id, _ := c.ParamsInt("id")
	entry, err := h.repo.GetByID(c.Context(), uint(id))
	if err != nil {
		return c.Status(404).JSON(fiber.Map{"error": "registry entry not found"})
	}

	if err := h.natsClient.Conn.Publish("cdc.cmd.batch-transform", []byte(entry.TargetTable)); err != nil {
		h.activityLogger.LogAsync(service.ActivityEntry{
			Operation: "transform", TargetTable: entry.TargetTable, Status: "error", ErrorMsg: err.Error(),
		})
		return c.Status(500).JSON(fiber.Map{"error": "failed to dispatch transform command: " + err.Error()})
	}

	h.activityLogger.LogAsync(service.ActivityEntry{
		Operation: "transform", TargetTable: entry.TargetTable, Status: "success",
		Details: map[string]any{"user": middleware.GetUsername(c)},
	})

	return c.Status(202).JSON(fiber.Map{
		"message":      "transform command accepted",
		"target_table": entry.TargetTable,
	})
}

// TransformStatus returns the transform progress for a table
func (h *RegistryHandler) TransformStatus(c *fiber.Ctx) error {
	id, _ := c.ParamsInt("id")
	entry, err := h.repo.GetByID(c.Context(), uint(id))
	if err != nil {
		return c.Status(404).JSON(fiber.Map{"error": "registry entry not found"})
	}

	probe, err := h.bridgeReader.ProbeBridgeStatus(c.UserContext(), "public", entry.TargetTable)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}
	if !probe.Exists {
		return c.JSON(fiber.Map{
			"target_table":   entry.TargetTable,
			"total_rows":     0,
			"bridged_rows":   0,
			"pending_bridge": 0,
			"last_bridge_at": entry.LastBridgeAt,
			"status":         "table_not_created",
		})
	}

	return c.JSON(fiber.Map{
		"target_table":   entry.TargetTable,
		"total_rows":     probe.TotalRows,
		"bridged_rows":   probe.RawDataRows,
		"pending_bridge": probe.TotalRows - probe.RawDataRows,
		"last_bridge_at": entry.LastBridgeAt,
	})
}

// CreateDefaultColumns creates CDC table + adds all approved mapping rule columns in one step.
// This is the "tạo field default" action for Luồng 1.
func (h *RegistryHandler) CreateDefaultColumns(c *fiber.Ctx) error {
	id, _ := c.ParamsInt("id")
	entry, err := h.repo.GetByID(c.Context(), uint(id))
	if err != nil {
		return c.Status(404).JSON(fiber.Map{"error": "registry entry not found"})
	}

	user := middleware.GetUsername(c)
	ctx := messaging.WithMetadata(c.UserContext(), user, c.Get("X-Correlation-Id"), c.Get("Idempotency-Key"))
	cmd := commands.CreateDefaultColumnsCommand{
		RegistryID:      entry.ID,
		TargetTable:     entry.TargetTable,
		SourceTable:     entry.SourceTable,
		PrimaryKeyField: entry.PrimaryKeyField,
		PrimaryKeyType:  entry.PrimaryKeyType,
	}
	if _, derr := h.bus.Dispatch(ctx, cmd); derr != nil {
		h.activityLogger.LogAsync(service.ActivityEntry{
			Operation: "create-default-columns", TargetTable: entry.TargetTable, Status: "error", ErrorMsg: derr.Error(),
		})
		return c.Status(500).JSON(fiber.Map{"error": "failed to dispatch: " + derr.Error()})
	}

	h.activityLogger.LogAsync(service.ActivityEntry{
		Operation: "create-default-columns", TargetTable: entry.TargetTable, Status: "success",
		Details: map[string]any{
			"pk_field": entry.PrimaryKeyField,
			"pk_type":  entry.PrimaryKeyType,
			"user":     user,
		},
	})

	return c.Status(202).JSON(fiber.Map{
		"message":      "create-default-columns command accepted",
		"target_table": entry.TargetTable,
	})
}

// DispatchStatus is kept as a compatibility delegate behind the V2
// /api/v1/source-objects/registry/:id/dispatch-status facade.
func (h *RegistryHandler) DispatchStatus(c *fiber.Ctx) error {
	id, err := strconv.ParseUint(c.Params("id"), 10, 32)
	if err != nil {
		return c.Status(400).JSON(fiber.Map{"error": "invalid id"})
	}
	entry, err := h.repo.GetByID(c.Context(), uint(id))
	if err != nil {
		return c.Status(404).JSON(fiber.Map{"error": "registry entry not found"})
	}

	subject := c.Query("subject")
	sinceStr := c.Query("since")

	// Normalize: subject can be either bare op ("scan-fields") or full subject ("cdc.cmd.scan-fields").
	op := subject
	op = strings.TrimPrefix(op, "cdc.cmd.")

	filter := service.ActivityFilter{TargetTable: entry.TargetTable, Operation: op, Limit: 50}
	if sinceStr != "" {
		if ts, err := time.Parse(time.RFC3339, sinceStr); err == nil {
			filter.Since = &ts
		}
	}

	entries, err := h.activityLogger.ListActivityLogs(c.UserContext(), filter)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": "query failed: " + err.Error()})
	}

	return c.JSON(fiber.Map{
		"target_table": entry.TargetTable,
		"operation":    op,
		"since":        sinceStr,
		"entries":      entries,
		"count":        len(entries),
	})
}

// DetectTimestampField dispatches a re-scan of the Mongo source collection
// so the worker can (re)pick the best timestamp field for recon windowing.
//
// Operators hit this when they see "SRC_FIELD_MISSING" or "timestamp_field
// confidence: low" on a row — the worker will sample the collection, score
// candidates (updated_at / lastUpdatedAt / createdAt / ...), and write the
// winner back into cdc_table_registry (timestamp_field,
// timestamp_field_source=auto, timestamp_field_confidence).
//
// Flow: CMS publishes → worker consumes cdc.cmd.detect-timestamp-field →
// worker updates registry row → next recon tick uses the new field.
//
// The public CMS route now lives under
// /api/v1/source-objects/registry/:id/detect-timestamp-field.
func (h *RegistryHandler) DetectTimestampField(c *fiber.Ctx) error {
	id, err := strconv.ParseUint(c.Params("id"), 10, 32)
	if err != nil {
		return c.Status(400).JSON(fiber.Map{"error": "invalid id"})
	}
	entry, err := h.repo.GetByID(c.Context(), uint(id))
	if err != nil {
		return c.Status(404).JSON(fiber.Map{"error": "not found"})
	}

	user := middleware.GetUsername(c)
	ctx := messaging.WithMetadata(c.UserContext(), user, c.Get("X-Correlation-Id"), c.Get("Idempotency-Key"))
	cmd := commands.DetectTimestampFieldCommand{
		RegistryID:  entry.ID,
		TargetTable: entry.TargetTable,
		SourceTable: entry.SourceTable,
		SourceDB:    entry.SourceDB,
		SourceType:  entry.SourceType,
	}
	if _, derr := h.bus.Dispatch(ctx, cmd); derr != nil {
		h.activityLogger.LogAsync(service.ActivityEntry{
			Operation: "detect-timestamp-field", TargetTable: entry.TargetTable, Status: "error", ErrorMsg: derr.Error(),
		})
		return c.Status(500).JSON(fiber.Map{"error": "dispatch failed: " + derr.Error()})
	}

	h.activityLogger.LogAsync(service.ActivityEntry{
		Operation: "detect-timestamp-field", TargetTable: entry.TargetTable, Status: "accepted",
		Details: map[string]any{"user": user, "source_table": entry.SourceTable},
	})

	return c.Status(202).JSON(fiber.Map{
		"message":      "timestamp field detection dispatched",
		"target_table": entry.TargetTable,
	})
}

// normalizeShadowIdent converts an arbitrary source_db value into a
// Postgres-safe identifier suffix used to derive shadow_<src> schema
// names. Lowercases letters; non-alphanumeric/underscore → underscore.
// No length cap here — caller's validateIdent enforces 63-byte limit
// at the schema layer.
func normalizeShadowIdent(s string) string {
	out := make([]byte, 0, len(s))
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= 'A' && c <= 'Z':
			out = append(out, c+32)
		case c >= 'a' && c <= 'z', c >= '0' && c <= '9', c == '_':
			out = append(out, c)
		default:
			out = append(out, '_')
		}
	}
	return string(out)
}

// isValidTimestampField returns true when the name is a safe Mongo field
// identifier: ^[A-Za-z_][A-Za-z0-9_]{0,63}$. Matches resolveTimestampField
// in centralized-data-service/internal/service/recon_source_agent.go so CMS
// and Worker agree on what is storable. Rejects dotted paths ($where, etc).
func isValidTimestampField(s string) bool {
	if s == "" || len(s) > 64 {
		return false
	}
	for i, r := range s {
		if r == '_' || (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') {
			continue
		}
		if i > 0 && r >= '0' && r <= '9' {
			continue
		}
		return false
	}
	return true
}
