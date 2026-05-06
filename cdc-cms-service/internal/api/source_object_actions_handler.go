package api

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"cdc-cms-service/internal/app/commands"
	"cdc-cms-service/internal/app/ports"
	"cdc-cms-service/internal/infra/messaging"
	"cdc-cms-service/internal/middleware"
	"cdc-cms-service/internal/service"
	"github.com/gofiber/fiber/v2"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

// SourceObjectActionsHandler exposes V2-namespace endpoints that operate
// directly on cdc_system.source_object_registry + shadow_binding without
// going through the legacy table_registry bridge. T14 P4 removed the
// thin-delegate methods that used to forward to RegistryHandler — those
// routes now mount RegistryHandler directly via the router.
type SourceObjectActionsHandler struct {
	db             *gorm.DB
	bus            ports.CommandBus
	activityLogger *service.ActivityLogger
	logger         *zap.Logger
}

func NewSourceObjectActionsHandler(
	db *gorm.DB,
	bus ports.CommandBus,
	activityLogger *service.ActivityLogger,
	logger *zap.Logger,
) *SourceObjectActionsHandler {
	return &SourceObjectActionsHandler{
		db:             db,
		bus:            bus,
		activityLogger: activityLogger,
		logger:         logger,
	}
}

type sourceObjectDispatchScope struct {
	SourceObjectID  int64  `gorm:"column:source_object_id"`
	TargetTable     string `gorm:"column:target_table"`
	ShadowSchema    string `gorm:"column:shadow_schema"`
	SourceDatabase  string `gorm:"column:source_database"`
	SourceTable     string `gorm:"column:source_table"`
	SourceType      string `gorm:"column:source_type"`
	PrimaryKeyField string `gorm:"column:primary_key_field"`
	PrimaryKeyType  string `gorm:"column:primary_key_type"`
}

func (h *SourceObjectActionsHandler) resolveDispatchScopeBySourceObjectID(id int64) (*sourceObjectDispatchScope, error) {
	var rows []sourceObjectDispatchScope
	err := h.db.Raw(`
		SELECT
			so.id AS source_object_id,
			sb.shadow_table AS target_table,
			COALESCE(sb.shadow_schema, 'public') AS shadow_schema,
			COALESCE(so.source_database, '') AS source_database,
			so.source_object_name AS source_table,
			so.source_engine_type AS source_type,
			so.primary_key_field AS primary_key_field,
			COALESCE(so.primary_key_type, '') AS primary_key_type
		FROM cdc_system.source_object_registry so
		LEFT JOIN cdc_system.shadow_binding sb
		  ON sb.source_object_id = so.id
		 AND sb.is_active = TRUE
		WHERE so.id = ?
		  AND so.is_active = TRUE
		ORDER BY sb.updated_at DESC NULLS LAST, sb.id DESC NULLS LAST
		LIMIT 2
	`, id).Scan(&rows).Error
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, gorm.ErrRecordNotFound
	}
	if len(rows) > 1 {
		return nil, fiber.NewError(fiber.StatusConflict, "ambiguous_source_object_scope")
	}
	if strings.TrimSpace(rows[0].TargetTable) == "" {
		return nil, fiber.NewError(fiber.StatusConflict, "source_object_has_no_active_shadow_binding")
	}
	return &rows[0], nil
}

// T14 P4 — Register / UpdateBridge thin-delegate methods removed.
// Routes /api/v1/source-objects/register + /api/v1/source-objects/registry/:id [patch]
// now mount RegistryHandler.Register / RegistryHandler.Update directly via router.

// UpdateV2 godoc
// @Summary      Update a V2 source object directly
// @Description  Updates source-object metadata directly in cdc_system for rows that no longer rely on the legacy registry bridge. Supported fields: is_active, timestamp_field, notes.
// @Tags         Source Objects
// @Accept       json
// @Produce      json
// @Param        id path int true "Source object ID"
// @Param        body body object true "Fields to update"
// @Success      200 {object} map[string]interface{}
// @Failure      400 {object} map[string]string
// @Failure      404 {object} map[string]string
// @Failure      500 {object} map[string]string
// @Security     BearerAuth
// @Router       /api/v1/source-objects/{id} [patch]
func (h *SourceObjectActionsHandler) UpdateV2(c *fiber.Ctx) error {
	id, err := strconv.ParseInt(c.Params("id"), 10, 64)
	if err != nil || id <= 0 {
		return c.Status(400).JSON(fiber.Map{"error": "invalid_source_object_id"})
	}

	var req struct {
		IsActive       *bool   `json:"is_active"`
		Notes          *string `json:"notes"`
		TimestampField *string `json:"timestamp_field"`
		Priority       *string `json:"priority"`
		SyncInterval   *string `json:"sync_interval"`
	}
	if err := c.BodyParser(&req); err != nil {
		return c.Status(400).JSON(fiber.Map{"error": err.Error()})
	}
	if req.Priority != nil || req.SyncInterval != nil {
		return c.Status(400).JSON(fiber.Map{"error": "priority/sync_interval still require legacy registry bridge"})
	}
	if h.bus == nil {
		return c.Status(503).JSON(fiber.Map{"error": "command bus not ready"})
	}

	user := middleware.GetUsername(c)
	cmd := commands.UpdateSourceObjectV2Command{
		ID:             id,
		IsActive:       req.IsActive,
		Notes:          req.Notes,
		TimestampField: req.TimestampField,
		UpdatedBy:      user,
	}
	ctx := messaging.WithMetadata(c.UserContext(), user, c.Get("X-Correlation-Id"), c.Get("Idempotency-Key"))

	res, err := h.bus.Execute(ctx, cmd)
	if err != nil {
		switch {
		case errors.Is(err, commands.ErrSourceObjectNotFound):
			return c.Status(404).JSON(fiber.Map{"error": "source_object_not_found"})
		case errors.Is(err, commands.ErrSourceObjectNoFields):
			return c.Status(400).JSON(fiber.Map{"error": "no_supported_fields_to_update"})
		case errors.Is(err, commands.ErrSourceObjectInvalidTSField):
			return c.Status(400).JSON(fiber.Map{"error": "invalid timestamp_field: must match [A-Za-z_][A-Za-z0-9_]{0,63}"})
		default:
			h.logger.Error("update v2 source object failed", zap.Int64("source_object_id", id), zap.Error(err))
			return c.Status(500).JSON(fiber.Map{"error": "update_v2_source_object_failed"})
		}
	}
	c.Type("application/json")
	return c.Status(200).Send(res.ResultBody)
}

// T14 P4 — BulkRegister / CreateDefaultColumns thin-delegate methods removed
// (mount registryHandler.BulkRegister / .CreateDefaultColumns trực tiếp).

// CreateDefaultColumnsV2 godoc
// @Summary      Create default columns for a V2 source object
// @Description  Dispatches create-default-columns using source_object_id and the active shadow binding. The worker receives schema-aware payload so post-create state can be reflected back into V2 metadata.
// @Tags         Source Objects
// @Accept       json
// @Produce      json
// @Param        id path int true "Source object ID"
// @Success      202 {object} map[string]interface{}
// @Failure      400 {object} map[string]string
// @Failure      404 {object} map[string]string
// @Failure      409 {object} map[string]string
// @Failure      500 {object} map[string]string
// @Security     BearerAuth
// @Router       /api/v1/source-objects/{id}/create-default-columns [post]
func (h *SourceObjectActionsHandler) CreateDefaultColumnsV2(c *fiber.Ctx) error {
	id, err := strconv.ParseInt(c.Params("id"), 10, 64)
	if err != nil || id <= 0 {
		return c.Status(400).JSON(fiber.Map{"error": "invalid_source_object_id"})
	}

	scope, err := h.resolveDispatchScopeBySourceObjectID(id)
	if err != nil {
		if ferr, ok := err.(*fiber.Error); ok {
			return c.Status(ferr.Code).JSON(fiber.Map{"error": ferr.Message})
		}
		if err == gorm.ErrRecordNotFound {
			return c.Status(404).JSON(fiber.Map{"error": "source_object_not_found"})
		}
		h.logger.Error("resolve source object create-default scope failed", zap.Int64("source_object_id", id), zap.Error(err))
		return c.Status(500).JSON(fiber.Map{"error": "resolve_source_object_scope_failed"})
	}

	user := middleware.GetUsername(c)
	ctx := messaging.WithMetadata(c.UserContext(), user, c.Get("X-Correlation-Id"), c.Get("Idempotency-Key"))
	cmd := commands.CreateDefaultColumnsCommand{
		SourceObjectID:  id,
		TargetTable:     scope.TargetTable,
		ShadowSchema:    scope.ShadowSchema,
		SourceTable:     scope.SourceTable,
		PrimaryKeyField: scope.PrimaryKeyField,
		PrimaryKeyType:  scope.PrimaryKeyType,
	}
	if _, derr := h.bus.Dispatch(ctx, cmd); derr != nil {
		h.activityLogger.LogAsync(service.ActivityEntry{
			Operation: "create-default-columns", TargetTable: scope.TargetTable, Status: "error", ErrorMsg: derr.Error(),
		})
		return c.Status(500).JSON(fiber.Map{"error": "failed to dispatch: " + derr.Error()})
	}

	h.activityLogger.LogAsync(service.ActivityEntry{
		Operation: "create-default-columns", TargetTable: scope.TargetTable, Status: "success",
		Details: map[string]any{
			"user":             middleware.GetUsername(c),
			"source_object_id": id,
			"pk_field":         scope.PrimaryKeyField,
			"pk_type":          scope.PrimaryKeyType,
			"path":             "v2_direct",
		},
	})

	return c.Status(202).JSON(fiber.Map{
		"message":          "create-default-columns command accepted",
		"source_object_id": id,
		"target_table":     scope.TargetTable,
		"shadow_schema":    scope.ShadowSchema,
	})
}

// T14 P4 — Standardize thin-delegate removed (route mounts registryHandler.Standardize).

// StandardizeV2 godoc
// @Summary      Standardize a V2 source object
// @Description  Dispatches standardize using source_object_id and the active shadow binding instead of the legacy registry bridge. Worker payload remains compatible because standardize only requires the resolved target table.
// @Tags         Source Objects
// @Accept       json
// @Produce      json
// @Param        id path int true "Source object ID"
// @Success      202 {object} map[string]interface{}
// @Failure      400 {object} map[string]string
// @Failure      404 {object} map[string]string
// @Failure      409 {object} map[string]string
// @Failure      500 {object} map[string]string
// @Security     BearerAuth
// @Router       /api/v1/source-objects/{id}/standardize [post]
func (h *SourceObjectActionsHandler) StandardizeV2(c *fiber.Ctx) error {
	id, err := strconv.ParseInt(c.Params("id"), 10, 64)
	if err != nil || id <= 0 {
		return c.Status(400).JSON(fiber.Map{"error": "invalid_source_object_id"})
	}

	scope, err := h.resolveDispatchScopeBySourceObjectID(id)
	if err != nil {
		if ferr, ok := err.(*fiber.Error); ok {
			return c.Status(ferr.Code).JSON(fiber.Map{"error": ferr.Message})
		}
		if err == gorm.ErrRecordNotFound {
			return c.Status(404).JSON(fiber.Map{"error": "source_object_not_found"})
		}
		h.logger.Error("resolve source object standardize scope failed", zap.Int64("source_object_id", id), zap.Error(err))
		return c.Status(500).JSON(fiber.Map{"error": "resolve_source_object_scope_failed"})
	}

	user := middleware.GetUsername(c)
	ctx := messaging.WithMetadata(c.UserContext(), user, c.Get("X-Correlation-Id"), c.Get("Idempotency-Key"))
	cmd := commands.StandardizeCommand{
		SourceObjectID: id,
		TargetTable:    scope.TargetTable,
		ShadowSchema:   scope.ShadowSchema,
	}
	if _, derr := h.bus.Dispatch(ctx, cmd); derr != nil {
		h.activityLogger.LogAsync(service.ActivityEntry{
			Operation: "standardize", TargetTable: scope.TargetTable, Status: "error", ErrorMsg: derr.Error(),
		})
		return c.Status(500).JSON(fiber.Map{"error": "failed to dispatch standardize command: " + derr.Error()})
	}

	h.activityLogger.LogAsync(service.ActivityEntry{
		Operation: "standardize", TargetTable: scope.TargetTable, Status: "success",
		Details: map[string]any{
			"user":             middleware.GetUsername(c),
			"source_object_id": id,
			"path":             "v2_direct",
		},
	})
	return c.Status(202).JSON(fiber.Map{
		"message":          "standardize command accepted",
		"source_object_id": id,
		"target_table":     scope.TargetTable,
	})
}

// T14 P4 — ScanFields thin-delegate removed (route mounts registryHandler.ScanFields).

// ScanFieldsV2 godoc
// @Summary      Scan fields for a V2 source object
// @Description  Dispatches scan-fields using source_object_id and the active shadow binding instead of the legacy registry bridge. Worker payload stays compatible with the current Debezium-only scan path.
// @Tags         Source Objects
// @Accept       json
// @Produce      json
// @Param        id path int true "Source object ID"
// @Success      202 {object} map[string]interface{}
// @Failure      400 {object} map[string]string
// @Failure      404 {object} map[string]string
// @Failure      409 {object} map[string]string
// @Failure      500 {object} map[string]string
// @Security     BearerAuth
// @Router       /api/v1/source-objects/{id}/scan-fields [post]
func (h *SourceObjectActionsHandler) ScanFieldsV2(c *fiber.Ctx) error {
	id, err := strconv.ParseInt(c.Params("id"), 10, 64)
	if err != nil || id <= 0 {
		return c.Status(400).JSON(fiber.Map{"error": "invalid_source_object_id"})
	}

	scope, err := h.resolveDispatchScopeBySourceObjectID(id)
	if err != nil {
		if ferr, ok := err.(*fiber.Error); ok {
			return c.Status(ferr.Code).JSON(fiber.Map{"error": ferr.Message})
		}
		if err == gorm.ErrRecordNotFound {
			return c.Status(404).JSON(fiber.Map{"error": "source_object_not_found"})
		}
		h.logger.Error("resolve source object scan scope failed", zap.Int64("source_object_id", id), zap.Error(err))
		return c.Status(500).JSON(fiber.Map{"error": "resolve_source_object_scope_failed"})
	}

	user := middleware.GetUsername(c)
	ctx := messaging.WithMetadata(c.UserContext(), user, c.Get("X-Correlation-Id"), c.Get("Idempotency-Key"))
	cmd := commands.ScanFieldsCommand{
		SourceObjectID: id,
		TargetTable:    scope.TargetTable,
		SourceTable:    scope.SourceTable,
		SyncEngine:     "debezium",
		SourceType:     scope.SourceType,
	}
	if _, derr := h.bus.Dispatch(ctx, cmd); derr != nil {
		h.activityLogger.LogAsync(service.ActivityEntry{
			Operation: "scan-fields", TargetTable: scope.TargetTable, Status: "error", ErrorMsg: derr.Error(),
		})
		return c.Status(500).JSON(fiber.Map{"error": "dispatch failed: " + derr.Error()})
	}

	h.activityLogger.LogAsync(service.ActivityEntry{
		Operation: "scan-fields", TargetTable: scope.TargetTable, Status: "accepted",
		Details: map[string]any{
			"user":             middleware.GetUsername(c),
			"source_object_id": id,
			"sync_engine":      "debezium",
			"path":             "v2_direct",
		},
	})

	return c.Status(202).JSON(fiber.Map{
		"message":          "scan-fields command accepted",
		"source_object_id": id,
		"target_table":     scope.TargetTable,
		"sync_engine":      "debezium",
	})
}

// T14 P4 — Transform / DispatchStatus thin-delegate removed (router mounts
// registryHandler.Transform / .DispatchStatus directly).

// DispatchStatusV2 godoc
// @Summary      Get dispatch status for a V2 source-object action
// @Description  Reads activity-log based dispatch status for direct V2 source-object actions such as timestamp re-detection, using source_object_id to resolve the current shadow target.
// @Tags         Source Objects
// @Produce      json
// @Param        id path int true "Source object ID"
// @Param        subject query string false "Operation filter, e.g. detect-timestamp-field"
// @Param        since query string false "RFC3339 timestamp lower bound"
// @Success      200 {object} map[string]interface{}
// @Failure      400 {object} map[string]string
// @Failure      404 {object} map[string]string
// @Failure      409 {object} map[string]string
// @Failure      500 {object} map[string]string
// @Security     BearerAuth
// @Router       /api/v1/source-objects/{id}/dispatch-status [get]
func (h *SourceObjectActionsHandler) DispatchStatusV2(c *fiber.Ctx) error {
	id, err := strconv.ParseInt(c.Params("id"), 10, 64)
	if err != nil || id <= 0 {
		return c.Status(400).JSON(fiber.Map{"error": "invalid_source_object_id"})
	}

	scope, err := h.resolveDispatchScopeBySourceObjectID(id)
	if err != nil {
		if ferr, ok := err.(*fiber.Error); ok {
			return c.Status(ferr.Code).JSON(fiber.Map{"error": ferr.Message})
		}
		if err == gorm.ErrRecordNotFound {
			return c.Status(404).JSON(fiber.Map{"error": "source_object_not_found"})
		}
		h.logger.Error("resolve source object dispatch scope failed", zap.Int64("source_object_id", id), zap.Error(err))
		return c.Status(500).JSON(fiber.Map{"error": "resolve_source_object_scope_failed"})
	}

	subject := strings.TrimSpace(strings.TrimPrefix(c.Query("subject"), "cdc.cmd."))
	sinceStr := strings.TrimSpace(c.Query("since"))

	q := h.db.Table("cdc_system.cdc_activity_log").Where("target_table = ?", scope.TargetTable)
	if subject != "" {
		q = q.Where("operation = ?", subject)
	}
	if sinceStr != "" {
		if ts, parseErr := time.Parse(time.RFC3339, sinceStr); parseErr == nil {
			q = q.Where("started_at >= ?", ts)
		}
	}

	var entries []map[string]interface{}
	if err := q.Order("started_at DESC").Limit(50).Find(&entries).Error; err != nil {
		h.logger.Error("query source object dispatch status failed", zap.Int64("source_object_id", id), zap.Error(err))
		return c.Status(500).JSON(fiber.Map{"error": "query_dispatch_status_failed"})
	}

	return c.JSON(fiber.Map{
		"source_object_id": id,
		"target_table":     scope.TargetTable,
		"operation":        subject,
		"since":            sinceStr,
		"entries":          entries,
		"count":            len(entries),
	})
}

// T14 P4 — DetectTimestampField thin-delegate removed (route mounts
// registryHandler.DetectTimestampField).

// DetectTimestampFieldV2 godoc
// @Summary      Re-detect timestamp field for a V2 source object
// @Description  Dispatches timestamp-field auto-detection using source_object_id and the active shadow binding instead of the legacy registry bridge. The worker still consumes the existing NATS subject and resolves by target_table fallback.
// @Tags         Source Objects
// @Accept       json
// @Produce      json
// @Param        id path int true "Source object ID"
// @Success      202 {object} map[string]interface{}
// @Failure      400 {object} map[string]string
// @Failure      404 {object} map[string]string
// @Failure      409 {object} map[string]string
// @Failure      500 {object} map[string]string
// @Security     BearerAuth
// @Router       /api/v1/source-objects/{id}/detect-timestamp-field [post]
func (h *SourceObjectActionsHandler) DetectTimestampFieldV2(c *fiber.Ctx) error {
	id, err := strconv.ParseInt(c.Params("id"), 10, 64)
	if err != nil || id <= 0 {
		return c.Status(400).JSON(fiber.Map{"error": "invalid_source_object_id"})
	}

	scope, err := h.resolveDispatchScopeBySourceObjectID(id)
	if err != nil {
		if ferr, ok := err.(*fiber.Error); ok {
			return c.Status(ferr.Code).JSON(fiber.Map{"error": ferr.Message})
		}
		if err == gorm.ErrRecordNotFound {
			return c.Status(404).JSON(fiber.Map{"error": "source_object_not_found"})
		}
		h.logger.Error("resolve source object detect scope failed", zap.Int64("source_object_id", id), zap.Error(err))
		return c.Status(500).JSON(fiber.Map{"error": "resolve_source_object_scope_failed"})
	}

	user := middleware.GetUsername(c)
	ctx := messaging.WithMetadata(c.UserContext(), user, c.Get("X-Correlation-Id"), c.Get("Idempotency-Key"))
	cmd := commands.DetectTimestampFieldCommand{
		SourceObjectID: id,
		TargetTable:    scope.TargetTable,
		SourceTable:    scope.SourceTable,
		SourceDB:       scope.SourceDatabase,
		SourceType:     scope.SourceType,
	}
	if _, derr := h.bus.Dispatch(ctx, cmd); derr != nil {
		h.activityLogger.LogAsync(service.ActivityEntry{
			Operation: "detect-timestamp-field", TargetTable: scope.TargetTable, Status: "error", ErrorMsg: derr.Error(),
		})
		return c.Status(500).JSON(fiber.Map{"error": "dispatch failed: " + derr.Error()})
	}

	h.activityLogger.LogAsync(service.ActivityEntry{
		Operation: "detect-timestamp-field", TargetTable: scope.TargetTable, Status: "accepted",
		Details: map[string]any{
			"user":             middleware.GetUsername(c),
			"source_object_id": id,
			"source_table":     scope.SourceTable,
			"path":             "v2_direct",
		},
	})

	return c.Status(202).JSON(fiber.Map{
		"message":          "timestamp field detection dispatched",
		"source_object_id": id,
		"target_table":     scope.TargetTable,
	})
}

// T14 P4 — TransformStatus thin-delegate removed (route mounts
// registryHandler.TransformStatus).

// TransformStatusV2 godoc
// @Summary      Get transform progress for a V2 source object
// @Description  Resolves the active shadow target for a source object and returns transform progress without requiring the legacy registry bridge.
// @Tags         Source Objects
// @Produce      json
// @Param        id path int true "Source object ID"
// @Success      200 {object} map[string]interface{}
// @Failure      400 {object} map[string]string
// @Failure      404 {object} map[string]string
// @Failure      409 {object} map[string]string
// @Failure      500 {object} map[string]string
// @Security     BearerAuth
// @Router       /api/v1/source-objects/{id}/transform-status [get]
func (h *SourceObjectActionsHandler) TransformStatusV2(c *fiber.Ctx) error {
	id, err := strconv.ParseInt(c.Params("id"), 10, 64)
	if err != nil || id <= 0 {
		return c.Status(400).JSON(fiber.Map{"error": "invalid_source_object_id"})
	}

	scope, err := h.resolveDispatchScopeBySourceObjectID(id)
	if err != nil {
		if ferr, ok := err.(*fiber.Error); ok {
			return c.Status(ferr.Code).JSON(fiber.Map{"error": ferr.Message})
		}
		if err == gorm.ErrRecordNotFound {
			return c.Status(404).JSON(fiber.Map{"error": "source_object_not_found"})
		}
		h.logger.Error("resolve source object transform scope failed", zap.Int64("source_object_id", id), zap.Error(err))
		return c.Status(500).JSON(fiber.Map{"error": "resolve_source_object_scope_failed"})
	}

	var tableExists bool
	h.db.Raw(
		"SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name = ? AND table_schema = ?)",
		scope.TargetTable,
		scope.ShadowSchema,
	).Scan(&tableExists)
	if !tableExists {
		return c.JSON(fiber.Map{
			"source_object_id": id,
			"shadow_schema":    scope.ShadowSchema,
			"target_table":     scope.TargetTable,
			"total_rows":       0,
			"bridged_rows":     0,
			"pending_bridge":   0,
			"status":           "table_not_created",
		})
	}

	var totalRows, rawDataRows int64
	h.db.Raw(fmt.Sprintf(`SELECT COUNT(*) FROM "%s"."%s"`, scope.ShadowSchema, scope.TargetTable)).Scan(&totalRows)

	var hasRawData bool
	h.db.Raw(
		"SELECT EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema = ? AND table_name = ? AND column_name = '_raw_data')",
		scope.ShadowSchema,
		scope.TargetTable,
	).Scan(&hasRawData)
	if hasRawData {
		h.db.Raw(fmt.Sprintf(`SELECT COUNT(*) FROM "%s"."%s" WHERE _raw_data IS NOT NULL AND _raw_data != '{}'::jsonb`, scope.ShadowSchema, scope.TargetTable)).Scan(&rawDataRows)
	}

	return c.JSON(fiber.Map{
		"source_object_id": id,
		"shadow_schema":    scope.ShadowSchema,
		"target_table":     scope.TargetTable,
		"total_rows":       totalRows,
		"bridged_rows":     rawDataRows,
		"pending_bridge":   totalRows - rawDataRows,
	})
}
