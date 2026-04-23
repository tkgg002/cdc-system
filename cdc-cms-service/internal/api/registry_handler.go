package api

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"cdc-cms-service/internal/middleware"
	"cdc-cms-service/internal/model"
	"cdc-cms-service/internal/repository"
	"cdc-cms-service/pkgs/natsconn"

	"github.com/gofiber/fiber/v2"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

type RegistryHandler struct {
	repo        *repository.RegistryRepo
	mappingRepo *repository.MappingRuleRepo
	db          *gorm.DB
	natsClient  *natsconn.NatsClient
	logger      *zap.Logger
}

func NewRegistryHandler(repo *repository.RegistryRepo, mappingRepo *repository.MappingRuleRepo, db *gorm.DB, nats *natsconn.NatsClient, logger *zap.Logger) *RegistryHandler {
	return &RegistryHandler{
		repo:        repo,
		mappingRepo: mappingRepo,
		db:          db,
		natsClient:  nats,
		logger:      logger,
	}
}

// logAction writes an activity log entry for any CMS action
func (h *RegistryHandler) logAction(operation, targetTable, status string, details map[string]interface{}, errMsg string) {
	detailsJSON, _ := json.Marshal(details)
	now := time.Now()
	var errPtr *string
	if errMsg != "" {
		errPtr = &errMsg
	}
	h.db.Create(&model.ActivityLog{
		Operation:    operation,
		TargetTable:  targetTable,
		Status:       status,
		Details:      detailsJSON,
		ErrorMessage: errPtr,
		TriggeredBy:  "manual",
		StartedAt:    now,
		CompletedAt:  &now,
	})
}

// List godoc
// @Summary      List table registry
// @Description  Returns all registered CDC tables with pagination and filters
// @Tags         Table Registry
// @Produce      json
// @Param        source_db    query string false "Filter by source database"
// @Param        priority     query string false "Filter by priority" Enums(critical, high, normal, low)
// @Param        is_active    query string false "Filter by active status" Enums(true, false)
// @Param        page         query int    false "Page number" default(1)
// @Param        page_size    query int    false "Page size"   default(20)
// @Success      200 {object} map[string]interface{}
// @Failure      500 {object} map[string]string
// @Security     BearerAuth
// @Router       /api/registry [get]
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

// Register godoc
// @Summary      Register a new CDC table
// @Description  Registers a new table in the registry and auto-creates the CDC table in PostgreSQL
// @Tags         Table Registry
// @Accept       json
// @Produce      json
// @Param        body body model.TableRegistry true "Table registration details"
// @Success      201 {object} map[string]interface{}
// @Failure      400 {object} map[string]string
// @Failure      500 {object} map[string]string
// @Security     BearerAuth
// @Router       /api/registry [post]
func (h *RegistryHandler) Register(c *fiber.Ctx) error {
	var entry model.TableRegistry
	if err := c.BodyParser(&entry); err != nil {
		return c.Status(400).JSON(fiber.Map{"error": err.Error()})
	}

	// (discover schema + update connection) được dispatch async qua NATS.
	if err := h.repo.Create(c.Context(), &entry); err != nil {
		return c.Status(500).JSON(fiber.Map{"error": "failed to register table: " + err.Error()})
	}

	dispatched := []string{}

	createColsPayload, _ := json.Marshal(map[string]interface{}{
		"registry_id":       entry.ID,
		"target_table":      entry.TargetTable,
		"source_table":      entry.SourceTable,
		"primary_key_field": entry.PrimaryKeyField,
		"primary_key_type":  entry.PrimaryKeyType,
	})
	if err := h.natsClient.Conn.Publish("cdc.cmd.create-default-columns", createColsPayload); err != nil {
		h.logger.Warn("publish create-default-columns failed", zap.Error(err))
	} else {
		dispatched = append(dispatched, "cdc.cmd.create-default-columns")
	}

	// Legacy sync-register dispatch removed post-Sprint 4 — Debezium-native
	// flow registers via cdc_internal.table_registry + Master Registry UI.

	h.natsClient.PublishReload(entry.TargetTable, middleware.GetUsername(c), "register", "")
	h.logAction("register", entry.TargetTable, "accepted", map[string]interface{}{
		"user":       middleware.GetUsername(c),
		"dispatched": dispatched,
	}, "")

	return c.Status(202).JSON(fiber.Map{
		"message":    "table registered — external sync dispatched",
		"entry":      entry,
		"dispatched": dispatched,
	})
}


// Update godoc
// @Summary      Update table registry entry
// @Description  Updates sync_engine, sync_interval, priority, or is_active for a registered table
// @Tags         Table Registry
// @Accept       json
// @Produce      json
// @Param        id   path int true "Registry entry ID"
// @Param        body body object true "Fields to update" SchemaExample({"sync_engine":"debezium","priority":"critical"})
// @Success      200 {object} map[string]interface{}
// @Failure      400 {object} map[string]string
// @Failure      404 {object} map[string]string
// @Failure      500 {object} map[string]string
// @Security     BearerAuth
// @Router       /api/registry/{id} [patch]
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

	if err := h.db.Model(&model.TableRegistry{}).Where("id = ?", existing.ID).Updates(updates).Error; err != nil {
		return c.Status(500).JSON(fiber.Map{"error": "failed to update: " + err.Error()})
	}

	// Khi inactive → active: auto approve tất cả mapping rules của stream
	if update.IsActive != nil && *update.IsActive {
		result := h.db.Model(&model.MappingRule{}).
			Where("source_table = ? AND status != ?", existing.SourceTable, "approved").
			Updates(map[string]interface{}{
				"status":    "approved",
				"is_active": true,
			})
		if result.RowsAffected > 0 {
			h.logAction("auto-approve-fields", existing.TargetTable, "success", map[string]interface{}{
				"fields_approved": result.RowsAffected,
				"source_table":    existing.SourceTable,
				"trigger":         "inactive→active",
			}, "")
			h.natsClient.PublishReload(existing.TargetTable, middleware.GetUsername(c), "auto_approve", "")
		}
	}

	// Legacy state-sync dispatch retired Sprint 4 4A.2. Modern Debezium
	// state is managed via Connect REST at /api/v1/system/connectors.
	dispatched := []string{}

	// Activity Log
	details := map[string]interface{}{"updates": updates, "user": middleware.GetUsername(c), "dispatched": dispatched}
	detailsJSON, _ := json.Marshal(details)
	now := time.Now()
	h.db.Create(&model.ActivityLog{
		Operation:   "registry-update",
		TargetTable: existing.TargetTable,
		Status:      "accepted",
		Details:     detailsJSON,
		TriggeredBy: "manual",
		StartedAt:   now,
		CompletedAt: &now,
	})

	h.natsClient.PublishReload(existing.TargetTable, middleware.GetUsername(c), "update", "")
	return c.Status(202).JSON(fiber.Map{
		"message":    "updated — external state dispatched",
		"entry":      existing,
		"dispatched": dispatched,
	})
}

// BulkRegister godoc
// @Summary      Bulk register tables
// @Description  Registers multiple tables at once and creates their CDC tables
// @Tags         Table Registry
// @Accept       json
// @Produce      json
// @Param        body body []model.TableRegistry true "Array of table registrations"
// @Success      201 {object} map[string]interface{}
// @Failure      400 {object} map[string]string
// @Failure      500 {object} map[string]string
// @Security     BearerAuth
// @Router       /api/registry/batch [post]
func (h *RegistryHandler) BulkRegister(c *fiber.Ctx) error {
	var entries []model.TableRegistry
	if err := c.BodyParser(&entries); err != nil {
		return c.Status(400).JSON(fiber.Map{"error": err.Error()})
	}

	created, err := h.repo.BulkCreate(c.Context(), entries)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": "bulk register failed: " + err.Error()})
	}

	// Dispatch create-default-columns per entry thay vì chạy SQL blocking
	// `create_all_pending_cdc_tables()`.
	dispatched := 0
	// Re-query created entries để có ID chính xác
	var createdEntries []model.TableRegistry
	tables := make([]string, 0, len(entries))
	for _, e := range entries {
		tables = append(tables, e.TargetTable)
	}
	h.db.Where("target_table IN ?", tables).Find(&createdEntries)
	for _, e := range createdEntries {
		payload, _ := json.Marshal(map[string]interface{}{
			"registry_id":       e.ID,
			"target_table":      e.TargetTable,
			"source_table":      e.SourceTable,
			"primary_key_field": e.PrimaryKeyField,
			"primary_key_type":  e.PrimaryKeyType,
		})
		if err := h.natsClient.Conn.Publish("cdc.cmd.create-default-columns", payload); err != nil {
			h.logger.Warn("publish create-default-columns failed", zap.Error(err), zap.String("table", e.TargetTable))
			continue
		}
		dispatched++
	}

	h.natsClient.PublishReload("*", middleware.GetUsername(c), "bulk_register", "")
	h.logAction("bulk-register", "*", "accepted", map[string]interface{}{
		"user":       middleware.GetUsername(c),
		"created":    created,
		"dispatched": dispatched,
	}, "")

	return c.Status(202).JSON(fiber.Map{
		"message":    "tables registered — create-default-columns dispatched per entry",
		"created":    created,
		"dispatched": dispatched,
	})
}

// GetStats godoc
// @Summary      Get registry statistics
// @Description  Returns summary stats: total tables, by source_db, by sync_engine, by priority
// @Tags         Table Registry
// @Produce      json
// @Success      200 {object} repository.RegistryStats
// @Failure      500 {object} map[string]string
// @Security     BearerAuth
// @Router       /api/registry/stats [get]
func (h *RegistryHandler) GetStats(c *fiber.Ctx) error {
	stats, err := h.repo.GetStats(c.Context())
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": "failed to get stats"})
	}
	return c.JSON(stats)
}

// GetStatus godoc
// @Tags         Table Registry
// @Produce      json
// @Param        id   path int true "Registry entry ID"
// @Success      200 {object} map[string]interface{}
// @Failure      400 {object} map[string]string
// @Failure      404 {object} map[string]string
// @Failure      500 {object} map[string]string
// @Security     BearerAuth
// @Router       /api/registry/{id}/status [get]
func (h *RegistryHandler) GetStatus(c *fiber.Ctx) error {
	id, err := strconv.ParseUint(c.Params("id"), 10, 32)
	if err != nil {
		return c.Status(400).JSON(fiber.Map{"error": "invalid id"})
	}

	entry, err := h.repo.GetByID(c.Context(), uint(id))
	if err != nil {
		return c.Status(404).JSON(fiber.Map{"error": "not found"})
	}

	return c.Status(410).JSON(fiber.Map{
		"error":       "legacy status endpoint retired",
		"sync_engine": entry.SyncEngine,
		"hint":        "use GET /api/v1/system/connectors[/:name] for Debezium connector status",
	})
}

// Standardize godoc
// @Summary      Standardize table metadata columns
// @Description  Adds missing metadata columns (_raw_data, _source, etc.) to an existing table
// @Tags         Table Registry
// @Produce      json
// @Param        id   path int true "Registry entry ID"
// @Success      200 {object} map[string]interface{}
// @Failure      400 {object} map[string]string
// @Failure      404 {object} map[string]string
// @Failure      500 {object} map[string]string
// @Security     BearerAuth
// @Router       /api/registry/{id}/standardize [post]
func (h *RegistryHandler) Standardize(c *fiber.Ctx) error {
	id, err := strconv.ParseUint(c.Params("id"), 10, 32)
	if err != nil {
		return c.Status(400).JSON(fiber.Map{"error": "invalid id"})
	}

	entry, err := h.repo.GetByID(c.Context(), uint(id))
	if err != nil {
		return c.Status(404).JSON(fiber.Map{"error": "registry entry not found"})
	}

	payload, _ := json.Marshal(map[string]interface{}{
		"registry_id":  entry.ID,
		"target_table": entry.TargetTable,
	})
	if err := h.natsClient.Conn.Publish("cdc.cmd.standardize", payload); err != nil {
		h.logAction("standardize", entry.TargetTable, "error", nil, err.Error())
		return c.Status(500).JSON(fiber.Map{"error": "failed to dispatch standardize command: " + err.Error()})
	}

	h.logAction("standardize", entry.TargetTable, "success", map[string]interface{}{"user": middleware.GetUsername(c)}, "")
	return c.Status(202).JSON(fiber.Map{
		"message":      "standardize command accepted",
		"target_table": entry.TargetTable,
	})
}

// Discover godoc
// @Summary      Discover field mappings from database
// @Description  Scans database columns and populates cdc_mapping_rules for the table
// @Tags         Table Registry
// @Produce      json
// @Param        id   path int true "Registry entry ID"
// @Success      200 {object} map[string]interface{}
// @Failure      400 {object} map[string]string
// @Failure      404 {object} map[string]string
// @Failure      500 {object} map[string]string
// @Security     BearerAuth
// @Router       /api/registry/{id}/discover [post]
func (h *RegistryHandler) Discover(c *fiber.Ctx) error {
	id, err := strconv.ParseUint(c.Params("id"), 10, 32)
	if err != nil {
		return c.Status(400).JSON(fiber.Map{"error": "invalid id"})
	}

	entry, err := h.repo.GetByID(c.Context(), uint(id))
	if err != nil {
		return c.Status(404).JSON(fiber.Map{"error": "registry entry not found"})
	}

	payload, _ := json.Marshal(map[string]interface{}{
		"registry_id":  entry.ID,
		"target_table": entry.TargetTable,
		"source_table": entry.SourceTable,
	})
	if err := h.natsClient.Conn.Publish("cdc.cmd.discover", payload); err != nil {
		h.logAction("discover", entry.TargetTable, "error", nil, err.Error())
		return c.Status(500).JSON(fiber.Map{"error": "failed to dispatch discover command: " + err.Error()})
	}

	h.logAction("discover", entry.TargetTable, "success", map[string]interface{}{"user": middleware.GetUsername(c)}, "")
	return c.Status(202).JSON(fiber.Map{
		"message":      "discover command accepted",
		"target_table": entry.TargetTable,
		"source_table": entry.SourceTable,
	})
}


// RefreshCatalog godoc
//
//	Use this when a stream was accidentally removed from catalog or when new tables are added to the source DB.
//
// @Tags         Table Registry
// @Produce      json
// @Param        id   path int true "Registry entry ID"
// @Success      202 {object} map[string]interface{}
// @Failure      400 {object} map[string]string
// @Failure      404 {object} map[string]string
// @Failure      500 {object} map[string]string
// @Security     BearerAuth
// @Router       /api/registry/{id}/refresh-catalog [post]

// Sync godoc
// @Tags         Table Registry
// @Produce      json
// @Param        id   path int true "Registry entry ID"
// @Success      200 {object} map[string]interface{}
// @Failure      400 {object} map[string]string
// @Failure      500 {object} map[string]string
// @Security     BearerAuth
// @Router       /api/registry/{id}/sync [post]
func (h *RegistryHandler) Sync(c *fiber.Ctx) error {
	return c.Status(410).JSON(fiber.Map{
		"error": "sync endpoint retired — use Debezium Command Center",
	})
}

// GetJobs godoc
// @Description  Returns the 10 most recent sync jobs for the connection associated with this table
// @Tags         Table Registry
// @Produce      json
// @Param        id   path int true "Registry entry ID"
// @Success      200 {object} []map[string]interface{}
// @Failure      400 {object} map[string]string
// @Failure      500 {object} map[string]string
// @Security     BearerAuth
// @Router       /api/registry/{id}/jobs [get]
func (h *RegistryHandler) GetJobs(c *fiber.Ctx) error {
	id, err := strconv.ParseUint(c.Params("id"), 10, 32)
	if err != nil {
		return c.Status(400).JSON(fiber.Map{"error": "invalid id"})
	}

	entry, err := h.repo.GetByID(c.Context(), uint(id))
	if err != nil {
		return c.Status(404).JSON(fiber.Map{"error": "not found"})
	}

	_ = entry
	return c.Status(410).JSON(fiber.Map{
		"error": "legacy jobs endpoint retired — use Debezium Command Center",
	})
}

// ScanSource godoc
// @Tags         Table Registry
// @Produce      json
// @Success      200 {object} map[string]interface{}
// @Failure      400 {object} map[string]string
// @Failure      500 {object} map[string]string
// @Security     BearerAuth
// @Router       /api/registry/scan-source [post]
func (h *RegistryHandler) ScanSource(c *fiber.Ctx) error {
	sourceID := c.Query("source_id")
	if sourceID == "" {
		return c.Status(400).JSON(fiber.Map{"error": "source_id is required"})
	}

	payload, _ := json.Marshal(map[string]interface{}{
		"legacy_source_id": sourceID,
		"triggered_by":      middleware.GetUsername(c),
	})
	if err := h.natsClient.Conn.Publish("cdc.cmd.scan-source", payload); err != nil {
		h.logAction("scan-source", sourceID, "error", nil, err.Error())
		return c.Status(500).JSON(fiber.Map{"error": "dispatch failed: " + err.Error()})
	}

	h.logAction("scan-source", sourceID, "accepted", map[string]interface{}{
		"user":              middleware.GetUsername(c),
		"legacy_source_id": sourceID,
	}, "")

	return c.Status(202).JSON(fiber.Map{
		"message":           "scan-source command accepted",
		"legacy_source_id": sourceID,
	})
}

// ScanFields godoc
// @Summary      Scan source fields for a registered table
// @Tags         Table Registry
// @Produce      json
// @Param        id   path int true "Registry entry ID"
// @Success      200 {object} map[string]interface{}
// @Failure      400 {object} map[string]string
// @Failure      404 {object} map[string]string
// @Failure      500 {object} map[string]string
// @Security     BearerAuth
// @Router       /api/registry/{id}/scan-fields [post]
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
	payload, _ := json.Marshal(map[string]interface{}{
		"registry_id":  entry.ID,
		"sync_engine":  entry.SyncEngine,
		"source_type":  entry.SourceType,
		"source_db":    entry.SourceDB,
		"source_table": entry.SourceTable,
		"target_table": entry.TargetTable,
	})
	if err := h.natsClient.Conn.Publish("cdc.cmd.scan-fields", payload); err != nil {
		h.logAction("scan-fields", entry.TargetTable, "error", nil, err.Error())
		return c.Status(500).JSON(fiber.Map{"error": "dispatch failed: " + err.Error()})
	}

	h.logAction("scan-fields", entry.TargetTable, "accepted", map[string]interface{}{
		"user":        middleware.GetUsername(c),
		"sync_engine": entry.SyncEngine,
	}, "")

	return c.Status(202).JSON(fiber.Map{
		"message":      "scan-fields command accepted",
		"target_table": entry.TargetTable,
		"sync_engine":  entry.SyncEngine,
	})
}

// (removed: inferSQLType) — schema inference now lives in Worker

// SyncHealth returns overall sync health summary
func (h *RegistryHandler) SyncHealth(c *fiber.Ctx) error {
	var totalRegistry, activeRegistry, tablesCreated int64
	var pendingRules, approvedRules int64

	h.db.Model(&model.TableRegistry{}).Count(&totalRegistry)
	h.db.Model(&model.TableRegistry{}).Where("is_active = ?", true).Count(&activeRegistry)
	h.db.Model(&model.TableRegistry{}).Where("is_table_created = ?", true).Count(&tablesCreated)

	h.db.Table("cdc_mapping_rules").Where("status = ?", "pending").Count(&pendingRules)
	h.db.Table("cdc_mapping_rules").Where("status = ?", "approved").Count(&approvedRules)

	return c.JSON(fiber.Map{
		"total_registered_cms":   totalRegistry,
		"active_tables":          activeRegistry,
		"tables_created":         tablesCreated,
		"pending_mapping_rules":  pendingRules,
		"approved_mapping_rules": approvedRules,
	})
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


// Bridge retired per Sprint 4 4A.2 — legacy pipeline removed. Use
// Debezium Command Center + Transmuter (plan v2 §R6) instead.
func (h *RegistryHandler) Bridge(c *fiber.Ctx) error {
	return c.Status(410).JSON(fiber.Map{
		"error": "bridge endpoint retired — use POST /api/v1/tables/:name/transmute",
	})
}

// Transform triggers batch transformation of _raw_data → typed columns
func (h *RegistryHandler) Transform(c *fiber.Ctx) error {
	id, _ := c.ParamsInt("id")
	entry, err := h.repo.GetByID(c.Context(), uint(id))
	if err != nil {
		return c.Status(404).JSON(fiber.Map{"error": "registry entry not found"})
	}

	if err := h.natsClient.Conn.Publish("cdc.cmd.batch-transform", []byte(entry.TargetTable)); err != nil {
		h.logAction("transform", entry.TargetTable, "error", nil, err.Error())
		return c.Status(500).JSON(fiber.Map{"error": "failed to dispatch transform command: " + err.Error()})
	}

	h.logAction("transform", entry.TargetTable, "success", map[string]interface{}{"user": middleware.GetUsername(c)}, "")

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

	// Check table exists
	var tableExists bool
	h.db.Raw("SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name = ? AND table_schema = 'public')", entry.TargetTable).Scan(&tableExists)
	if !tableExists {
		return c.JSON(fiber.Map{
			"target_table":   entry.TargetTable,
			"total_rows":     0,
			"bridged_rows":   0,
			"pending_bridge": 0,
			"last_bridge_at": entry.LastBridgeAt,
			"status":         "table_not_created",
		})
	}

	var totalRows, rawDataRows int64
	h.db.Raw(fmt.Sprintf(`SELECT COUNT(*) FROM "%s"`, entry.TargetTable)).Scan(&totalRows)

	// Check _raw_data column exists
	var hasRawData bool
	h.db.Raw("SELECT EXISTS(SELECT 1 FROM information_schema.columns WHERE table_name = ? AND column_name = '_raw_data')", entry.TargetTable).Scan(&hasRawData)
	if hasRawData {
		h.db.Raw(fmt.Sprintf(`SELECT COUNT(*) FROM "%s" WHERE _raw_data IS NOT NULL AND _raw_data != '{}'::jsonb`, entry.TargetTable)).Scan(&rawDataRows)
	}

	return c.JSON(fiber.Map{
		"target_table":   entry.TargetTable,
		"total_rows":     totalRows,
		"bridged_rows":   rawDataRows,
		"pending_bridge": totalRows - rawDataRows,
		"last_bridge_at": entry.LastBridgeAt,
	})
}

// Reconciliation retired per Sprint 4 4A.2 — compared rows of legacy raw
// tables vs CDC tables. Modern reconciliation lives at
// /api/v1/tables + /api/v1/system/connectors (Debezium Command Center)
// and the Transmuter's last_stats field on cdc_internal.transmute_schedule.
func (h *RegistryHandler) Reconciliation(c *fiber.Ctx) error {
	return c.Status(410).JSON(fiber.Map{
		"error": "reconciliation endpoint retired — use /api/v1/tables + Command Center",
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

	payload, _ := json.Marshal(map[string]interface{}{
		"registry_id":      entry.ID,
		"target_table":     entry.TargetTable,
		"source_table":     entry.SourceTable,
		"primary_key_field": entry.PrimaryKeyField,
		"primary_key_type":  entry.PrimaryKeyType,
	})

	if err := h.natsClient.Conn.Publish("cdc.cmd.create-default-columns", payload); err != nil {
		h.logAction("create-default-columns", entry.TargetTable, "error", nil, err.Error())
		return c.Status(500).JSON(fiber.Map{"error": "failed to dispatch: " + err.Error()})
	}

	h.logAction("create-default-columns", entry.TargetTable, "success", map[string]interface{}{
		"pk_field": entry.PrimaryKeyField,
		"pk_type":  entry.PrimaryKeyType,
		"user":     middleware.GetUsername(c),
	}, "")

	return c.Status(202).JSON(fiber.Map{
		"message":      "create-default-columns command accepted",
		"target_table": entry.TargetTable,
	})
}

// DropGINIndex triggers dropping the GIN index on _raw_data for a fully-transformed table.
func (h *RegistryHandler) DropGINIndex(c *fiber.Ctx) error {
	id, _ := c.ParamsInt("id")
	entry, err := h.repo.GetByID(c.Context(), uint(id))
	if err != nil {
		return c.Status(404).JSON(fiber.Map{"error": "registry entry not found"})
	}

	if err := h.natsClient.Conn.Publish("cdc.cmd.drop-gin-index", []byte(entry.TargetTable)); err != nil {
		return c.Status(500).JSON(fiber.Map{"error": "failed to dispatch: " + err.Error()})
	}

	return c.Status(202).JSON(fiber.Map{
		"message":      "drop-gin-index command accepted",
		"target_table": entry.TargetTable,
	})
}

// DispatchStatus godoc
// @Summary      Poll status of a dispatched async command
// @Description  Reads cdc_activity_log entries for a registry entry filtered by operation/subject.
//               Returns list of status transitions (accepted, running, success, error).
// @Tags         Table Registry
// @Produce      json
// @Param        id      path  int    true  "Registry entry ID"
// @Param        subject query string false "NATS subject or operation filter (e.g. cdc.cmd.scan-fields)"
// @Param        since   query string false "RFC3339 timestamp; only entries after this are returned"
// @Success      200 {object} map[string]interface{}
// @Failure      400 {object} map[string]string
// @Failure      404 {object} map[string]string
// @Security     BearerAuth
// @Router       /api/registry/{id}/dispatch-status [get]
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

	q := h.db.Model(&model.ActivityLog{}).
		Where("target_table = ?", entry.TargetTable)
	if op != "" {
		q = q.Where("operation = ?", op)
	}
	if sinceStr != "" {
		if ts, err := time.Parse(time.RFC3339, sinceStr); err == nil {
			q = q.Where("started_at >= ?", ts)
		}
	}

	var entries []model.ActivityLog
	if err := q.Order("started_at DESC").Limit(50).Find(&entries).Error; err != nil {
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
// POST /api/registry/:id/detect-timestamp-field
// Response: 202 Accepted with the target_table so FE can poll.
func (h *RegistryHandler) DetectTimestampField(c *fiber.Ctx) error {
	id, err := strconv.ParseUint(c.Params("id"), 10, 32)
	if err != nil {
		return c.Status(400).JSON(fiber.Map{"error": "invalid id"})
	}
	entry, err := h.repo.GetByID(c.Context(), uint(id))
	if err != nil {
		return c.Status(404).JSON(fiber.Map{"error": "not found"})
	}

	payload, _ := json.Marshal(map[string]interface{}{
		"registry_id":  entry.ID,
		"target_table": entry.TargetTable,
		"source_table": entry.SourceTable,
		"source_db":    entry.SourceDB,
		"source_type":  entry.SourceType,
	})
	if err := h.natsClient.Conn.Publish("cdc.cmd.detect-timestamp-field", payload); err != nil {
		h.logAction("detect-timestamp-field", entry.TargetTable, "error", nil, err.Error())
		return c.Status(500).JSON(fiber.Map{"error": "dispatch failed: " + err.Error()})
	}

	h.logAction("detect-timestamp-field", entry.TargetTable, "accepted", map[string]interface{}{
		"user":         middleware.GetUsername(c),
		"source_table": entry.SourceTable,
	}, "")

	return c.Status(202).JSON(fiber.Map{
		"message":      "timestamp field detection dispatched",
		"target_table": entry.TargetTable,
	})
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
