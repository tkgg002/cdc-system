package handler

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"time"

	"centralized-data-service/internal/model"
	"centralized-data-service/internal/repository"
	"centralized-data-service/internal/service"

	"github.com/nats-io/nats.go"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

// CommandHandler handles async CDC commands published from the API service via NATS.
// These commands operate on the DW (Data Warehouse) database, which only the worker can access.
type CommandHandler struct {
	db           *gorm.DB
	mappingRepo  *repository.MappingRuleRepo
	registryRepo *repository.RegistryRepo
	pendingRepo  *repository.PendingFieldRepo
	logger       *zap.Logger
	// kafkaConnectURL — base URL for Kafka Connect REST (boundary
	// refactor). Empty disables the Debezium-specific handlers.
	kafkaConnectURL string
	// natsConn — used by boundary-refactor handlers to publish async
	// result events (cdc.result.*). The NATS library does not expose the
	// connection from *nats.Subscription so we inject it explicitly.
	natsConn *nats.Conn
}

// SetKafkaConnectURL injects the Kafka Connect REST base URL (used by
// boundary refactor handlers HandleRestartDebezium / HandleSyncState).
// Called from worker_server wiring to keep NewCommandHandler signature
// stable.
func (h *CommandHandler) SetKafkaConnectURL(url string) {
	h.kafkaConnectURL = url
}

// SetNATSConn injects the shared NATS connection. Required by all
// boundary-refactor handlers that publish async results. Left optional
// so unit tests can swap a mock.
func (h *CommandHandler) SetNATSConn(conn *nats.Conn) {
	h.natsConn = conn
}

// CommandResult is the admin-facing result envelope. It must stay
// sanitized before being logged, stored in ActivityLog, or published to
// downstream control-plane consumers.
type CommandResult struct {
	Command      string `json:"command"`
	RegistryID   uint   `json:"registry_id,omitempty"`
	TargetTable  string `json:"target_table,omitempty"`
	RowsAffected int    `json:"rows_affected,omitempty"`
	Status       string `json:"status"`
	Error        string `json:"error,omitempty"`
}

func NewCommandHandler(db *gorm.DB, mappingRepo *repository.MappingRuleRepo, registryRepo *repository.RegistryRepo, pendingRepo *repository.PendingFieldRepo, logger *zap.Logger) *CommandHandler {
	return &CommandHandler{
		db:           db,
		mappingRepo:  mappingRepo,
		registryRepo: registryRepo,
		pendingRepo:  pendingRepo,
		logger:       logger,
	}
}

// Returns error if table doesn't exist. Safe to call multiple times (ADD COLUMN IF NOT EXISTS).
func (h *CommandHandler) ensureCDCColumns(tableName string) error {
	var exists bool
	h.db.Raw("SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name = ? AND table_schema = 'public')", tableName).Scan(&exists)
	if !exists {
		return fmt.Errorf("table %s does not exist", tableName)
	}

	cdcColumns := []struct{ name, def string }{
		{"_raw_data", "JSONB"},
		{"_source", "VARCHAR(20) DEFAULT 'cdc-legacy'"},
		{"_synced_at", "TIMESTAMP DEFAULT NOW()"},
		{"_version", "BIGINT DEFAULT 1"},
		{"_hash", "VARCHAR(64)"},
		{"_deleted", "BOOLEAN DEFAULT FALSE"},
		{"_created_at", "TIMESTAMP DEFAULT NOW()"},
		{"_updated_at", "TIMESTAMP DEFAULT NOW()"},
	}
	for _, col := range cdcColumns {
		h.db.Exec(fmt.Sprintf(`ALTER TABLE "%s" ADD COLUMN IF NOT EXISTS %s %s`, tableName, col.name, col.def))
	}
	return nil
}

// hasColumn checks if a column exists in a table
func (h *CommandHandler) hasColumn(tableName, columnName string) bool {
	var exists bool
	h.db.Raw("SELECT EXISTS(SELECT 1 FROM information_schema.columns WHERE table_name = ? AND column_name = ?)", tableName, columnName).Scan(&exists)
	return exists
}

// tableExists checks if a table exists
func (h *CommandHandler) tableExists(tableName string) bool {
	var exists bool
	h.db.Raw("SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name = ? AND table_schema = 'public')", tableName).Scan(&exists)
	return exists
}

// HandleStandardize subscribes to "cdc.cmd.standardize" and runs standardize_cdc_table() on the DW DB.
func (h *CommandHandler) HandleStandardize(msg *nats.Msg) {
	var payload struct {
		RegistryID  uint   `json:"registry_id"`
		TargetTable string `json:"target_table"`
	}
	if err := json.Unmarshal(msg.Data, &payload); err != nil {
		h.logger.Error("cdc.cmd.standardize: invalid payload", zap.Error(err))
		return
	}

	h.logger.Info("standardizing table", zap.String("table", payload.TargetTable))

	if err := h.db.WithContext(context.Background()).
		Exec("SELECT standardize_cdc_table(?)", payload.TargetTable).Error; err != nil {
		h.logger.Error("standardize failed",
			zap.String("table", payload.TargetTable),
			zap.Error(err),
		)
		h.publishResult(msg, CommandResult{
			Command:     "standardize",
			RegistryID:  payload.RegistryID,
			TargetTable: payload.TargetTable,
			Status:      "error",
			Error:       err.Error(),
		})
		return
	}

	h.logger.Info("standardize complete", zap.String("table", payload.TargetTable))
	h.publishResult(msg, CommandResult{
		Command:     "standardize",
		RegistryID:  payload.RegistryID,
		TargetTable: payload.TargetTable,
		Status:      "success",
	})
}

// HandleCreateDefaultColumns creates the CDC table + adds all approved mapping rule columns.
// This is the "tạo field default" action from Luồng 1.
// Subject: "cdc.cmd.create-default-columns"
func (h *CommandHandler) HandleCreateDefaultColumns(msg *nats.Msg) {
	var payload struct {
		RegistryID  uint   `json:"registry_id"`
		TargetTable string `json:"target_table"`
		SourceTable string `json:"source_table"`
		PKField     string `json:"primary_key_field"`
		PKType      string `json:"primary_key_type"`
	}
	if err := json.Unmarshal(msg.Data, &payload); err != nil {
		h.publishResult(msg, CommandResult{Command: "create-default-columns", Status: "error", Error: "invalid payload"})
		return
	}

	h.logger.Info("creating default columns", zap.String("table", payload.TargetTable))

	tableAlreadyExists := h.tableExists(payload.TargetTable)
	columnsAdded := 0

	if !tableAlreadyExists {
		// Table chưa tồn tại → tạo mới với create_cdc_table (CDC schema + approved fields)
		pkType := payload.PKType
		if pkType == "" {
			pkType = "BIGINT"
		}
		if err := h.db.Exec("SELECT create_cdc_table(?, ?, ?)", payload.TargetTable, payload.PKField, pkType).Error; err != nil {
			h.publishResult(msg, CommandResult{Command: "create-default-columns", TargetTable: payload.TargetTable, Status: "error", Error: "create table: " + err.Error()})
			return
		}

		// Thêm approved fields vào table mới
		rules, err := h.mappingRepo.GetByTable(context.Background(), payload.SourceTable)
		if err == nil {
			for _, rule := range rules {
				if !rule.IsActive {
					continue
				}
				alterSQL := fmt.Sprintf(`ALTER TABLE "%s" ADD COLUMN IF NOT EXISTS "%s" %s`,
					payload.TargetTable, rule.TargetColumn, rule.DataType)
				if err := h.db.Exec(alterSQL).Error; err != nil {
					h.logger.Warn("failed to add column", zap.String("column", rule.TargetColumn), zap.Error(err))
					continue
				}
				columnsAdded++
			}
		}

		// Update registry
		h.db.Model(&model.TableRegistry{}).Where("target_table = ?", payload.TargetTable).Update("is_table_created", true)

		h.logger.Info("table created with default columns",
			zap.String("table", payload.TargetTable),
			zap.Int("approved_fields", columnsAdded),
		)
	} else {
		if err := h.ensureCDCColumns(payload.TargetTable); err != nil {
			h.publishResult(msg, CommandResult{Command: "create-default-columns", TargetTable: payload.TargetTable, Status: "error", Error: err.Error()})
			return
		}
		columnsAdded = 8 // 8 CDC columns

		// Update registry
		h.db.Model(&model.TableRegistry{}).Where("target_table = ?", payload.TargetTable).Update("is_table_created", true)

		h.logger.Info("CDC system columns added to existing legacy table",
			zap.String("table", payload.TargetTable),
		)
	}

	h.publishResult(msg, CommandResult{
		Command:      "create-default-columns",
		TargetTable:  payload.TargetTable,
		RowsAffected: columnsAdded,
		Status:       "success",
	})
}

// HandleDiscover subscribes to "cdc.cmd.discover" and auto-generates mapping rules
// by scanning DW table columns via information_schema.
func (h *CommandHandler) HandleDiscover(msg *nats.Msg) {
	var payload struct {
		RegistryID  uint   `json:"registry_id"`
		TargetTable string `json:"target_table"`
		SourceTable string `json:"source_table"`
	}
	if err := json.Unmarshal(msg.Data, &payload); err != nil {
		h.logger.Error("cdc.cmd.discover: invalid payload", zap.Error(err))
		return
	}

	h.logger.Info("discovering mappings",
		zap.String("target", payload.TargetTable),
		zap.String("source", payload.SourceTable),
	)

	// 1. Get columns from DW information_schema
	type ColInfo struct {
		ColumnName string
		DataType   string
	}
	var cols []ColInfo
	if err := h.db.WithContext(context.Background()).Raw(`
		SELECT column_name, data_type
		FROM information_schema.columns
		WHERE table_name = ? AND table_schema = 'public'
		ORDER BY ordinal_position
	`, payload.TargetTable).Scan(&cols).Error; err != nil {
		h.logger.Error("discover: failed to get columns", zap.Error(err))
		h.publishResult(msg, CommandResult{
			Command:     "discover",
			RegistryID:  payload.RegistryID,
			TargetTable: payload.TargetTable,
			Status:      "error",
			Error:       fmt.Sprintf("get columns: %v", err),
		})
		return
	}

	// 2. Get existing mapping rules to avoid duplicates
	existing, err := h.mappingRepo.GetByTable(context.Background(), payload.SourceTable)
	if err != nil {
		h.logger.Error("discover: failed to get existing rules", zap.Error(err))
	}
	existingFields := make(map[string]bool, len(existing))
	for _, r := range existing {
		existingFields[r.SourceField] = true
	}

	// 3. Create new mapping rules for unknown columns
	count := 0
	for _, col := range cols {
		// Skip CDC metadata columns
		if strings.HasPrefix(col.ColumnName, "_") {
			continue
		}
		if existingFields[col.ColumnName] {
			continue
		}
		rule := &model.MappingRule{
			SourceTable:  payload.SourceTable,
			SourceField:  col.ColumnName,
			TargetColumn: col.ColumnName,
			DataType:     col.DataType,
			IsActive:     true,
			IsEnriched:   false,
			IsNullable:   true,
		}
		if err := h.mappingRepo.Create(context.Background(), rule); err != nil {
			h.logger.Error("discover: failed to create rule", zap.String("field", col.ColumnName), zap.Error(err))
			continue
		}
		count++
	}

	h.logger.Info("discover complete", zap.Int("new_rules", count), zap.String("table", payload.TargetTable))
	h.publishResult(msg, CommandResult{
		Command:      "discover",
		RegistryID:   payload.RegistryID,
		TargetTable:  payload.TargetTable,
		RowsAffected: count,
		Status:       "success",
	})
}

// HandleBackfill subscribes to "cdc.cmd.backfill" and populates target columns from _raw_data.
func (h *CommandHandler) HandleBackfill(msg *nats.Msg) {
	var payload struct {
		RegistryID   uint   `json:"registry_id"`
		TargetTable  string `json:"target_table"`
		SourceField  string `json:"source_field"`
		TargetColumn string `json:"target_column"`
		DataType     string `json:"data_type"`
	}
	if err := json.Unmarshal(msg.Data, &payload); err != nil {
		h.logger.Error("cdc.cmd.backfill: invalid payload", zap.Error(err))
		return
	}

	h.logger.Info("backfilling column",
		zap.String("table", payload.TargetTable),
		zap.String("field", payload.SourceField),
		zap.String("column", payload.TargetColumn),
	)

	// Build type-aware extraction expression
	castExpr := buildCastExpr(payload.SourceField, payload.DataType)

	// Update NULL target column values from _raw_data JSONB
	sql := fmt.Sprintf(
		`UPDATE "%s" SET %s = %s WHERE %s IS NULL AND _raw_data IS NOT NULL`,
		payload.TargetTable, payload.TargetColumn, castExpr, payload.TargetColumn,
	)
	result := h.db.WithContext(context.Background()).Exec(sql)
	if result.Error != nil {
		h.logger.Error("backfill failed",
			zap.String("table", payload.TargetTable),
			zap.Error(result.Error),
		)
		h.publishResult(msg, CommandResult{
			Command:     "backfill",
			RegistryID:  payload.RegistryID,
			TargetTable: payload.TargetTable,
			Status:      "error",
			Error:       result.Error.Error(),
		})
		return
	}

	rows := int(result.RowsAffected)
	h.logger.Info("backfill complete",
		zap.String("table", payload.TargetTable),
		zap.String("column", payload.TargetColumn),
		zap.Int("rows", rows),
	)
	h.publishResult(msg, CommandResult{
		Command:      "backfill",
		RegistryID:   payload.RegistryID,
		TargetTable:  payload.TargetTable,
		RowsAffected: rows,
		Status:       "success",
	})
}

// HandleIntrospect subscribes to "cdc.cmd.introspect" and scans a sample of _raw_data
// from the DW table to find unmapped fields. It replies via NATS Request-Reply.

// Debezium path is the sole CDC engine. Shadow→Master via TransmuterModule (R6).

// HandleBatchTransform applies mapping rules to populate typed columns from _raw_data.
// Subject: "cdc.cmd.batch-transform" (pub/sub pattern)
func (h *CommandHandler) HandleBatchTransform(msg *nats.Msg) {
	targetTable := string(msg.Data)
	h.logger.Info("batch transforming table", zap.String("table", targetTable))

	// 0. Check table exists + has _raw_data column
	if !h.tableExists(targetTable) {
		h.publishResult(msg, CommandResult{Command: "batch-transform", TargetTable: targetTable, Status: "skipped", Error: "table does not exist"})
		return
	}
	if !h.hasColumn(targetTable, "_raw_data") {
		h.publishResult(msg, CommandResult{Command: "batch-transform", TargetTable: targetTable, Status: "skipped", Error: "table has no _raw_data column yet"})
		return
	}

	// 1. Find source_table from registry
	reg, _ := h.registryRepo.GetByTargetTable(context.Background(), targetTable)
	var sourceTable string
	if reg != nil {
		sourceTable = reg.SourceTable
	} else {
		sourceTable = targetTable
	}

	// 2. Get active mapping rules
	rules, err := h.mappingRepo.GetByTable(context.Background(), sourceTable)
	if err != nil || len(rules) == 0 {
		h.publishResult(msg, CommandResult{
			Command:     "batch-transform",
			TargetTable: targetTable,
			Status:      "error",
			Error:       fmt.Sprintf("no active mapping rules for table %s (source: %s)", targetTable, sourceTable),
		})
		return
	}

	// 3. Build UPDATE SET clause from mapping rules
	var setClauses []string
	var whereClauses []string
	for _, rule := range rules {
		if !rule.IsActive {
			continue
		}
		castExpr := buildCastExpr(rule.SourceField, rule.DataType)
		setClauses = append(setClauses, fmt.Sprintf("%s = %s", rule.TargetColumn, castExpr))
		whereClauses = append(whereClauses, fmt.Sprintf("%s IS NULL", rule.TargetColumn))
	}

	if len(setClauses) == 0 {
		h.publishResult(msg, CommandResult{
			Command:     "batch-transform",
			TargetTable: targetTable,
			Status:      "success",
			Error:       "no active rules to transform",
		})
		return
	}

	setClauses = append(setClauses, "_updated_at = NOW()")

	// 4. Execute transform
	transformSQL := fmt.Sprintf(`UPDATE "%s" SET %s WHERE _raw_data IS NOT NULL AND (%s)`,
		targetTable,
		strings.Join(setClauses, ", "),
		strings.Join(whereClauses, " OR "),
	)

	result := h.db.Exec(transformSQL)
	if result.Error != nil {
		h.publishResult(msg, CommandResult{
			Command:     "batch-transform",
			TargetTable: targetTable,
			Status:      "error",
			Error:       result.Error.Error(),
		})
		return
	}

	h.logger.Info("batch transform completed",
		zap.String("table", targetTable),
		zap.Int64("rows_affected", result.RowsAffected),
	)
	h.publishResult(msg, CommandResult{
		Command:      "batch-transform",
		TargetTable:  targetTable,
		RowsAffected: int(result.RowsAffected),
		Status:       "success",
	})
}

// HandleScanRawData scans _raw_data JSONB column to find fields not yet mapped.
// Subject: "cdc.cmd.scan-raw-data" (request-reply pattern)
func (h *CommandHandler) HandleScanRawData(msg *nats.Msg) {
	targetTable := string(msg.Data)
	h.logger.Info("scanning _raw_data for unmapped fields", zap.String("table", targetTable))

	// 0. Check table + _raw_data exists
	if !h.tableExists(targetTable) || !h.hasColumn(targetTable, "_raw_data") {
		res, _ := json.Marshal(map[string]interface{}{"status": "skipped", "reason": "table or _raw_data column not found"})
		if msg.Reply != "" {
			msg.Respond(res)
		}
		return
	}

	// 1. Get all distinct keys from _raw_data JSONB (sample from recent rows)
	var rawKeys []string
	sql := fmt.Sprintf(
		`SELECT DISTINCT key FROM (
			SELECT jsonb_object_keys(_raw_data) AS key
			FROM "%s"
			WHERE _raw_data IS NOT NULL AND _raw_data != '{}'::jsonb
			LIMIT 1000
		) sub ORDER BY key`, targetTable)
	if err := h.db.Raw(sql).Scan(&rawKeys).Error; err != nil {
		res := map[string]interface{}{
			"status": "error",
			"error":  fmt.Sprintf("failed to scan _raw_data: %s", err.Error()),
		}
		resBytes, _ := json.Marshal(res)
		_ = msg.Respond(resBytes)
		return
	}

	// 2. Get existing mapping rules for this table
	reg, _ := h.registryRepo.GetByTargetTable(context.Background(), targetTable)
	var sourceTable string
	if reg != nil {
		sourceTable = reg.SourceTable
	} else {
		sourceTable = targetTable
	}

	existingRules, _ := h.mappingRepo.GetByTable(context.Background(), sourceTable)
	mappedFields := make(map[string]bool)
	for _, r := range existingRules {
		mappedFields[r.SourceField] = true
	}

	// 3. Also skip system/internal fields
	skipFields := map[string]bool{
		"_id": true, "_raw_data": true, "_source": true, "_synced_at": true,
		"_version": true, "_hash": true, "_deleted": true, "_created_at": true, "_updated_at": true,
		"id": true,
	}

	// 4. Find unmapped fields
	var unmappedFields []string
	for _, key := range rawKeys {
		if !mappedFields[key] && !skipFields[key] {
			unmappedFields = append(unmappedFields, key)
		}
	}

	res := map[string]interface{}{
		"status":         "ok",
		"table":          targetTable,
		"source_table":   sourceTable,
		"total_raw_keys": len(rawKeys),
		"mapped_count":   len(existingRules),
		"new_fields":     unmappedFields,
	}
	res = sanitizeAdminResultMap(res)
	resBytes, _ := json.Marshal(res)
	if msg.Reply != "" {
		_ = msg.Respond(resBytes)
	}

	h.logger.Info("_raw_data scan completed",
		zap.String("table", targetTable),
		zap.Int("raw_keys", len(rawKeys)),
		zap.Int("unmapped", len(unmappedFields)),
	)
}

// HandlePeriodicScan scans _raw_data for all active tables and auto-creates pending mapping rules.
// Subject: "cdc.cmd.periodic-scan" (pub/sub, triggered by scheduler)
func (h *CommandHandler) HandlePeriodicScan(msg *nats.Msg) {
	entries, err := h.registryRepo.GetAllActive(context.Background())
	if err != nil {
		h.logger.Error("periodic scan: failed to get active registries", zap.Error(err))
		return
	}

	totalNew := 0
	for _, entry := range entries {
		// Skip tables that don't exist or don't have _raw_data
		if !h.tableExists(entry.TargetTable) || !h.hasColumn(entry.TargetTable, "_raw_data") {
			continue
		}

		// Scan _raw_data keys
		var rawKeys []string
		sql := fmt.Sprintf(
			`SELECT DISTINCT key FROM (
				SELECT jsonb_object_keys(_raw_data) AS key FROM "%s"
				WHERE _raw_data IS NOT NULL AND _raw_data != '{}'::jsonb LIMIT 1000
			) sub`, entry.TargetTable)
		if err := h.db.Raw(sql).Scan(&rawKeys).Error; err != nil {
			continue
		}

		// Get existing rules
		existingRules, _ := h.mappingRepo.GetByTable(context.Background(), entry.SourceTable)
		mapped := make(map[string]bool)
		for _, r := range existingRules {
			mapped[r.SourceField] = true
		}

		skip := map[string]bool{
			"_id": true, "_raw_data": true, "_source": true, "_synced_at": true,
			"_version": true, "_hash": true, "_deleted": true, "_created_at": true, "_updated_at": true,
			"id": true, "_airbyte_ab_id": true, "_airbyte_emitted_at": true, "_airbyte_extracted_at": true, "_airbyte_meta": true,
		}

		for _, key := range rawKeys {
			if mapped[key] || skip[key] {
				continue
			}
			// Auto-create pending mapping rule
			rule := model.MappingRule{
				SourceTable:  entry.SourceTable,
				SourceField:  key,
				TargetColumn: key,
				DataType:     "TEXT",
				IsActive:     false,
				Status:       "pending",
				RuleType:     "discovered",
			}
			created, _ := h.mappingRepo.CreateIfNotExists(context.Background(), &rule)
			if created {
				totalNew++
			}
		}
	}

	h.logger.Info("periodic scan completed", zap.Int("new_rules_created", totalNew), zap.Int("tables_scanned", len(entries)))
}

// buildCastExpr builds a PostgreSQL expression to extract a typed value from _raw_data JSONB.
func buildCastExpr(field, dataType string) string {
	base := fmt.Sprintf("(_raw_data->>'%s')", field)
	switch strings.ToLower(dataType) {
	case "integer", "int", "int4", "int8", "bigint", "smallint":
		return fmt.Sprintf("(%s)::INTEGER", base)
	case "numeric", "decimal", "float", "float8", "double precision":
		return fmt.Sprintf("(%s)::NUMERIC", base)
	case "boolean", "bool":
		return fmt.Sprintf("(%s)::BOOLEAN", base)
	case "timestamp", "timestamp without time zone", "timestamp with time zone", "timestamptz":
		return fmt.Sprintf("(%s)::TIMESTAMP", base)
	default:
		return fmt.Sprintf("(%s)::TEXT", base)
	}
}

// publishResult publishes a sanitized command result to reply-to (if
// present), then writes the same sanitized payload into ActivityLog.
func (h *CommandHandler) publishResult(msg *nats.Msg, result CommandResult) {
	safeResult := result
	safeResult.Error = sanitizeAdminError(result.Error)
	data, _ := json.Marshal(safeResult)
	if msg.Reply != "" {
		if err := msg.Respond(data); err != nil {
			h.logger.Error("failed to reply to NATS", zap.Error(err))
		}
	}
	h.logCommandResult(safeResult)

	// Activity Log — every command result
	now := time.Now()
	var errPtr *string
	if safeResult.Error != "" {
		e := safeResult.Error
		errPtr = &e
	}
	h.db.Create(&model.ActivityLog{
		Operation:    "cmd-" + safeResult.Command,
		TargetTable:  safeResult.TargetTable,
		Status:       safeResult.Status,
		RowsAffected: int64(safeResult.RowsAffected),
		ErrorMessage: errPtr,
		Details:      data,
		TriggeredBy:  "nats-command",
		StartedAt:    now,
		CompletedAt:  &now,
	})
}

// HandleDropGINIndex drops the GIN index on _raw_data after a table is fully transformed.
// This reclaims significant storage (GIN indexes on JSONB are large).
// Subject: "cdc.cmd.drop-gin-index"
func (h *CommandHandler) HandleDropGINIndex(msg *nats.Msg) {
	targetTable := string(msg.Data)
	h.logger.Info("checking GIN index cleanup eligibility", zap.String("table", targetTable))

	if !h.tableExists(targetTable) {
		h.publishResult(msg, CommandResult{Command: "drop-gin-index", TargetTable: targetTable, Status: "skipped", Error: "table does not exist"})
		return
	}

	// 1. Check if table is fully transformed (no pending rows)
	var pendingRows int64
	reg, _ := h.registryRepo.GetByTargetTable(context.Background(), targetTable)
	if reg == nil {
		h.publishResult(msg, CommandResult{Command: "drop-gin-index", TargetTable: targetTable, Status: "error", Error: "registry entry not found"})
		return
	}

	sourceTable := reg.SourceTable
	rules, _ := h.mappingRepo.GetByTable(context.Background(), sourceTable)
	if len(rules) == 0 {
		h.publishResult(msg, CommandResult{Command: "drop-gin-index", TargetTable: targetTable, Status: "error", Error: "no mapping rules"})
		return
	}

	// Find first active mapped column as proxy
	var firstCol string
	for _, r := range rules {
		if r.IsActive {
			firstCol = r.TargetColumn
			break
		}
	}
	if firstCol == "" {
		h.publishResult(msg, CommandResult{Command: "drop-gin-index", TargetTable: targetTable, Status: "error", Error: "no active mapping rules"})
		return
	}

	h.db.Raw(fmt.Sprintf(`SELECT COUNT(*) FROM "%s" WHERE _raw_data IS NOT NULL AND "%s" IS NULL`, targetTable, firstCol)).Scan(&pendingRows)
	if pendingRows > 0 {
		h.publishResult(msg, CommandResult{
			Command:     "drop-gin-index",
			TargetTable: targetTable,
			Status:      "skipped",
			Error:       fmt.Sprintf("%d rows still pending transform", pendingRows),
		})
		return
	}

	// 2. Drop GIN index
	indexName := "idx_" + targetTable + "_raw"
	result := h.db.Exec(fmt.Sprintf(`DROP INDEX IF EXISTS "%s"`, indexName))
	if result.Error != nil {
		h.publishResult(msg, CommandResult{Command: "drop-gin-index", TargetTable: targetTable, Status: "error", Error: result.Error.Error()})
		return
	}

	h.logger.Info("GIN index dropped", zap.String("table", targetTable), zap.String("index", indexName))
	h.publishResult(msg, CommandResult{Command: "drop-gin-index", TargetTable: targetTable, Status: "success"})
}

// ============================================================================
// Boundary-refactor handlers (workspace feature-cdc-integration).
// Each handler owns an ex-CMS operation so the CMS API stays thin-proxy.
// Subject catalogue:
//   1. cdc.cmd.scan-fields             -> HandleScanFields
//   2. cdc.cmd.scan-source             -> HandleScanSource
//   3. cdc.cmd.refresh-catalog         -> HandleRefreshCatalog
//   5. cdc.cmd.sync-register           -> HandleSyncRegister
//   6. cdc.cmd.sync-state              -> HandleSyncState
//   7. cdc.cmd.restart-debezium        -> HandleRestartDebezium
//   8. cdc.cmd.alter-column            -> HandleAlterColumn
//   9. cdc.cmd.import-streams          -> HandleImportStreams
// Result subjects mirror cdc.result.<subject-tail>.
// ============================================================================

// systemFieldSet — same list used by the legacy CMS ScanFields flow.
func systemFieldSet() map[string]bool {
	return map[string]bool{
		"_raw_data": true, "_source": true, "_created_at": true,
		"_updated_at": true, "_deleted": true, "_hash": true,
		"_synced_at": true, "_version": true,
		"_airbyte_raw_id": true, "_airbyte_extracted_at": true, "_airbyte_meta": true,
		"_airbyte_ab_id": true, "_airbyte_emitted_at": true, "_airbyte_generation_id": true,
	}
}

// migrated logic stays byte-identical (Rule #4 no-improvement port).
func inferSQLTypeFromLegacyCatalogProp(prop interface{}) string {
	propMap, ok := prop.(map[string]interface{})
	if !ok {
		return "TEXT"
	}
	legacyType, _ := propMap["type"].(string)
	if types, ok := propMap["type"].([]interface{}); ok && len(types) > 0 {
		for _, t := range types {
			if tStr, ok := t.(string); ok && tStr != "null" {
				legacyType = tStr
				break
			}
		}
	}
	switch legacyType {
	case "integer":
		return "BIGINT"
	case "number":
		return "NUMERIC"
	case "boolean":
		return "BOOLEAN"
	case "array", "object":
		return "JSONB"
	default:
		if format, _ := propMap["format"].(string); format == "date-time" {
			return "TIMESTAMP"
		}
		return "TEXT"
	}
}

// scanFieldsDebezium samples _raw_data JSONB (last 100 rows) to infer
// new fields for Debezium-backed tables.
func (h *CommandHandler) scanFieldsDebezium(ctx context.Context, targetTable, sourceTable string) (int, int, error) {
	if !h.tableExists(targetTable) || !h.hasColumn(targetTable, "_raw_data") {
		return 0, 0, fmt.Errorf("table %s has no _raw_data column", targetTable)
	}
	type sampleRow struct {
		Raw json.RawMessage `gorm:"column:_raw_data"`
	}
	var rows []sampleRow
	sql := fmt.Sprintf(`SELECT _raw_data FROM "%s" WHERE _raw_data IS NOT NULL AND _raw_data != '{}'::jsonb ORDER BY _synced_at DESC LIMIT 100`, targetTable)
	if err := h.db.WithContext(ctx).Raw(sql).Scan(&rows).Error; err != nil {
		return 0, 0, fmt.Errorf("sample raw_data: %w", err)
	}
	// Merge first-seen types so we get one vote per field.
	typeByField := make(map[string]string)
	for _, r := range rows {
		var m map[string]interface{}
		if err := json.Unmarshal(r.Raw, &m); err != nil {
			continue
		}
		for k, v := range m {
			if _, ok := typeByField[k]; ok {
				continue
			}
			typeByField[k] = service.InferTypeFromRawData(v)
		}
	}
	existing, _ := h.mappingRepo.GetByTable(ctx, sourceTable)
	seen := make(map[string]bool, len(existing))
	for _, r := range existing {
		seen[r.SourceField] = true
	}
	sysFields := systemFieldSet()
	added, total := 0, len(typeByField)
	for field, dtype := range typeByField {
		if seen[field] {
			continue
		}
		status := "pending"
		ruleType := "discovered"
		if sysFields[field] {
			status = "approved"
			ruleType = "system"
		}
		rule := &model.MappingRule{
			SourceTable:  sourceTable,
			SourceField:  field,
			TargetColumn: field,
			DataType:     dtype,
			Status:       status,
			RuleType:     ruleType,
			IsActive:     true,
		}
		if err := h.mappingRepo.Create(ctx, rule); err != nil {
			h.logger.Warn("scan-fields debezium: create rule failed", zap.String("field", field), zap.Error(err))
			continue
		}
		added++
	}
	return added, total, nil
}

// HandleScanFields implements boundary-refactor #1 (subject
// Debezium _raw_data sampling based on sync_engine.
func (h *CommandHandler) HandleScanFields(msg *nats.Msg) {
	var payload struct {
		RegistryID     uint   `json:"registry_id"`
		TargetTable    string `json:"target_table"`
		SourceTable    string `json:"source_table"`
		SyncEngine     string `json:"sync_engine"`
		SourceType     string `json:"source_type"`
		LegacySourceID string `json:"legacy_source_id"`
	}
	if err := json.Unmarshal(msg.Data, &payload); err != nil {
		h.logger.Error("cdc.cmd.scan-fields: invalid payload", zap.Error(err))
		h.publishResultWithSubject(msg, "cdc.result.scan-fields", CommandResult{Command: "scan-fields", Status: "error", Error: "invalid payload"})
		return
	}
	ctx := context.Background()
	engine := strings.ToLower(strings.TrimSpace(payload.SyncEngine))
	h.logger.Info("scan-fields dispatch",
		zap.String("target", payload.TargetTable),
		zap.String("source", payload.SourceTable),
		zap.String("engine", engine),
	)

	var added, total int
	var sourceUsed string = "debezium"
	var err error
	_ = engine
	added, total, err = h.scanFieldsDebezium(ctx, payload.TargetTable, payload.SourceTable)

	if err != nil {
		h.publishResultWithSubject(msg, "cdc.result.scan-fields", CommandResult{
			Command: "scan-fields", RegistryID: payload.RegistryID, TargetTable: payload.TargetTable,
			Status: "error", Error: err.Error(),
		})
		return
	}

	// Detailed result event (Activity Log is hit by publishResult too).
	result := map[string]interface{}{
		"command":      "scan-fields",
		"registry_id":  payload.RegistryID,
		"target_table": payload.TargetTable,
		"added":        added,
		"total":        total,
		"source_used":  sourceUsed,
		"status":       "success",
	}
	result = sanitizeAdminResultMap(result)
	data, _ := json.Marshal(result)
	h.nats_publish(msg, "cdc.result.scan-fields", data)
	h.writeActivity("scan-fields", payload.TargetTable, "success", int64(added), result, "")
}

// HandleScanSource implements boundary-refactor #2 (subject
// missing registry rows (is_active=false, matching legacy CMS logic).

// HandleRefreshCatalog implements boundary-refactor #3 (subject
// drift into schema_changes_log when the catalogue changes.

// HandleSyncRegister implements boundary-refactor #5 (subject
// cdc.cmd.sync-register). Routes the "sync registry entry with sync
func (h *CommandHandler) HandleSyncRegister(msg *nats.Msg) {
	var payload struct {
		RegistryID  uint   `json:"registry_id"`
		TargetTable string `json:"target_table"`
		SourceTable string `json:"source_table"`
		SourceType  string `json:"source_type"`
		SyncEngine  string `json:"sync_engine"`
	}
	if err := json.Unmarshal(msg.Data, &payload); err != nil {
		h.publishResultWithSubject(msg, "cdc.result.sync-register", CommandResult{Command: "sync-register", Status: "error", Error: "invalid payload"})
		return
	}
	ctx := context.Background()
	_ = payload.SyncEngine
	debeziumOK := false
	var debeziumErr string
	if err := h.verifyDebeziumConnector(ctx); err != nil {
		debeziumErr = err.Error()
	} else {
		debeziumOK = true
	}

	status := "success"
	if !debeziumOK {
		status = "error"
	}
	result := map[string]interface{}{
		"command":        "sync-register",
		"registry_id":    payload.RegistryID,
		"target_table":   payload.TargetTable,
		"sync_engine":    "debezium",
		"debezium_ok":    debeziumOK,
		"debezium_error": debeziumErr,
		"status":         status,
	}
	result = sanitizeAdminResultMap(result)
	data, _ := json.Marshal(result)
	h.nats_publish(msg, "cdc.result.sync-register", data)
	h.writeActivity("sync-register", payload.TargetTable, status, 0, result, "")
}

// path. Keeps the stream config minimal (selected=true).

// verifyDebeziumConnector probes Kafka Connect for the configured
// connector. Returns nil on 2xx, an error otherwise. Circuit-breaker:
// 10s timeout + single retry (per boundary refactor Rule #3).
func (h *CommandHandler) verifyDebeziumConnector(ctx context.Context) error {
	if h.kafkaConnectURL == "" {
		return fmt.Errorf("kafka connect url not configured")
	}
	// The actual connector name is injected by worker_server via config.
	// We hit the /connectors endpoint which returns a JSON list — any
	// 2xx means the control plane is reachable.
	url := strings.TrimRight(h.kafkaConnectURL, "/") + "/connectors"
	return h.connectGET(ctx, url)
}

// HandleSyncState implements boundary-refactor #6 (subject
// pauses/resumes the Debezium connector via Kafka Connect REST.
func (h *CommandHandler) HandleSyncState(msg *nats.Msg) {
	var payload struct {
		RegistryID uint   `json:"registry_id"`
		Action     string `json:"action"`
	}
	if err := json.Unmarshal(msg.Data, &payload); err != nil {
		h.publishResultWithSubject(msg, "cdc.result.sync-state", CommandResult{Command: "sync-state", Status: "error", Error: "invalid payload"})
		return
	}
	action := strings.ToLower(payload.Action)
	if action != "activate" && action != "deactivate" {
		h.publishResultWithSubject(msg, "cdc.result.sync-state", CommandResult{Command: "sync-state", Status: "error", Error: "action must be activate|deactivate"})
		return
	}
	ctx := context.Background()
	entry, err := h.registryRepo.GetByID(ctx, payload.RegistryID)
	if err != nil {
		h.publishResultWithSubject(msg, "cdc.result.sync-state", CommandResult{Command: "sync-state", Status: "error", Error: "registry not found"})
		return
	}

	debeziumStatus := "skipped"
	var firstErr string

	// Debezium path — pause / resume the connector.
	if service.ShouldUseDebezium(entry) && h.kafkaConnectURL != "" {
		connector := h.detectConnectorName(entry)
		verb := "pause"
		if action == "activate" {
			verb = "resume"
		}
		url := fmt.Sprintf("%s/connectors/%s/%s", strings.TrimRight(h.kafkaConnectURL, "/"), connector, verb)
		if err := h.connectPUT(ctx, url); err != nil {
			debeziumStatus = "error"
			if firstErr == "" {
				firstErr = "debezium: " + err.Error()
			}
		} else {
			debeziumStatus = "ok"
		}
	}

	status := "success"
	if firstErr != "" && debeziumStatus != "ok" {
		status = "error"
	}
	result := map[string]interface{}{
		"command":         "sync-state",
		"registry_id":     payload.RegistryID,
		"target_table":    entry.TargetTable,
		"action":          action,
		"debezium_status": debeziumStatus,
		"error":           firstErr,
		"status":          status,
	}
	result = sanitizeAdminResultMap(result)
	data, _ := json.Marshal(result)
	h.nats_publish(msg, "cdc.result.sync-state", data)
	h.writeActivity("sync-state", entry.TargetTable, status, 0, result, firstErr)
}

// HandleRestartDebezium implements boundary-refactor #7 (subject
// cdc.cmd.restart-debezium). Calls Kafka Connect REST restart endpoint.
func (h *CommandHandler) HandleRestartDebezium(msg *nats.Msg) {
	var payload struct {
		ConnectorName string `json:"connector_name"`
	}
	if err := json.Unmarshal(msg.Data, &payload); err != nil {
		h.publishResultWithSubject(msg, "cdc.result.restart-debezium", CommandResult{Command: "restart-debezium", Status: "error", Error: "invalid payload"})
		return
	}
	if h.kafkaConnectURL == "" {
		h.publishResultWithSubject(msg, "cdc.result.restart-debezium", CommandResult{Command: "restart-debezium", Status: "error", Error: "kafka_connect_url not configured"})
		return
	}
	connector := strings.TrimSpace(payload.ConnectorName)
	if connector == "" {
		connector = h.detectConnectorName(nil)
	}
	url := fmt.Sprintf("%s/connectors/%s/restart?includeTasks=true&onlyFailed=false",
		strings.TrimRight(h.kafkaConnectURL, "/"), connector)
	if err := h.connectPOST(context.Background(), url); err != nil {
		h.publishResultWithSubject(msg, "cdc.result.restart-debezium", CommandResult{Command: "restart-debezium", Status: "error", Error: err.Error()})
		h.writeActivity("restart-debezium", connector, "error", 0, nil, err.Error())
		return
	}
	result := map[string]interface{}{
		"command":        "restart-debezium",
		"connector_name": connector,
		"status":         "success",
	}
	result = sanitizeAdminResultMap(result)
	data, _ := json.Marshal(result)
	h.nats_publish(msg, "cdc.result.restart-debezium", data)
	h.writeActivity("restart-debezium", connector, "success", 0, result, "")
}

// HandleAlterColumn implements boundary-refactor #8 (subject
// cdc.cmd.alter-column). Executes ALTER TABLE ... ADD|DROP|ALTER COLUMN.
// Identifiers are validated (whitelist of [A-Za-z0-9_-]) before being
// interpolated (Rule #3 security gate — Postgres does not allow bound
// identifiers).
func (h *CommandHandler) HandleAlterColumn(msg *nats.Msg) {
	var payload struct {
		TargetTable string `json:"target_table"`
		ColumnName  string `json:"column_name"`
		DataType    string `json:"data_type"`
		Action      string `json:"action"`
	}
	if err := json.Unmarshal(msg.Data, &payload); err != nil {
		h.publishResultWithSubject(msg, "cdc.result.alter-column", CommandResult{Command: "alter-column", Status: "error", Error: "invalid payload"})
		return
	}
	if !isSafeIdent(payload.TargetTable) || !isSafeIdent(payload.ColumnName) {
		h.publishResultWithSubject(msg, "cdc.result.alter-column", CommandResult{Command: "alter-column", TargetTable: payload.TargetTable, Status: "error", Error: "invalid identifier"})
		return
	}
	var sql string
	switch strings.ToLower(payload.Action) {
	case "add":
		if !isSafeType(payload.DataType) {
			h.publishResultWithSubject(msg, "cdc.result.alter-column", CommandResult{Command: "alter-column", TargetTable: payload.TargetTable, Status: "error", Error: "invalid data_type"})
			return
		}
		sql = fmt.Sprintf(`ALTER TABLE "%s" ADD COLUMN IF NOT EXISTS "%s" %s`, payload.TargetTable, payload.ColumnName, payload.DataType)
	case "drop":
		sql = fmt.Sprintf(`ALTER TABLE "%s" DROP COLUMN IF EXISTS "%s"`, payload.TargetTable, payload.ColumnName)
	case "alter_type":
		if !isSafeType(payload.DataType) {
			h.publishResultWithSubject(msg, "cdc.result.alter-column", CommandResult{Command: "alter-column", TargetTable: payload.TargetTable, Status: "error", Error: "invalid data_type"})
			return
		}
		sql = fmt.Sprintf(`ALTER TABLE "%s" ALTER COLUMN "%s" TYPE %s USING "%s"::%s`,
			payload.TargetTable, payload.ColumnName, payload.DataType, payload.ColumnName, payload.DataType)
	default:
		h.publishResultWithSubject(msg, "cdc.result.alter-column", CommandResult{Command: "alter-column", TargetTable: payload.TargetTable, Status: "error", Error: "action must be add|drop|alter_type"})
		return
	}
	if err := h.db.Exec(sql).Error; err != nil {
		h.publishResultWithSubject(msg, "cdc.result.alter-column", CommandResult{Command: "alter-column", TargetTable: payload.TargetTable, Status: "error", Error: err.Error()})
		h.writeActivity("alter-column", payload.TargetTable, "error", 0, nil, err.Error())
		return
	}
	result := map[string]interface{}{
		"command":      "alter-column",
		"target_table": payload.TargetTable,
		"column_name":  payload.ColumnName,
		"action":       payload.Action,
		"status":       "success",
	}
	result = sanitizeAdminResultMap(result)
	data, _ := json.Marshal(result)
	h.nats_publish(msg, "cdc.result.alter-column", data)
	h.writeActivity("alter-column", payload.TargetTable, "success", 0, result, "")
}

// HandleImportStreams implements boundary-refactor #9 (subject
// cdc.cmd.import-streams). Inserts registry rows, calls
// create_cdc_table() on DW, and seeds default mapping rules per stream.

// workspace and imports missing registry rows + default mapping rules.
// Heavy operation — logs once per connection.

// ---------------------------------------------------------------------------
// Helpers used across boundary-refactor handlers.
// ---------------------------------------------------------------------------

// nats_publish emits a sanitized result event to the configured subject
// when the caller used pub/sub; falls back to request-reply otherwise.
func (h *CommandHandler) nats_publish(msg *nats.Msg, subject string, data []byte) {
	if msg.Reply != "" {
		_ = msg.Respond(data)
		return
	}
	if h.natsConn != nil {
		if err := h.natsConn.Publish(subject, data); err != nil {
			h.logger.Warn("publish result failed", zap.String("subject", subject), zap.Error(err))
		}
		return
	}
	var result CommandResult
	if err := json.Unmarshal(data, &result); err == nil {
		h.logCommandResult(result, zap.String("subject", subject))
		return
	}
	h.logger.Info("command result", zap.String("subject", subject), zap.Int("bytes", len(data)))
}

// publishResultWithSubject is the error-path variant; it keeps
// ActivityLog and outbound result subjects aligned on the same
// sanitized payload shape.
func (h *CommandHandler) publishResultWithSubject(msg *nats.Msg, subject string, result CommandResult) {
	result.Error = sanitizeAdminError(result.Error)
	data, _ := json.Marshal(result)
	h.nats_publish(msg, subject, data)
	h.writeActivity("cmd-"+result.Command, result.TargetTable, result.Status, int64(result.RowsAffected), nil, result.Error)
}

// writeActivity mirrors publishResult's ActivityLog side-effect in a
// reusable form and never stores unsanitized admin-facing details.
func (h *CommandHandler) writeActivity(op, table, status string, rows int64, details map[string]interface{}, errMsg string) {
	now := time.Now()
	var errPtr *string
	if errMsg = sanitizeAdminError(errMsg); errMsg != "" {
		e := errMsg
		errPtr = &e
	}
	var detailsJSON []byte
	if details != nil {
		details = sanitizeAdminResultMap(details)
		detailsJSON, _ = json.Marshal(details)
	}
	h.db.Create(&model.ActivityLog{
		Operation:    op,
		TargetTable:  table,
		Status:       status,
		RowsAffected: rows,
		ErrorMessage: errPtr,
		Details:      detailsJSON,
		TriggeredBy:  "nats-command",
		StartedAt:    now,
		CompletedAt:  &now,
	})
}

// isSafeIdent allows [A-Za-z0-9_-] only — no quoting escape surface.
func isSafeIdent(s string) bool {
	if s == "" || len(s) > 64 {
		return false
	}
	for _, r := range s {
		if !((r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '_' || r == '-') {
			return false
		}
	}
	return true
}

// isSafeType restricts the DDL TYPE slot to a small allowlist to keep
// HandleAlterColumn safe.
func isSafeType(t string) bool {
	u := strings.ToUpper(strings.TrimSpace(t))
	switch u {
	case "TEXT", "VARCHAR", "VARCHAR(255)", "BIGINT", "INTEGER", "SMALLINT",
		"NUMERIC", "DECIMAL", "REAL", "DOUBLE PRECISION", "BOOLEAN",
		"TIMESTAMP", "TIMESTAMP WITH TIME ZONE", "TIMESTAMPTZ",
		"DATE", "TIME", "JSONB", "JSON", "UUID":
		return true
	}
	return false
}

// detectConnectorName falls back through explicit payload → registry
// notes → cmd handler default (injected via wiring later).
func (h *CommandHandler) detectConnectorName(entry *model.TableRegistry) string {
	// Allow an environment-injected default later if needed — for now we
	// use the canonical name documented in config-local.yml.
	return "goopay-mongodb-cdc"
}

// connectGET issues a short-timeout GET with a single retry.
func (h *CommandHandler) connectGET(ctx context.Context, url string) error {
	return h.connectCall(ctx, http.MethodGet, url, nil)
}

func (h *CommandHandler) connectPOST(ctx context.Context, url string) error {
	return h.connectCall(ctx, http.MethodPost, url, nil)
}

func (h *CommandHandler) connectPUT(ctx context.Context, url string) error {
	return h.connectCall(ctx, http.MethodPut, url, nil)
}

// connectCall centralises Kafka Connect REST calls — 10s timeout, 1
// retry on transport/5xx error (Rule #3 circuit-breaker lite).
func (h *CommandHandler) connectCall(ctx context.Context, method, url string, body []byte) error {
	client := &http.Client{Timeout: 10 * time.Second}
	var lastErr error
	for attempt := 0; attempt < 2; attempt++ {
		var reqBody io.Reader
		if body != nil {
			reqBody = bytes.NewReader(body)
		}
		req, err := http.NewRequestWithContext(ctx, method, url, reqBody)
		if err != nil {
			return err
		}
		req.Header.Set("Content-Type", "application/json")
		resp, err := client.Do(req)
		if err != nil {
			lastErr = err
			continue
		}
		respBody, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode < 300 {
			return nil
		}
		if resp.StatusCode >= 500 {
			lastErr = fmt.Errorf("kafka-connect %d: %s", resp.StatusCode, string(respBody))
			continue
		}
		return fmt.Errorf("kafka-connect %d: %s", resp.StatusCode, string(respBody))
	}
	return lastErr
}

func (h *CommandHandler) logCommandResult(result CommandResult, fields ...zap.Field) {
	baseFields := []zap.Field{
		zap.String("command", result.Command),
		zap.String("target_table", result.TargetTable),
		zap.String("status", result.Status),
		zap.Int("rows_affected", result.RowsAffected),
	}
	if result.RegistryID > 0 {
		baseFields = append(baseFields, zap.Uint("registry_id", result.RegistryID))
	}
	if result.Error != "" {
		baseFields = append(baseFields, zap.String("error", sanitizeAdminError(result.Error)))
	}
	baseFields = append(baseFields, fields...)
	h.logger.Info("command result", baseFields...)
}

func sanitizeAdminError(errMsg string) string {
	return service.SanitizeFreeformText(errMsg, 240)
}

func sanitizeAdminResultMap(input map[string]interface{}) map[string]interface{} {
	if len(input) == 0 {
		return map[string]interface{}{}
	}
	allowed := map[string]struct{}{
		"command": {}, "registry_id": {}, "target_table": {}, "table": {}, "rows_affected": {},
		"status": {}, "error": {}, "reason": {}, "added": {}, "total": {},
		"source_used": {}, "source_table": {}, "mapped_count": {}, "new_fields": {},
		"total_raw_keys": {}, "sync_engine": {}, "debezium_ok": {}, "debezium_error": {},
		"debezium_status": {}, "action": {}, "connector_name": {}, "column_name": {},
	}
	out := make(map[string]interface{}, len(input))
	for key, value := range input {
		if _, ok := allowed[key]; !ok {
			continue
		}
		switch key {
		case "error", "reason", "debezium_error":
			out[key] = sanitizeAdminError(fmt.Sprintf("%v", value))
		case "new_fields":
			out[key] = sanitizeAdminFields(value)
		default:
			out[key] = value
		}
	}
	return out
}

func sanitizeAdminFields(value interface{}) []string {
	list, ok := value.([]string)
	if ok {
		out := append([]string(nil), list...)
		sort.Strings(out)
		return out
	}
	items, ok := value.([]interface{})
	if !ok {
		return nil
	}
	out := make([]string, 0, len(items))
	for _, item := range items {
		if s, ok := item.(string); ok && s != "" {
			out = append(out, s)
		}
	}
	sort.Strings(out)
	return out
}
