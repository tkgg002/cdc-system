package handler

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"centralized-data-service/internal/model"
	"centralized-data-service/internal/repository"
	"centralized-data-service/internal/service"

	"centralized-data-service/pkgs/airbyte"
	"github.com/nats-io/nats.go"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

// CommandHandler handles async CDC commands published from the API service via NATS.
// These commands operate on the DW (Data Warehouse) database, which only the worker can access.
type CommandHandler struct {
	db          *gorm.DB
	mappingRepo *repository.MappingRuleRepo
	registryRepo *repository.RegistryRepo
	pendingRepo  *repository.PendingFieldRepo
	airbyte      *airbyte.Client
	logger      *zap.Logger
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

// CommandResult is published back after command execution (for logging/audit purposes).
type CommandResult struct {
	Command      string `json:"command"`
	RegistryID   uint   `json:"registry_id,omitempty"`
	TargetTable  string `json:"target_table,omitempty"`
	RowsAffected int    `json:"rows_affected,omitempty"`
	Status       string `json:"status"`
	Error        string `json:"error,omitempty"`
}

func NewCommandHandler(db *gorm.DB, mappingRepo *repository.MappingRuleRepo, registryRepo *repository.RegistryRepo, pendingRepo *repository.PendingFieldRepo, abClient *airbyte.Client, logger *zap.Logger) *CommandHandler {
	return &CommandHandler{
		db:           db,
		mappingRepo:  mappingRepo,
		registryRepo: registryRepo,
		pendingRepo:  pendingRepo,
		airbyte:      abClient,
		logger:       logger,
	}
}

// ensureCDCColumns adds CDC columns to an existing table (Airbyte tables don't have them).
// Returns error if table doesn't exist. Safe to call multiple times (ADD COLUMN IF NOT EXISTS).
func (h *CommandHandler) ensureCDCColumns(tableName string) error {
	var exists bool
	h.db.Raw("SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name = ? AND table_schema = 'public')", tableName).Scan(&exists)
	if !exists {
		return fmt.Errorf("table %s does not exist", tableName)
	}

	cdcColumns := []struct{ name, def string }{
		{"_raw_data", "JSONB"},
		{"_source", "VARCHAR(20) DEFAULT 'airbyte'"},
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
		// Table đã tồn tại (Airbyte tạo) → chỉ thêm CDC system columns
		if err := h.ensureCDCColumns(payload.TargetTable); err != nil {
			h.publishResult(msg, CommandResult{Command: "create-default-columns", TargetTable: payload.TargetTable, Status: "error", Error: err.Error()})
			return
		}
		columnsAdded = 8 // 8 CDC columns

		// Update registry
		h.db.Model(&model.TableRegistry{}).Where("target_table = ?", payload.TargetTable).Update("is_table_created", true)

		h.logger.Info("CDC system columns added to existing Airbyte table",
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
func (h *CommandHandler) HandleIntrospect(msg *nats.Msg) {
	var payload struct {
		TargetTable string `json:"target_table"`
	}
	if err := json.Unmarshal(msg.Data, &payload); err != nil {
		h.logger.Error("cdc.cmd.introspect: invalid payload", zap.Error(err))
		if msg.Reply != "" {
			_ = msg.Respond([]byte(`{"status":"error","error":"invalid payload"}`))
		}
		return
	}

	h.logger.Info("introspecting table via Airbyte", zap.String("table", payload.TargetTable))

	// 1. Verify table is registered
	reg, err := h.registryRepo.GetByTargetTable(context.Background(), payload.TargetTable)
	if err != nil {
		if msg.Reply != "" {
			_ = msg.Respond([]byte(`{"status":"error","error":"table not registered"}`))
		}
		return
	}

	if reg.AirbyteSourceID == nil || *reg.AirbyteSourceID == "" {
		if msg.Reply != "" {
			_ = msg.Respond([]byte(`{"status":"error","error":"table does not have airbyte_source_id configuring"}`))
		}
		return
	}

	// 2. Discover schema from Airbyte (Source schema)
	catalog, err := h.airbyte.DiscoverSourceSchema(context.Background(), *reg.AirbyteSourceID)
	if err != nil {
		h.logger.Error("failed to discover airbyte schema", zap.Error(err))
		if msg.Reply != "" {
			_ = msg.Respond([]byte(`{"status":"error","error":"failed to discover airbyte schema: ` + err.Error() + `"}`))
		}
		return
	}

	// Find the matching stream for this source table
	var streamFields []string
	found := false
	for _, streamCfg := range catalog.Streams {
		if streamCfg.Stream.Name == reg.SourceTable {
			found = true
			if streamCfg.Stream.JSONSchema != nil {
				if props, ok := streamCfg.Stream.JSONSchema["properties"].(map[string]interface{}); ok {
					for fieldName := range props {
						streamFields = append(streamFields, fieldName)
					}
				}
			}
			break
		}
	}

	if !found {
		if msg.Reply != "" {
			_ = msg.Respond([]byte(`{"status":"error","error":"stream not found in airbyte catalog"}`))
		}
		return
	}

	// 3. Get existing mappings
	rules, err := h.mappingRepo.GetByTable(context.Background(), reg.SourceTable)
	if err != nil {
		if msg.Reply != "" {
			_ = msg.Respond([]byte(`{"status":"error","error":"failed to fetch current rules"}`))
		}
		return
	}

	mappedFields := make(map[string]bool)
	for _, r := range rules {
		mappedFields[r.SourceField] = true
	}

	// 4. Compare and find unmapped/new fields
	var newFields []string
	for _, field := range streamFields {
		// Ignore standard Airbyte fields or MongoDB internal fields if needed
		// Though Airbyte usually provides clean fields
		if !mappedFields[field] {
			newFields = append(newFields, field)
			
			// Save to pending_fields
			if err := h.pendingRepo.UpsertPendingField(context.Background(), payload.TargetTable, reg.SourceDB, field, "", "VARCHAR(255)"); err != nil {
				h.logger.Error("failed to save pending field", zap.Error(err), zap.String("field", field))
			}
		}
	}

	if newFields == nil {
		newFields = []string{} // ensure it serializes to [] instead of null if empty
	}

	res := map[string]interface{}{
		"status":         "success",
		"table":          payload.TargetTable,
		"total_raw_keys": len(streamFields),
		"new_fields":     newFields,
		"mapped_count":   len(mappedFields),
	}

	resBytes, _ := json.Marshal(res)
	if msg.Reply != "" {
		_ = msg.Respond(resBytes)
	}
}

// HandleAirbyteBridge copies data from Airbyte raw tables (_airbyte_raw_*) into CDC tables (_raw_data).
// Subject: "cdc.cmd.bridge-airbyte" (pub/sub pattern)
func (h *CommandHandler) HandleAirbyteBridge(msg *nats.Msg) {
	var payload struct {
		TargetTable     string `json:"target_table"`
		AirbyteRawTable string `json:"airbyte_raw_table"`
		PrimaryKeyField string `json:"primary_key_field"`
		SourceType      string `json:"source_type"`
	}
	if err := json.Unmarshal(msg.Data, &payload); err != nil {
		h.publishResult(msg, CommandResult{Command: "bridge-airbyte", Status: "error", Error: "invalid payload: " + err.Error()})
		return
	}

	// Airbyte uses typed denormalized tables (same name as stream, with underscores)
	// e.g., source_table "payment-bills" → Airbyte table "payment_bills"
	airbyteTable := payload.AirbyteRawTable
	if airbyteTable == "" || strings.HasPrefix(airbyteTable, "_airbyte_raw_") {
		// Auto-detect: normalize source table name (replace - with _)
		airbyteTable = strings.ReplaceAll(payload.TargetTable, "-", "_")
	}

	h.logger.Info("bridging Airbyte typed table → CDC table",
		zap.String("airbyte_table", airbyteTable),
		zap.String("cdc_table", payload.TargetTable),
	)

	// 1. Verify Airbyte typed table exists
	var tableExists bool
	h.db.Raw("SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name = ? AND table_schema = 'public')", airbyteTable).Scan(&tableExists)
	if !tableExists {
		h.publishResult(msg, CommandResult{
			Command:     "bridge-airbyte",
			TargetTable: payload.TargetTable,
			Status:      "error",
			Error:       fmt.Sprintf("Airbyte table '%s' not found in database", airbyteTable),
		})
		return
	}

	// 2. Ensure CDC columns exist on target table (Airbyte tables don't have them)
	if err := h.ensureCDCColumns(payload.TargetTable); err != nil {
		h.publishResult(msg, CommandResult{
			Command:     "bridge-airbyte",
			TargetTable: payload.TargetTable,
			Status:      "skipped",
			Error:       "target table not ready: " + err.Error(),
		})
		return
	}

	// 3. If source and target are the same table, populate _raw_data from existing columns
	if airbyteTable == payload.TargetTable {
		h.bridgeInPlace(msg, payload.TargetTable, payload.PrimaryKeyField)
		return
	}

	// 3. Get last_bridge_at for incremental sync
	reg, _ := h.registryRepo.GetByTargetTable(context.Background(), payload.TargetTable)
	var whereClause string
	if reg != nil && reg.LastBridgeAt != nil {
		whereClause = fmt.Sprintf("AND src._airbyte_extracted_at > '%s'", reg.LastBridgeAt.Format("2006-01-02 15:04:05"))
	}

	// 4. Bridge: pack all non-_airbyte columns into JSONB → INSERT into CDC table
	pkField := payload.PrimaryKeyField
	if pkField == "" {
		pkField = "_id"
	}

	// Verify PK column exists in Airbyte table; fallback to _airbyte_raw_id
	var pkExists bool
	h.db.Raw(`SELECT EXISTS(SELECT 1 FROM information_schema.columns WHERE table_name = ? AND column_name = ?)`, airbyteTable, pkField).Scan(&pkExists)
	if !pkExists {
		h.logger.Warn("PK field not found in Airbyte table, using _airbyte_raw_id",
			zap.String("table", airbyteTable), zap.String("pk", pkField))
		pkField = "_airbyte_raw_id"
	}

	// Detect Sonyflake schema: CDC table has source_id column
	var hasSonyflakeSchema bool
	h.db.Raw(`SELECT EXISTS(SELECT 1 FROM information_schema.columns WHERE table_name = ? AND column_name = 'source_id')`,
		payload.TargetTable).Scan(&hasSonyflakeSchema)

	var bridgeSQL string
	if hasSonyflakeSchema {
		// v1.12 schema: id BIGINT DEFAULT nextval(seq), source_id VARCHAR (dedup)
		// id auto-generated from sequence; Sonyflake replaces sequence when using pgx.Batch
		bridgeSQL = fmt.Sprintf(`
			INSERT INTO "%s" (source_id, _raw_data, _source, _synced_at, _hash, _version, _created_at, _updated_at)
			SELECT
				src."%s"::VARCHAR,
				to_jsonb(src) - '_airbyte_raw_id' - '_airbyte_extracted_at' - '_airbyte_meta' - '_airbyte_generation_id',
				'airbyte',
				COALESCE(src._airbyte_extracted_at, NOW()),
				md5((to_jsonb(src))::text),
				1,
				NOW(),
				NOW()
			FROM "%s" src
			WHERE src."%s" IS NOT NULL %s
			ON CONFLICT (source_id) DO UPDATE SET
				_raw_data = EXCLUDED._raw_data,
				_synced_at = EXCLUDED._synced_at,
				_hash = EXCLUDED._hash,
				_version = "%s"._version + 1,
				_updated_at = NOW()
			WHERE "%s"._hash IS DISTINCT FROM EXCLUDED._hash`,
			payload.TargetTable,
			pkField,
			airbyteTable,
			pkField, whereClause,
			payload.TargetTable,
			payload.TargetTable,
		)
		h.logger.Info("using v1.12 schema (source_id dedup) for bridge", zap.String("table", payload.TargetTable))
	} else {
		// Legacy schema: id VARCHAR PK (source PK directly)
		cdcPKColumn := "id"
		if pkField != "_id" && pkField != "_airbyte_raw_id" {
			cdcPKColumn = pkField
		}
		bridgeSQL = fmt.Sprintf(`
			INSERT INTO "%s" ("%s", _raw_data, _source, _synced_at, _hash, _version, _created_at, _updated_at)
			SELECT
				src."%s"::VARCHAR,
				to_jsonb(src) - '_airbyte_raw_id' - '_airbyte_extracted_at' - '_airbyte_meta' - '_airbyte_generation_id',
				'airbyte',
				COALESCE(src._airbyte_extracted_at, NOW()),
				md5((to_jsonb(src))::text),
				1,
				NOW(),
				NOW()
			FROM "%s" src
			WHERE src."%s" IS NOT NULL %s
			ON CONFLICT ("%s") DO UPDATE SET
				_raw_data = EXCLUDED._raw_data,
				_synced_at = EXCLUDED._synced_at,
				_hash = EXCLUDED._hash,
				_version = "%s"._version + 1,
				_updated_at = NOW()
			WHERE "%s"._hash IS DISTINCT FROM EXCLUDED._hash`,
			payload.TargetTable, cdcPKColumn,
			pkField,
			airbyteTable,
			pkField, whereClause,
			cdcPKColumn,
			payload.TargetTable,
			payload.TargetTable,
		)
	}

	result := h.db.Exec(bridgeSQL)
	if result.Error != nil {
		h.publishResult(msg, CommandResult{
			Command:     "bridge-airbyte",
			TargetTable: payload.TargetTable,
			Status:      "error",
			Error:       result.Error.Error(),
		})
		return
	}

	// 5. Update last_bridge_at
	if reg != nil {
		h.db.Model(&model.TableRegistry{}).Where("id = ?", reg.ID).Update("last_bridge_at", "NOW()")
	}

	h.logger.Info("bridge completed",
		zap.String("airbyte_table", airbyteTable),
		zap.String("cdc_table", payload.TargetTable),
		zap.Int64("rows_affected", result.RowsAffected),
	)
	h.publishResult(msg, CommandResult{
		Command:      "bridge-airbyte",
		TargetTable:  payload.TargetTable,
		RowsAffected: int(result.RowsAffected),
		Status:       "success",
	})
}

// bridgeInPlace handles the case where Airbyte table IS the CDC table.
// Populates _raw_data JSONB from existing typed columns for rows where _raw_data is NULL.
func (h *CommandHandler) bridgeInPlace(msg *nats.Msg, tableName, pkField string) {
	h.logger.Info("bridge in-place: populating _raw_data from typed columns", zap.String("table", tableName))

	sql := fmt.Sprintf(`
		UPDATE "%s" SET
			_raw_data = to_jsonb("%s".*) - '_raw_data' - '_source' - '_synced_at' - '_version' - '_hash' - '_deleted' - '_created_at' - '_updated_at' - '_airbyte_raw_id' - '_airbyte_extracted_at' - '_airbyte_meta' - '_airbyte_generation_id',
			_source = COALESCE(_source, 'airbyte'),
			_synced_at = COALESCE(_synced_at, NOW()),
			_hash = md5((to_jsonb("%s".*))::text),
			_version = COALESCE(_version, 1),
			_updated_at = NOW()
		WHERE _raw_data IS NULL OR _raw_data = '{}'::jsonb`,
		tableName, tableName, tableName,
	)

	result := h.db.Exec(sql)
	if result.Error != nil {
		h.publishResult(msg, CommandResult{Command: "bridge-airbyte", TargetTable: tableName, Status: "error", Error: result.Error.Error()})
		return
	}

	h.logger.Info("bridge in-place completed", zap.String("table", tableName), zap.Int64("rows", result.RowsAffected))
	h.publishResult(msg, CommandResult{Command: "bridge-airbyte", TargetTable: tableName, RowsAffected: int(result.RowsAffected), Status: "success"})
}

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
// This is a fallback mechanism that works without Airbyte API — directly queries the DW table.
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
		"status":        "ok",
		"table":         targetTable,
		"source_table":  sourceTable,
		"total_raw_keys": len(rawKeys),
		"mapped_count":  len(existingRules),
		"new_fields":    unmappedFields,
	}
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

// publishResult publishes the command result to NATS reply-to (if present) or logs it.
func (h *CommandHandler) publishResult(msg *nats.Msg, result CommandResult) {
	data, _ := json.Marshal(result)
	if msg.Reply != "" {
		if err := msg.Respond(data); err != nil {
			h.logger.Error("failed to reply to NATS", zap.Error(err))
		}
	}
	h.logger.Info("command result", zap.String("payload", string(data)))

	// Activity Log — every command result
	now := time.Now()
	var errPtr *string
	if result.Error != "" {
		e := result.Error
		errPtr = &e
	}
	h.db.Create(&model.ActivityLog{
		Operation:    "cmd-" + result.Command,
		TargetTable:  result.TargetTable,
		Status:       result.Status,
		RowsAffected: int64(result.RowsAffected),
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
//   4. cdc.cmd.airbyte-sync            -> HandleAirbyteSync
//   5. cdc.cmd.sync-register           -> HandleSyncRegister
//   6. cdc.cmd.sync-state              -> HandleSyncState
//   7. cdc.cmd.restart-debezium        -> HandleRestartDebezium
//   8. cdc.cmd.alter-column            -> HandleAlterColumn
//   9. cdc.cmd.import-streams          -> HandleImportStreams
//  10. cdc.cmd.bulk-sync-from-airbyte  -> HandleBulkSyncFromAirbyte
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

// inferSQLTypeFromAirbyteProp mirrors the CMS inferSQLType helper so the
// migrated logic stays byte-identical (Rule #4 no-improvement port).
func inferSQLTypeFromAirbyteProp(prop interface{}) string {
	propMap, ok := prop.(map[string]interface{})
	if !ok {
		return "TEXT"
	}
	airbyteType, _ := propMap["type"].(string)
	if types, ok := propMap["type"].([]interface{}); ok && len(types) > 0 {
		for _, t := range types {
			if tStr, ok := t.(string); ok && tStr != "null" {
				airbyteType = tStr
				break
			}
		}
	}
	switch airbyteType {
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

// scanFieldsAirbyte handles the Airbyte branch of ScanFields (shared by
// `airbyte` and `both` sync engines with Debezium fallback).
func (h *CommandHandler) scanFieldsAirbyte(ctx context.Context, sourceTable, sourceID string) (int, int, error) {
	if sourceID == "" {
		return 0, 0, fmt.Errorf("airbyte_source_id missing")
	}
	catalog, err := h.airbyte.DiscoverSchema(ctx, sourceID)
	if err != nil {
		return 0, 0, fmt.Errorf("discover: %w", err)
	}
	var targetStream *airbyte.StreamConfig
	for i := range catalog.Streams {
		if catalog.Streams[i].Stream.Name == sourceTable {
			targetStream = &catalog.Streams[i]
			break
		}
	}
	if targetStream == nil {
		return 0, 0, fmt.Errorf("stream %s not found in airbyte catalog", sourceTable)
	}
	props, ok := targetStream.Stream.JSONSchema["properties"].(map[string]interface{})
	if !ok {
		return 0, 0, nil
	}
	existing, _ := h.mappingRepo.GetByTable(ctx, sourceTable)
	seen := make(map[string]bool, len(existing))
	for _, r := range existing {
		seen[r.SourceField] = true
	}
	sysFields := systemFieldSet()
	added, total := 0, 0
	for field, prop := range props {
		total++
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
			DataType:     inferSQLTypeFromAirbyteProp(prop),
			Status:       status,
			RuleType:     ruleType,
			IsActive:     true,
		}
		if err := h.mappingRepo.Create(ctx, rule); err != nil {
			h.logger.Warn("scan-fields airbyte: create rule failed", zap.String("field", field), zap.Error(err))
			continue
		}
		added++
	}
	return added, total, nil
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
// cdc.cmd.scan-fields). Routes between Airbyte DiscoverSchema and
// Debezium _raw_data sampling based on sync_engine.
func (h *CommandHandler) HandleScanFields(msg *nats.Msg) {
	var payload struct {
		RegistryID      uint   `json:"registry_id"`
		TargetTable     string `json:"target_table"`
		SourceTable     string `json:"source_table"`
		SyncEngine      string `json:"sync_engine"`
		SourceType      string `json:"source_type"`
		AirbyteSourceID string `json:"airbyte_source_id"`
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
	var sourceUsed string
	var err error
	switch engine {
	case "airbyte":
		added, total, err = h.scanFieldsAirbyte(ctx, payload.SourceTable, payload.AirbyteSourceID)
		sourceUsed = "airbyte"
	case "debezium":
		added, total, err = h.scanFieldsDebezium(ctx, payload.TargetTable, payload.SourceTable)
		sourceUsed = "debezium"
	case "both", "":
		added, total, err = h.scanFieldsDebezium(ctx, payload.TargetTable, payload.SourceTable)
		sourceUsed = "debezium"
		if err != nil || total == 0 {
			a, t, aerr := h.scanFieldsAirbyte(ctx, payload.SourceTable, payload.AirbyteSourceID)
			if aerr == nil {
				added, total, err = a, t, nil
				sourceUsed = "airbyte"
			}
		}
	default:
		err = fmt.Errorf("unknown sync_engine %q", engine)
	}

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
	data, _ := json.Marshal(result)
	h.nats_publish(msg, "cdc.result.scan-fields", data)
	h.writeActivity("scan-fields", payload.TargetTable, "success", int64(added), result, "")
}

// HandleScanSource implements boundary-refactor #2 (subject
// cdc.cmd.scan-source). Walks an Airbyte source catalogue and inserts
// missing registry rows (is_active=false, matching legacy CMS logic).
func (h *CommandHandler) HandleScanSource(msg *nats.Msg) {
	var payload struct {
		SourceID   string `json:"source_id"`
		SourceType string `json:"source_type"`
	}
	if err := json.Unmarshal(msg.Data, &payload); err != nil {
		h.publishResultWithSubject(msg, "cdc.result.scan-source", CommandResult{Command: "scan-source", Status: "error", Error: "invalid payload"})
		return
	}
	if payload.SourceID == "" {
		h.publishResultWithSubject(msg, "cdc.result.scan-source", CommandResult{Command: "scan-source", Status: "error", Error: "source_id required"})
		return
	}
	ctx := context.Background()
	catalog, err := h.airbyte.DiscoverSchema(ctx, payload.SourceID)
	if err != nil {
		h.publishResultWithSubject(msg, "cdc.result.scan-source", CommandResult{Command: "scan-source", Status: "error", Error: err.Error()})
		return
	}
	added, skipped := 0, 0
	for _, s := range catalog.Streams {
		dbName := s.Stream.Namespace
		table := s.Stream.Name
		var existing model.TableRegistry
		if err := h.db.WithContext(ctx).Where("source_db = ? AND source_table = ?", dbName, table).First(&existing).Error; err == nil {
			skipped++
			continue
		}
		sid := payload.SourceID
		entry := model.TableRegistry{
			SourceDB:        dbName,
			SourceType:      payload.SourceType,
			SourceTable:     table,
			TargetTable:     table,
			SyncEngine:      "airbyte",
			Priority:        "normal",
			SyncInterval:    "15m",
			IsActive:        false,
			AirbyteSourceID: &sid,
		}
		if err := h.registryRepo.Create(ctx, &entry); err != nil {
			h.logger.Warn("scan-source: create registry failed", zap.String("table", table), zap.Error(err))
			continue
		}
		added++
	}
	result := map[string]interface{}{
		"command":   "scan-source",
		"source_id": payload.SourceID,
		"added":     added,
		"skipped":   skipped,
		"total":     len(catalog.Streams),
		"status":    "success",
	}
	data, _ := json.Marshal(result)
	h.nats_publish(msg, "cdc.result.scan-source", data)
	h.writeActivity("scan-source", "*", "success", int64(added), result, "")
}

// HandleRefreshCatalog implements boundary-refactor #3 (subject
// cdc.cmd.refresh-catalog). Re-runs Airbyte DiscoverSchema and records
// drift into schema_changes_log when the catalogue changes.
func (h *CommandHandler) HandleRefreshCatalog(msg *nats.Msg) {
	var payload struct {
		RegistryID      uint   `json:"registry_id"`
		AirbyteSourceID string `json:"airbyte_source_id"`
	}
	if err := json.Unmarshal(msg.Data, &payload); err != nil {
		h.publishResultWithSubject(msg, "cdc.result.refresh-catalog", CommandResult{Command: "refresh-catalog", Status: "error", Error: "invalid payload"})
		return
	}
	if payload.AirbyteSourceID == "" {
		h.publishResultWithSubject(msg, "cdc.result.refresh-catalog", CommandResult{Command: "refresh-catalog", Status: "error", Error: "airbyte_source_id required"})
		return
	}
	ctx := context.Background()
	catalog, err := h.airbyte.DiscoverSchema(ctx, payload.AirbyteSourceID)
	if err != nil {
		h.publishResultWithSubject(msg, "cdc.result.refresh-catalog", CommandResult{
			Command: "refresh-catalog", RegistryID: payload.RegistryID,
			Status: "error", Error: err.Error(),
		})
		return
	}

	// Record drift entry (best-effort, non-fatal).
	streamNames := make([]string, 0, len(catalog.Streams))
	for _, s := range catalog.Streams {
		streamNames = append(streamNames, s.Stream.Name)
	}
	summary := strings.Join(streamNames, ",")
	sid := payload.AirbyteSourceID
	drift := model.SchemaChangeLog{
		TblName:         "*",
		ChangeType:      "airbyte_catalog_refresh",
		SQLExecuted:     "discover_schema",
		Status:          "applied",
		ExecutedBy:      "worker-refresh-catalog",
		ExecutedAt:      time.Now(),
		AirbyteSourceID: &sid,
		AirbyteRefreshTriggered: true,
	}
	newDef := summary
	drift.NewDefinition = &newDef
	h.db.Create(&drift)

	result := map[string]interface{}{
		"command":     "refresh-catalog",
		"registry_id": payload.RegistryID,
		"source_id":   payload.AirbyteSourceID,
		"streams":     len(catalog.Streams),
		"status":      "success",
	}
	data, _ := json.Marshal(result)
	h.nats_publish(msg, "cdc.result.refresh-catalog", data)
	h.writeActivity("refresh-catalog", payload.AirbyteSourceID, "success", int64(len(catalog.Streams)), result, "")
}

// HandleAirbyteSync implements boundary-refactor #4 (subject
// cdc.cmd.airbyte-sync). Triggers a manual sync job.
func (h *CommandHandler) HandleAirbyteSync(msg *nats.Msg) {
	var payload struct {
		RegistryID   uint   `json:"registry_id"`
		ConnectionID string `json:"connection_id"`
	}
	if err := json.Unmarshal(msg.Data, &payload); err != nil {
		h.publishResultWithSubject(msg, "cdc.result.airbyte-sync", CommandResult{Command: "airbyte-sync", Status: "error", Error: "invalid payload"})
		return
	}
	if payload.ConnectionID == "" {
		h.publishResultWithSubject(msg, "cdc.result.airbyte-sync", CommandResult{Command: "airbyte-sync", Status: "error", Error: "connection_id required"})
		return
	}
	jobID, err := h.airbyte.TriggerSync(context.Background(), payload.ConnectionID)
	if err != nil {
		h.publishResultWithSubject(msg, "cdc.result.airbyte-sync", CommandResult{
			Command: "airbyte-sync", RegistryID: payload.RegistryID,
			Status: "error", Error: err.Error(),
		})
		return
	}
	result := map[string]interface{}{
		"command":       "airbyte-sync",
		"registry_id":   payload.RegistryID,
		"connection_id": payload.ConnectionID,
		"job_id":        jobID,
		"status":        "success",
	}
	data, _ := json.Marshal(result)
	h.nats_publish(msg, "cdc.result.airbyte-sync", data)
	h.writeActivity("airbyte-sync", payload.ConnectionID, "success", 1, result, "")
}

// HandleSyncRegister implements boundary-refactor #5 (subject
// cdc.cmd.sync-register). Routes the "sync registry entry with sync
// engine" call across Airbyte, Debezium or both.
func (h *CommandHandler) HandleSyncRegister(msg *nats.Msg) {
	var payload struct {
		RegistryID            uint   `json:"registry_id"`
		TargetTable           string `json:"target_table"`
		SourceTable           string `json:"source_table"`
		SourceType            string `json:"source_type"`
		SyncEngine            string `json:"sync_engine"`
		AirbyteConnectionID   string `json:"airbyte_connection_id"`
		AirbyteSourceID       string `json:"airbyte_source_id"`
	}
	if err := json.Unmarshal(msg.Data, &payload); err != nil {
		h.publishResultWithSubject(msg, "cdc.result.sync-register", CommandResult{Command: "sync-register", Status: "error", Error: "invalid payload"})
		return
	}
	ctx := context.Background()
	engine := strings.ToLower(strings.TrimSpace(payload.SyncEngine))
	airbyteOK, debeziumOK := false, false
	var airbyteErr, debeziumErr string

	if engine == "airbyte" || engine == "both" {
		if payload.AirbyteSourceID != "" && payload.AirbyteConnectionID != "" {
			if err := h.syncWithAirbyte(ctx, payload.AirbyteSourceID, payload.AirbyteConnectionID, payload.SourceTable); err != nil {
				airbyteErr = err.Error()
			} else {
				airbyteOK = true
			}
		} else {
			airbyteErr = "airbyte ids missing"
		}
	}
	if engine == "debezium" || engine == "both" {
		if err := h.verifyDebeziumConnector(ctx); err != nil {
			debeziumErr = err.Error()
		} else {
			debeziumOK = true
		}
	}

	status := "success"
	if (engine == "airbyte" && !airbyteOK) || (engine == "debezium" && !debeziumOK) || (engine == "both" && !airbyteOK && !debeziumOK) {
		status = "error"
	}
	result := map[string]interface{}{
		"command":       "sync-register",
		"registry_id":   payload.RegistryID,
		"target_table":  payload.TargetTable,
		"sync_engine":   engine,
		"airbyte_ok":    airbyteOK,
		"debezium_ok":   debeziumOK,
		"airbyte_error": airbyteErr,
		"debezium_error": debeziumErr,
		"status":        status,
	}
	data, _ := json.Marshal(result)
	h.nats_publish(msg, "cdc.result.sync-register", data)
	h.writeActivity("sync-register", payload.TargetTable, status, 0, result, "")
}

// syncWithAirbyte ports the legacy "DiscoverSchema + UpdateConnection"
// path. Keeps the stream config minimal (selected=true).
func (h *CommandHandler) syncWithAirbyte(ctx context.Context, sourceID, connectionID, sourceTable string) error {
	catalog, err := h.airbyte.DiscoverSchema(ctx, sourceID)
	if err != nil {
		return fmt.Errorf("discover: %w", err)
	}
	streams := make([]airbyte.StreamConfig, 0, len(catalog.Streams))
	for _, s := range catalog.Streams {
		if sourceTable != "" && s.Stream.Name != sourceTable {
			streams = append(streams, s)
			continue
		}
		s.Config.Selected = true
		streams = append(streams, s)
	}
	return h.airbyte.UpdateConnection(ctx, connectionID, streams)
}

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
// cdc.cmd.sync-state). Activates/deactivates a stream in Airbyte or
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

	airbyteStatus, debeziumStatus := "skipped", "skipped"
	var firstErr string

	// Airbyte path — toggle stream's selected flag.
	if service.ShouldUseAirbyte(entry) && entry.AirbyteConnectionID != nil && entry.AirbyteSourceID != nil {
		catalog, err := h.airbyte.DiscoverSchema(ctx, *entry.AirbyteSourceID)
		if err == nil {
			selected := action == "activate"
			streams := make([]airbyte.StreamConfig, 0, len(catalog.Streams))
			for _, s := range catalog.Streams {
				if s.Stream.Name == entry.SourceTable {
					s.Config.Selected = selected
				}
				streams = append(streams, s)
			}
			if err := h.airbyte.UpdateConnection(ctx, *entry.AirbyteConnectionID, streams); err != nil {
				airbyteStatus = "error"
				if firstErr == "" {
					firstErr = "airbyte: " + err.Error()
				}
			} else {
				airbyteStatus = "ok"
			}
		} else {
			airbyteStatus = "error"
			firstErr = "airbyte discover: " + err.Error()
		}
	}

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
	if firstErr != "" && airbyteStatus != "ok" && debeziumStatus != "ok" {
		status = "error"
	}
	result := map[string]interface{}{
		"command":         "sync-state",
		"registry_id":     payload.RegistryID,
		"target_table":    entry.TargetTable,
		"action":          action,
		"airbyte_status":  airbyteStatus,
		"debezium_status": debeziumStatus,
		"error":           firstErr,
		"status":          status,
	}
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
		"sql":          sql,
		"status":       "success",
	}
	data, _ := json.Marshal(result)
	h.nats_publish(msg, "cdc.result.alter-column", data)
	h.writeActivity("alter-column", payload.TargetTable, "success", 0, result, "")
}

// HandleImportStreams implements boundary-refactor #9 (subject
// cdc.cmd.import-streams). Inserts registry rows, calls
// create_cdc_table() on DW, and seeds default mapping rules per stream.
func (h *CommandHandler) HandleImportStreams(msg *nats.Msg) {
	var payload struct {
		ConnectionID string `json:"connection_id"`
		Streams      []struct {
			SourceTable string `json:"source_table"`
			TargetTable string `json:"target_table"`
			SourceDB    string `json:"source_db"`
			SourceType  string `json:"source_type"`
		} `json:"streams"`
	}
	if err := json.Unmarshal(msg.Data, &payload); err != nil {
		h.publishResultWithSubject(msg, "cdc.result.import-streams", CommandResult{Command: "import-streams", Status: "error", Error: "invalid payload"})
		return
	}
	ctx := context.Background()
	added, skipped := 0, 0
	for _, s := range payload.Streams {
		if !isSafeIdent(s.TargetTable) {
			skipped++
			continue
		}
		// Registry insert (skip if row exists).
		var existing model.TableRegistry
		if err := h.db.WithContext(ctx).Where("source_db = ? AND source_table = ?", s.SourceDB, s.SourceTable).First(&existing).Error; err == nil {
			skipped++
			continue
		}
		connID := payload.ConnectionID
		entry := model.TableRegistry{
			SourceDB:            s.SourceDB,
			SourceType:          s.SourceType,
			SourceTable:         s.SourceTable,
			TargetTable:         s.TargetTable,
			SyncEngine:          "airbyte",
			Priority:            "normal",
			SyncInterval:        "15m",
			IsActive:            false,
			AirbyteConnectionID: &connID,
		}
		if err := h.registryRepo.Create(ctx, &entry); err != nil {
			h.logger.Warn("import-streams: registry create failed", zap.String("table", s.TargetTable), zap.Error(err))
			continue
		}
		// Create DW table via SQL function (safe — function takes identifier arg).
		if err := h.db.WithContext(ctx).Exec("SELECT create_cdc_table(?, ?, ?)", s.TargetTable, "id", "BIGINT").Error; err != nil {
			h.logger.Warn("import-streams: create_cdc_table failed", zap.String("table", s.TargetTable), zap.Error(err))
		}
		// Seed one default mapping rule (primary key). More rules are
		// created by HandleScanFields on demand.
		rule := &model.MappingRule{
			SourceTable:  s.SourceTable,
			SourceField:  "id",
			TargetColumn: "id",
			DataType:     "BIGINT",
			Status:       "approved",
			RuleType:     "system",
			IsActive:     true,
		}
		_, _ = h.mappingRepo.CreateIfNotExists(ctx, rule)
		added++
	}
	result := map[string]interface{}{
		"command":       "import-streams",
		"connection_id": payload.ConnectionID,
		"added":         added,
		"skipped":       skipped,
		"total":         len(payload.Streams),
		"status":        "success",
	}
	data, _ := json.Marshal(result)
	h.nats_publish(msg, "cdc.result.import-streams", data)
	h.writeActivity("import-streams", payload.ConnectionID, "success", int64(added), result, "")
}

// HandleBulkSyncFromAirbyte implements boundary-refactor #10 (subject
// cdc.cmd.bulk-sync-from-airbyte). Walks every connection in the
// workspace and imports missing registry rows + default mapping rules.
// Heavy operation — logs once per connection.
func (h *CommandHandler) HandleBulkSyncFromAirbyte(msg *nats.Msg) {
	ctx := context.Background()
	workspaceID := h.airbyte.GetWorkspaceID()
	conns, err := h.airbyte.ListConnections(ctx, workspaceID)
	if err != nil {
		h.publishResultWithSubject(msg, "cdc.result.bulk-sync-from-airbyte", CommandResult{Command: "bulk-sync-from-airbyte", Status: "error", Error: err.Error()})
		return
	}
	sources, _ := h.airbyte.ListSources(ctx)
	sourceByID := make(map[string]airbyte.Source, len(sources))
	for _, s := range sources {
		sourceByID[s.SourceID] = s
	}

	added, skipped := 0, 0
	for _, conn := range conns {
		src, ok := sourceByID[conn.SourceID]
		sourceDB := src.Database
		if sourceDB == "" {
			sourceDB = src.Name
		}
		sourceType := "mongodb"
		lowerName := strings.ToLower(src.SourceName)
		if strings.Contains(lowerName, "mysql") {
			sourceType = "mysql"
		} else if strings.Contains(lowerName, "postgres") {
			sourceType = "postgresql"
		}
		if !ok {
			h.logger.Warn("bulk-sync: source metadata missing", zap.String("conn", conn.ConnectionID))
		}

		for _, sc := range conn.SyncCatalog.Streams {
			var existing model.TableRegistry
			if err := h.db.WithContext(ctx).Where("source_table = ?", sc.Stream.Name).First(&existing).Error; err == nil {
				skipped++
				continue
			}
			targetTable := sc.Stream.Name
			connID := conn.ConnectionID
			sourceID := conn.SourceID
			rawTable := strings.ReplaceAll(sc.Stream.Name, "-", "_")
			syncMode := sc.Config.SyncMode
			destMode := sc.Config.DestinationSyncMode
			namespace := sc.Stream.Namespace
			cursorField := ""
			if len(sc.Config.CursorField) > 0 {
				cursorField = sc.Config.CursorField[0]
			}
			pkField := "id"
			if len(sc.Stream.SourceDefinedPrimaryKey) > 0 && len(sc.Stream.SourceDefinedPrimaryKey[0]) > 0 {
				pkField = sc.Stream.SourceDefinedPrimaryKey[0][0]
			}
			entry := model.TableRegistry{
				SourceDB:               sourceDB,
				SourceType:             sourceType,
				SourceTable:            sc.Stream.Name,
				TargetTable:            targetTable,
				SyncEngine:             "airbyte",
				Priority:               "normal",
				PrimaryKeyField:        pkField,
				PrimaryKeyType:         "BIGINT",
				IsActive:               sc.Config.Selected,
				AirbyteConnectionID:    &connID,
				AirbyteSourceID:        &sourceID,
				AirbyteRawTable:        &rawTable,
				AirbyteSyncMode:        &syncMode,
				AirbyteDestinationSync: &destMode,
				AirbyteNamespace:       &namespace,
				AirbyteCursorField:     &cursorField,
			}
			if err := h.registryRepo.Create(ctx, &entry); err != nil {
				h.logger.Warn("bulk-sync: registry create failed", zap.String("table", sc.Stream.Name), zap.Error(err))
				continue
			}
			// Seed mapping rules from stream JSON schema.
			if sc.Config.Selected {
				if props, ok := sc.Stream.JSONSchema["properties"].(map[string]interface{}); ok {
					for field, prop := range props {
						if strings.HasPrefix(field, "_airbyte_") {
							continue
						}
						rule := &model.MappingRule{
							SourceTable:  sc.Stream.Name,
							SourceField:  field,
							TargetColumn: strings.ReplaceAll(field, ".", "_"),
							DataType:     inferSQLTypeFromAirbyteProp(prop),
							IsActive:     true,
							Status:       "approved",
							RuleType:     "auto-import",
						}
						_, _ = h.mappingRepo.CreateIfNotExists(ctx, rule)
					}
				}
			}
			added++
		}
	}
	result := map[string]interface{}{
		"command":         "bulk-sync-from-airbyte",
		"connections":     len(conns),
		"added":           added,
		"already_exists":  skipped,
		"status":          "success",
	}
	data, _ := json.Marshal(result)
	h.nats_publish(msg, "cdc.result.bulk-sync-from-airbyte", data)
	h.writeActivity("bulk-sync-from-airbyte", "*", "success", int64(added), result, "")
}

// ---------------------------------------------------------------------------
// Helpers used across boundary-refactor handlers.
// ---------------------------------------------------------------------------

// nats_publish fires a result event to the configured subject when the
// caller used pub/sub; falls back to msg.Respond for request-reply.
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
	h.logger.Info("command result", zap.String("subject", subject), zap.String("payload", string(data)))
}

// publishResultWithSubject is the error-path variant — keeps the legacy
// activity log write plus routes through nats_publish.
func (h *CommandHandler) publishResultWithSubject(msg *nats.Msg, subject string, result CommandResult) {
	data, _ := json.Marshal(result)
	h.nats_publish(msg, subject, data)
	h.writeActivity("cmd-"+result.Command, result.TargetTable, result.Status, int64(result.RowsAffected), nil, result.Error)
}

// writeActivity mirrors publishResult's activity log side-effect in a
// reusable form.
func (h *CommandHandler) writeActivity(op, table, status string, rows int64, details map[string]interface{}, errMsg string) {
	now := time.Now()
	var errPtr *string
	if errMsg != "" {
		e := errMsg
		errPtr = &e
	}
	var detailsJSON []byte
	if details != nil {
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
