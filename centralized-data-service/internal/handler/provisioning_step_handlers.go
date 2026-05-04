// Package handler — provisioning_step_handlers.go
//
// Phase D — two NEW worker handlers that close the auto-loop:
//
//   cdc.cmd.shadow.bind      → HandleShadowBind
//   cdc.cmd.schedule.enable  → HandleScheduleEnable
//
// Architect rulings:
//   Q1 — HandleShadowBind MUST go through SchemaAdapter
//        (PrepareForCDCInsertInSchema, P2-hardened with auto CREATE
//        TABLE IF NOT EXISTS) so a fresh source can land without
//        manual bootstrap.
//   Q3 — HandleScheduleEnable only flips is_enabled=true and emits
//        NOTHING. The first successful transmute tick (via JobMonitor
//        on cdc.evt.transmute.completed status=success) is what
//        publishes cdc.evt.provisioning.step_completed for
//        schedule_enable. State `schedule_pending` is the tracker.
//
// HandleMasterCreate (cdc.cmd.master.bind alias, Q2) lives in
// master_ddl_handler.go — extended in-place with provisioning flag.
// HandleDiscover (cdc.cmd.discover) lives in command_handler.go —
// wired with defer emit in-place.
package handler

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"centralized-data-service/internal/service"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jackc/pgx/v5"
	"github.com/nats-io/nats.go"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

// ProvisioningStepHandler bundles the deps for the two new step
// handlers (shadow_bind, schedule_enable). master_bind and discover
// are extended in-place since they share their existing handlers'
// deps.
type ProvisioningStepHandler struct {
	db            *gorm.DB
	natsConn      *nats.Conn
	schemaAdapter *service.SchemaAdapter
	// mongoClient is OPTIONAL. Phase multi_engine_unified Cascade
	// Liability gate (lesson L1399 + L994 schemaless inference): when
	// the source row carries source_engine_type='mongodb', shadow_bind
	// pre-flights the source collection (exists + non-empty) before
	// cementing the shadow schema. nil disables the gate (PG/MariaDB
	// still get the universal Discover gate downstream).
	mongoClient *mongo.Client
	logger      *zap.Logger
}

func NewProvisioningStepHandler(
	db *gorm.DB,
	natsConn *nats.Conn,
	schemaAdapter *service.SchemaAdapter,
	mongoClient *mongo.Client,
	logger *zap.Logger,
) *ProvisioningStepHandler {
	if logger == nil {
		logger = zap.NewNop()
	}
	return &ProvisioningStepHandler{
		db: db, natsConn: natsConn,
		schemaAdapter: schemaAdapter,
		mongoClient:   mongoClient,
		logger:        logger,
	}
}

// shadowBindRequest — payload schema published by orchestrator's
// Advance(draft → shadow_pending). source_id resolved by orchestrator;
// schema/table/pk derived here from source_object_registry.
type shadowBindRequest struct {
	SourceID      int64  `json:"source_id"`
	SourceObject  string `json:"source_object_name,omitempty"`
	SchemaName    string `json:"shadow_schema,omitempty"`
	TableName     string `json:"shadow_table,omitempty"`
	PKColumn      string `json:"pk_column,omitempty"`
	CorrelationID string `json:"correlation_id,omitempty"`
	TriggeredBy   string `json:"triggered_by,omitempty"`
	TraceID       string `json:"trace_id,omitempty"`
	SpanID        string `json:"span_id,omitempty"`
}

// HandleShadowBind subscribes to cdc.cmd.shadow.bind. Q1 ruling:
// MUST call SchemaAdapter.PrepareForCDCInsertInSchema so the shadow
// table is auto-created (P2 idempotent CREATE TABLE IF NOT EXISTS)
// then registers the binding row. Emits step_completed at the end
// regardless of outcome (defer + named return).
func (h *ProvisioningStepHandler) HandleShadowBind(msg *nats.Msg) {
	var req shadowBindRequest
	if uErr := json.Unmarshal(msg.Data, &req); uErr != nil {
		h.logger.Warn("shadow_bind: bad payload", zap.Error(uErr))
		return
	}

	var stepErr error
	defer func() {
		emitStepCompleted(h.natsConn, h.logger,
			req.SourceID, "shadow_bind", stepErr,
			req.CorrelationID, "shadow_bind_handler",
			req.TraceID, req.SpanID)
	}()

	if req.SourceID == 0 {
		stepErr = fmt.Errorf("shadow_bind: source_id required")
		h.logger.Warn(stepErr.Error())
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	resolved, rErr := h.resolveShadowTarget(ctx, req)
	if rErr != nil {
		stepErr = fmt.Errorf("resolve shadow target: %w", rErr)
		return
	}

	// Phase multi_engine_unified — Cascade Liability gate for Mongo
	// schemaless source. Refuse to cement a shadow schema if the source
	// collection is missing or empty: cdcCols-only shadow + zero docs
	// would surface as a phantom `running` source with no data flow.
	if eng, db, obj, eErr := h.fetchSourceEngine(ctx, req.SourceID); eErr == nil && isMongoEngine(eng) {
		if pErr := h.preflightMongoSource(ctx, db, obj); pErr != nil {
			stepErr = fmt.Errorf("mongo source preflight: %w", pErr)
			return
		}
	}

	if h.schemaAdapter == nil {
		stepErr = fmt.Errorf("shadow_bind: schemaAdapter not wired")
		return
	}

	// Phase Auto Provisioning Feature A — clone source schema so the
	// shadow table has business columns to discover. Inference is best
	// effort: any failure logs a warning and falls back to PK-only
	// (legacy V1 behaviour). The Discover step's universal gate
	// downstream still catches "0 mapping rules" and pins state to
	// failed if inference yielded nothing.
	businessCols := h.inferSourceColumns(ctx, req.SourceID, resolved.PKColumn)

	if pErr := h.schemaAdapter.PrepareForCDCInsertWithBusinessCols(
		resolved.SchemaName, resolved.TableName, resolved.PKColumn, businessCols,
	); pErr != nil {
		stepErr = fmt.Errorf("prepare shadow table %s.%s: %w",
			resolved.SchemaName, resolved.TableName, pErr)
		return
	}

	if bErr := h.upsertShadowBinding(ctx, req.SourceID, resolved); bErr != nil {
		stepErr = fmt.Errorf("upsert shadow_binding: %w", bErr)
		return
	}

	h.logger.Info("shadow_bind: success",
		zap.Int64("source_id", req.SourceID),
		zap.String("schema", resolved.SchemaName),
		zap.String("table", resolved.TableName),
		zap.String("correlation_id", req.CorrelationID))
}

type shadowTarget struct {
	SchemaName, TableName, PKColumn string
}

// isMongoEngine — accept both `mongodb` (canonical) and `mongo`
// (loose value some legacy seeds use) for the gate switch.
func isMongoEngine(eng string) bool {
	switch strings.ToLower(strings.TrimSpace(eng)) {
	case "mongodb", "mongo":
		return true
	}
	return false
}

// fetchSourceEngine reads the engine triple
// (source_engine_type, source_database, source_object_name) used by
// the Mongo pre-flight gate. Returns ("",...,err) on lookup failure
// — caller treats that as "skip the gate" so non-Mongo flows are
// unaffected if the row is missing during a race.
func (h *ProvisioningStepHandler) fetchSourceEngine(
	ctx context.Context, sourceID int64,
) (engine, db, obj string, err error) {
	var row struct {
		Engine string `gorm:"column:source_engine_type"`
		DB     string `gorm:"column:source_database"`
		Obj    string `gorm:"column:source_object_name"`
	}
	err = h.db.WithContext(ctx).Raw(
		`SELECT source_engine_type, source_database, source_object_name
		 FROM cdc_system.source_object_registry WHERE id = ?`, sourceID,
	).Scan(&row).Error
	return row.Engine, row.DB, row.Obj, err
}

// preflightMongoSource validates that the Mongo source collection
// exists and is non-empty BEFORE shadow_bind cements a cdcCols-only
// shadow schema. Three failure modes are surfaced as step errors so
// the orchestrator pins state to `failed` instead of cascading:
//   - mongoClient is nil (not wired): explicit fail (we WERE asked
//     to gate, but cannot)
//   - source_database / source_object_name unset
//   - EstimatedDocumentCount returns 0 or transport error
func (h *ProvisioningStepHandler) preflightMongoSource(
	ctx context.Context, dbName, collName string,
) error {
	if h.mongoClient == nil {
		return fmt.Errorf("mongoClient not wired — cannot pre-flight Mongo source")
	}
	if dbName == "" || collName == "" {
		return fmt.Errorf("source_database/source_object_name unset on registry row")
	}
	coll := h.mongoClient.Database(dbName).Collection(collName)
	cnt, err := coll.EstimatedDocumentCount(ctx)
	if err != nil {
		return fmt.Errorf("estimatedDocumentCount(%s.%s): %w", dbName, collName, err)
	}
	if cnt == 0 {
		return fmt.Errorf("collection %s.%s is empty — refusing to cascade with no source data to infer schema from", dbName, collName)
	}
	return nil
}

// resolveShadowTarget — fills missing schema/table/pk from
// source_object_registry. Convention: schema = "shadow_<connection>",
// table = source_object_name. PK falls back to "id" if registry has
// no explicit primary_key column.
func (h *ProvisioningStepHandler) resolveShadowTarget(
	ctx context.Context, req shadowBindRequest,
) (shadowTarget, error) {
	if req.SchemaName != "" && req.TableName != "" && req.PKColumn != "" {
		return shadowTarget{req.SchemaName, req.TableName, req.PKColumn}, nil
	}
	var row struct {
		SourceObjectName string  `gorm:"column:source_object_name"`
		ConnectionCode   string  `gorm:"column:connection_code"`
		PrimaryKeyField  *string `gorm:"column:primary_key_field"`
	}
	if err := h.db.WithContext(ctx).Raw(
		`SELECT sor.source_object_name,
		        cr.connection_code,
		        sor.primary_key_field
		   FROM cdc_system.source_object_registry sor
		   LEFT JOIN cdc_system.connection_registry cr
		          ON cr.id = sor.source_connection_id
		  WHERE sor.id = ?`, req.SourceID).Scan(&row).Error; err != nil {
		return shadowTarget{}, fmt.Errorf("registry lookup: %w", err)
	}
	if row.SourceObjectName == "" {
		return shadowTarget{}, fmt.Errorf("source_id=%d not found in registry", req.SourceID)
	}
	out := shadowTarget{
		SchemaName: req.SchemaName,
		TableName:  req.TableName,
		PKColumn:   req.PKColumn,
	}
	if out.SchemaName == "" {
		conn := strings.TrimSpace(row.ConnectionCode)
		if conn == "" {
			conn = "default"
		}
		out.SchemaName = "shadow_" + conn
	}
	if out.TableName == "" {
		out.TableName = row.SourceObjectName
	}
	if out.PKColumn == "" {
		if row.PrimaryKeyField != nil && *row.PrimaryKeyField != "" {
			out.PKColumn = *row.PrimaryKeyField
		} else {
			out.PKColumn = "id"
		}
	}
	return out, nil
}

// upsertShadowBinding — INSERT into cdc_system.shadow_binding,
// idempotent via ON CONFLICT (binding_code). Schema constraints:
// shadow_connection_id, binding_code, physical_table_fqn are NOT NULL.
// PK metadata stays in source_object_registry.primary_key_field; the
// shadow_binding table itself has no pk column.
func (h *ProvisioningStepHandler) upsertShadowBinding(
	ctx context.Context, sourceID int64, t shadowTarget,
) error {
	var shadowConn struct {
		ID int64 `gorm:"column:id"`
	}
	q := h.db.WithContext(ctx).Table("cdc_system.connection_registry").
		Select("id").Where("role_type = ? AND status = ?", "shadow", "active")
	if code := strings.TrimSpace(os.Getenv("PROVISIONING_DEFAULT_SHADOW_CONNECTION_CODE")); code != "" {
		q = q.Where("connection_code = ?", code)
	}
	if err := q.Order("id ASC").Limit(1).Scan(&shadowConn).Error; err != nil {
		return fmt.Errorf("lookup shadow connection: %w", err)
	}
	if shadowConn.ID == 0 {
		return fmt.Errorf("no active shadow connection (set PROVISIONING_DEFAULT_SHADOW_CONNECTION_CODE)")
	}
	bindingCode := fmt.Sprintf("shadow_src_%d", sourceID)
	fqn := fmt.Sprintf("%s.%s", t.SchemaName, t.TableName)
	return h.db.WithContext(ctx).Exec(
		`INSERT INTO cdc_system.shadow_binding
		   (binding_code, source_object_id, shadow_connection_id,
		    shadow_schema, shadow_table, physical_table_fqn,
		    is_active, created_at, updated_at)
		 VALUES (?, ?, ?, ?, ?, ?, true, NOW(), NOW())
		 ON CONFLICT (binding_code) DO UPDATE
		    SET shadow_connection_id = EXCLUDED.shadow_connection_id,
		        shadow_schema        = EXCLUDED.shadow_schema,
		        shadow_table         = EXCLUDED.shadow_table,
		        physical_table_fqn   = EXCLUDED.physical_table_fqn,
		        is_active            = true,
		        updated_at           = NOW()`,
		bindingCode, sourceID, shadowConn.ID, t.SchemaName, t.TableName, fqn).Error
}

// scheduleEnableRequest — payload from Advance(mapping_ready →
// schedule_pending). master_table identifies the transmute_schedule
// row to flip.
type scheduleEnableRequest struct {
	SourceID      int64  `json:"source_id"`
	MasterTable   string `json:"master_table"`
	CorrelationID string `json:"correlation_id,omitempty"`
	TriggeredBy   string `json:"triggered_by,omitempty"`
	TraceID       string `json:"trace_id,omitempty"`
	SpanID        string `json:"span_id,omitempty"`
}

// HandleScheduleEnable subscribes to cdc.cmd.schedule.enable. Q3
// ruling: only enable the schedule row; DO NOT emit step_completed
// here. JobMonitor.HandleCompleted publishes the event after the
// first successful transmute tick (status='success'). The state
// `schedule_pending` is the cross-event tracker — RecoveryLoop's
// 10-min TTL bounds the wait so a transmute that never runs flips
// to failed.
func (h *ProvisioningStepHandler) HandleScheduleEnable(msg *nats.Msg) {
	var req scheduleEnableRequest
	if uErr := json.Unmarshal(msg.Data, &req); uErr != nil {
		h.logger.Warn("schedule_enable: bad payload", zap.Error(uErr))
		return
	}

	// Failure-path emit only. On success, JobMonitor handles the emit.
	var stepErr error
	defer func() {
		if stepErr == nil {
			return
		}
		emitStepCompleted(h.natsConn, h.logger,
			req.SourceID, "schedule_enable", stepErr,
			req.CorrelationID, "schedule_enable_handler",
			req.TraceID, req.SpanID)
	}()

	if req.SourceID == 0 || req.MasterTable == "" {
		stepErr = fmt.Errorf("schedule_enable: source_id and master_table required (got source_id=%d master_table=%q)",
			req.SourceID, req.MasterTable)
		h.logger.Warn(stepErr.Error())
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Resolve master_binding_id from source_id — the active binding
	// seeded by master_bind step. transmute_schedule is keyed by
	// (master_binding_id, mode), not by master_table.
	var bindingID int64
	if err := h.db.WithContext(ctx).Raw(
		`SELECT id FROM cdc_system.master_binding
		  WHERE source_object_id = ?
		    AND is_active = true
		  ORDER BY id DESC
		  LIMIT 1`, req.SourceID).Row().Scan(&bindingID); err != nil {
		stepErr = fmt.Errorf("lookup master_binding: %w", err)
		return
	}
	if bindingID == 0 {
		stepErr = fmt.Errorf("no active master_binding for source_id=%d — master_bind step must run first", req.SourceID)
		return
	}

	cron := strings.TrimSpace(os.Getenv("PROVISIONING_DEFAULT_CRON_EXPR"))
	if cron == "" {
		cron = "*/1 * * * *"
	}
	res := h.db.WithContext(ctx).Exec(
		`INSERT INTO cdc_system.transmute_schedule
		    (master_binding_id, mode, cron_expr, is_enabled, created_by, created_at, updated_at)
		 VALUES (?, 'cron', ?, true, ?, NOW(), NOW())
		 ON CONFLICT (master_binding_id, mode) DO UPDATE
		    SET is_enabled = true,
		        cron_expr  = COALESCE(cdc_system.transmute_schedule.cron_expr, EXCLUDED.cron_expr),
		        updated_at = NOW()`,
		bindingID, cron, "provisioning:"+req.TriggeredBy)
	if res.Error != nil {
		stepErr = fmt.Errorf("upsert schedule: %w", res.Error)
		return
	}

	h.logger.Info("schedule_enable: armed (awaiting first transmute tick)",
		zap.Int64("source_id", req.SourceID),
		zap.String("master_table", req.MasterTable),
		zap.Int64("master_binding_id", bindingID),
		zap.String("cron", cron),
		zap.Int64("rows_affected", res.RowsAffected),
		zap.String("correlation_id", req.CorrelationID))
}

// inferSourceColumns — Phase Auto Provisioning Feature A. Reads
// source_object_registry to learn (engine, db, object) for the source,
// then connects to the source DB via env-supplied DSN to introspect
// (PG/MariaDB) or sample (Mongo) the business columns. Returns a
// best-effort manifest; on any failure path returns nil so the caller
// silently falls back to PK-only shadow shape (Discover gate downstream
// catches the empty-mapping case).
//
// DSN env conventions (per-connection takes priority, then engine-wide
// default, then source_database — only for libpq host where the DB lives
// inside the URL):
//
//	SOURCE_DSN_<connection_id>     // exact match wins
//	SOURCE_PG_DSN                   // PG default, e.g. postgres://u:p@host:5432
//	SOURCE_MYSQL_DSN                // MariaDB default, e.g. user:pwd@tcp(host:3306)/
//	(Mongo reuses h.mongoClient — no extra env)
func (h *ProvisioningStepHandler) inferSourceColumns(
	ctx context.Context, sourceID int64, pkColumn string,
) []service.BusinessColumn {
	type srcRow struct {
		Engine       string `gorm:"column:source_engine_type"`
		DB           string `gorm:"column:source_database"`
		Schema       string `gorm:"column:source_schema"`
		Obj          string `gorm:"column:source_object_name"`
		ConnectionID int64  `gorm:"column:source_connection_id"`
	}
	var row srcRow
	if err := h.db.WithContext(ctx).Raw(
		`SELECT source_engine_type, source_database,
		        COALESCE(source_schema, '') AS source_schema,
		        source_object_name, source_connection_id
		   FROM cdc_system.source_object_registry WHERE id = ?`, sourceID,
	).Scan(&row).Error; err != nil {
		h.logger.Warn("infer source cols: registry lookup failed",
			zap.Int64("source_id", sourceID), zap.Error(err))
		return nil
	}
	engine := strings.ToLower(strings.TrimSpace(row.Engine))
	switch {
	case engine == "mongodb" || engine == "mongo":
		cols, err := h.inferMongoCols(ctx, row.DB, row.Obj, pkColumn)
		if err != nil {
			h.logger.Warn("infer mongo cols failed",
				zap.Int64("source_id", sourceID), zap.Error(err))
			return nil
		}
		return cols
	case engine == "postgres" || engine == "postgresql" || engine == "pg":
		dsn := pickSourceDSN(row.ConnectionID, "SOURCE_PG_DSN")
		if dsn == "" {
			h.logger.Warn("infer pg cols: no DSN configured",
				zap.Int64("source_id", sourceID),
				zap.String("hint", "set SOURCE_PG_DSN or SOURCE_DSN_<connection_id>"))
			return nil
		}
		schema := row.Schema
		if schema == "" {
			schema = "public"
		}
		cols, err := h.inferPGCols(ctx, dsn, schema, row.Obj, pkColumn)
		if err != nil {
			h.logger.Warn("infer pg cols failed",
				zap.Int64("source_id", sourceID), zap.Error(err))
			return nil
		}
		return cols
	case engine == "mariadb" || engine == "mysql":
		dsn := pickSourceDSN(row.ConnectionID, "SOURCE_MYSQL_DSN")
		if dsn == "" {
			h.logger.Warn("infer mysql cols: no DSN configured",
				zap.Int64("source_id", sourceID),
				zap.String("hint", "set SOURCE_MYSQL_DSN or SOURCE_DSN_<connection_id>"))
			return nil
		}
		cols, err := h.inferMySQLCols(ctx, dsn, row.DB, row.Obj, pkColumn)
		if err != nil {
			h.logger.Warn("infer mysql cols failed",
				zap.Int64("source_id", sourceID), zap.Error(err))
			return nil
		}
		return cols
	default:
		h.logger.Warn("infer source cols: unsupported engine — falling back to PK-only",
			zap.Int64("source_id", sourceID), zap.String("engine", row.Engine))
		return nil
	}
}

func pickSourceDSN(connectionID int64, fallbackEnv string) string {
	if connectionID > 0 {
		if v := strings.TrimSpace(os.Getenv(fmt.Sprintf("SOURCE_DSN_%d", connectionID))); v != "" {
			return v
		}
	}
	return strings.TrimSpace(os.Getenv(fallbackEnv))
}

// inferPGCols — query information_schema on the SOURCE database. The
// shadow lives elsewhere (cdc_dw); this dial is a separate transient
// connection, closed before return. PK is excluded so SchemaAdapter's
// inline pkIdent stays the single owner of the key column.
func (h *ProvisioningStepHandler) inferPGCols(
	ctx context.Context, dsn, schema, table, pkColumn string,
) ([]service.BusinessColumn, error) {
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("dial source pg: %w", err)
	}
	defer conn.Close(ctx)
	rows, err := conn.Query(ctx,
		`SELECT column_name, data_type, is_nullable
		   FROM information_schema.columns
		  WHERE table_schema = $1 AND table_name = $2
		  ORDER BY ordinal_position`,
		schema, table)
	if err != nil {
		return nil, fmt.Errorf("query information_schema: %w", err)
	}
	defer rows.Close()
	var out []service.BusinessColumn
	for rows.Next() {
		var name, dataType, isNullable string
		if err := rows.Scan(&name, &dataType, &isNullable); err != nil {
			return nil, err
		}
		if strings.EqualFold(name, pkColumn) {
			continue
		}
		out = append(out, service.BusinessColumn{
			Name:     name,
			DataType: pgSafeType(dataType),
			Nullable: strings.EqualFold(isNullable, "YES"),
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

// inferMySQLCols — same idea as inferPGCols but against MariaDB's
// information_schema. Type mapping is conservative: anything outside
// the explicit safe-list lands as TEXT to avoid runtime ALTER drama.
func (h *ProvisioningStepHandler) inferMySQLCols(
	ctx context.Context, dsn, dbName, table, pkColumn string,
) ([]service.BusinessColumn, error) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("open mysql: %w", err)
	}
	defer db.Close()
	if err := db.PingContext(ctx); err != nil {
		return nil, fmt.Errorf("ping mysql: %w", err)
	}
	rows, err := db.QueryContext(ctx,
		`SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE
		   FROM information_schema.COLUMNS
		  WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
		  ORDER BY ORDINAL_POSITION`,
		dbName, table)
	if err != nil {
		return nil, fmt.Errorf("query information_schema: %w", err)
	}
	defer rows.Close()
	var out []service.BusinessColumn
	for rows.Next() {
		var name, dataType, isNullable string
		if err := rows.Scan(&name, &dataType, &isNullable); err != nil {
			return nil, err
		}
		if strings.EqualFold(name, pkColumn) {
			continue
		}
		out = append(out, service.BusinessColumn{
			Name:     name,
			DataType: mysqlToPGType(dataType),
			Nullable: strings.EqualFold(isNullable, "YES"),
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

// inferMongoCols — sample one document from the collection and turn its
// top-level keys into a column manifest. Mongo is schemaless so this
// represents "shape at sample time"; new fields landing later just get
// ignored by the discover step until a re-bind. Conservative typing:
// any container value (subdoc, array) → JSONB; anything else not in
// the safe-list → TEXT.
func (h *ProvisioningStepHandler) inferMongoCols(
	ctx context.Context, dbName, collName, pkColumn string,
) ([]service.BusinessColumn, error) {
	if h.mongoClient == nil {
		return nil, fmt.Errorf("mongoClient not wired")
	}
	coll := h.mongoClient.Database(dbName).Collection(collName)
	var doc bson.M
	if err := coll.FindOne(ctx, bson.M{}).Decode(&doc); err != nil {
		return nil, fmt.Errorf("find sample doc: %w", err)
	}
	var out []service.BusinessColumn
	for k, v := range doc {
		if strings.EqualFold(k, pkColumn) {
			continue
		}
		out = append(out, service.BusinessColumn{
			Name:     k,
			DataType: bsonToPGType(v),
			Nullable: true,
		})
	}
	return out, nil
}

// pgSafeType — pass through PG types but normalize a couple of variants
// information_schema uses (e.g. "character varying" → VARCHAR). Returns
// the literal verbatim where possible so we don't lose precision/length.
func pgSafeType(dataType string) string {
	dt := strings.ToLower(strings.TrimSpace(dataType))
	switch dt {
	case "character varying":
		return "TEXT"
	case "character":
		return "TEXT"
	case "timestamp without time zone":
		return "TIMESTAMP"
	case "timestamp with time zone":
		return "TIMESTAMPTZ"
	case "double precision":
		return "DOUBLE PRECISION"
	case "user-defined":
		return "TEXT"
	default:
		return strings.ToUpper(dt)
	}
}

// mysqlToPGType — minimal-blast safe mapping. Anything fancy (SET,
// ENUM, GEOMETRY, ...) lands as TEXT.
func mysqlToPGType(dataType string) string {
	switch strings.ToLower(strings.TrimSpace(dataType)) {
	case "tinyint", "smallint", "mediumint", "int", "integer":
		return "INTEGER"
	case "bigint":
		return "BIGINT"
	case "decimal", "numeric":
		return "NUMERIC"
	case "float":
		return "REAL"
	case "double":
		return "DOUBLE PRECISION"
	case "bit", "bool", "boolean":
		return "BOOLEAN"
	case "date":
		return "DATE"
	case "datetime", "timestamp":
		return "TIMESTAMP"
	case "time":
		return "TIME"
	case "year":
		return "INTEGER"
	case "json":
		return "JSONB"
	case "varchar", "char", "text", "tinytext", "mediumtext", "longtext":
		return "TEXT"
	case "binary", "varbinary", "blob", "tinyblob", "mediumblob", "longblob":
		return "BYTEA"
	default:
		return "TEXT"
	}
}

// bsonToPGType — runtime type switch on a sampled BSON value. Any
// surprising shape lands as TEXT; nested docs/arrays go to JSONB so
// the shadow keeps a faithful payload until master-side mapping rules
// reshape it.
func bsonToPGType(v interface{}) string {
	switch v.(type) {
	case nil:
		return "TEXT"
	case bool:
		return "BOOLEAN"
	case int32, int:
		return "INTEGER"
	case int64:
		return "BIGINT"
	case float32, float64:
		return "DOUBLE PRECISION"
	case string:
		return "TEXT"
	case primitive.ObjectID:
		return "TEXT"
	case primitive.DateTime:
		return "TIMESTAMPTZ"
	case primitive.Decimal128:
		return "NUMERIC"
	case bson.M, bson.D, bson.A, []interface{}, map[string]interface{}:
		return "JSONB"
	default:
		return "TEXT"
	}
}
