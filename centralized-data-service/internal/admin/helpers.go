package admin

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"go.uber.org/zap"
)

// ──────────────────────────────────────────────
// Shadow schema naming
// ──────────────────────────────────────────────

// shadowSchemaFor derives the PostgreSQL schema name for shadow tables.
// Convention: shadow_<database> (mongo appends _mongo suffix).
func shadowSchemaFor(req RegisterSourceRequest) string {
	db := stringFromLocator(req.SourceLocator, "database")
	switch req.SourceEngineType {
	case "postgresql":
		return "shadow_" + strings.ReplaceAll(db, "-", "_")
	case "mongodb":
		return "shadow_" + strings.ReplaceAll(db, "-", "_") + "_mongo"
	case "mariadb", "mysql":
		return "shadow_" + strings.ReplaceAll(db, "-", "_") + "_mariadb"
	}
	return "shadow_default"
}

// sourceObjectTypeFor derives object type (table|collection|view).
func sourceObjectTypeFor(req RegisterSourceRequest) string {
	if req.SourceObjectType != "" {
		return req.SourceObjectType
	}
	switch req.SourceEngineType {
	case "mongodb":
		return "collection"
	default:
		return "table"
	}
}

// normalizedSourceKeyFor — unique key dùng UNIQUE constraint normalized_source_key.
// Format: <engine>:<database>:<namespace>.<object>
func normalizedSourceKeyFor(req RegisterSourceRequest) string {
	db := stringFromLocator(req.SourceLocator, "database")
	return fmt.Sprintf("%s:%s:%s", req.SourceEngineType, db, req.SourceObjectName)
}

func stringFromLocator(loc map[string]interface{}, key string) string {
	if v, ok := loc[key].(string); ok {
		return v
	}
	return ""
}

// ──────────────────────────────────────────────
// Debezium / topic helpers
// ──────────────────────────────────────────────

// qualifiedSourceObjectName — format dùng trong Debezium include list.
// Mongo: "<database>.<collection>"
// PG: "<schema>.<table>"
func qualifiedSourceObjectName(req RegisterSourceRequest) string {
	switch req.SourceEngineType {
	case "postgresql":
		ns := stringFromLocator(req.SourceLocator, "schema")
		if ns == "" {
			ns = "public"
		}
		return ns + "." + stringFromLocator(req.SourceLocator, "table")
	case "mongodb":
		return stringFromLocator(req.SourceLocator, "database") + "." +
			stringFromLocator(req.SourceLocator, "collection")
	case "mariadb", "mysql":
		return stringFromLocator(req.SourceLocator, "database") + "." +
			req.SourceObjectName
	}
	return req.SourceObjectName
}

// includeKeyFor — Debezium config key tùy engine.
func includeKeyFor(engineType string) string {
	switch engineType {
	case "mongodb":
		return "collection.include.list"
	default:
		return "table.include.list"
	}
}

// connectorNameFor — resolve connector name từ Debezium.
// Thực tế verified:
//
//	mongodb  → "goopay-mongodb-cdc"  (P0.1 smoke verified)
//	postgres → "cdc-pg-source"
//	mariadb  → "cdc-mariadb-source"
//
// TODO: load mapping từ env DEBEZIUM_CONNECTORS_JSON để không hardcode.
func connectorNameFor(engineType string, _ map[string]interface{}) string {
	switch engineType {
	case "mongodb":
		return "goopay-mongodb-cdc"
	case "postgresql":
		return "cdc-pg-source"
	case "mariadb", "mysql":
		return "cdc-mariadb-source"
	}
	return ""
}

// topicNameFor — Kafka topic dự kiến cho subject Schema Registry preempt.
// Convention thực tế (verified từ Schema Registry subjects):
//
//	cdc.goopay.<database>.<collection_or_table>
//
// Ví dụ: cdc.goopay.payment-bill-service.payment-bills
func topicNameFor(req RegisterSourceRequest) string {
	prefix := "goopay" // TODO: đọc từ env TOPIC_PREFIX nếu cần đa-cluster
	db := stringFromLocator(req.SourceLocator, "database")
	var obj string
	switch req.SourceEngineType {
	case "mongodb":
		obj = stringFromLocator(req.SourceLocator, "collection")
	case "postgresql":
		obj = req.SourceObjectName
	default:
		obj = req.SourceObjectName
	}
	return fmt.Sprintf("cdc.%s.%s.%s", prefix, db, obj)
}

// ──────────────────────────────────────────────
// Debezium include list extend
// ──────────────────────────────────────────────

// ExtendResult — kết quả của extendDebeziumInclude, cho biết tier nào thực sự thay đổi.
type ExtendResult struct {
	DatabaseTierAdded   bool
	CollectionTierAdded bool
	UpdatedConfig       map[string]string
}

// extendDatabaseList appends `value` to `config[key]` nếu chưa có.
// Idempotent: gọi 2 lần với cùng value → lần 2 wasAdded=false.
// Returns (updatedValue, wasAdded).
func extendDatabaseList(config map[string]string, key, value string) (string, bool) {
	current := strings.TrimSpace(config[key])
	if current == "" {
		config[key] = value
		return value, true
	}
	parts := strings.Split(current, ",")
	seen := make(map[string]struct{}, len(parts))
	for _, p := range parts {
		seen[strings.TrimSpace(p)] = struct{}{}
	}
	if _, ok := seen[value]; ok {
		return current, false
	}
	next := current + "," + value
	config[key] = next
	return next, true
}

// extendConfigInMemory — per-engine multi-tier include list extension (pure, no HTTP).
// mongodb: database.include.list + collection.include.list (db.coll)
// mysql/mariadb: database.include.list + table.include.list (db.tbl)
// postgres: verify database.dbname match (fail-fast per L-cascade-liability), extend table.include.list (schema.tbl)
// default: error "unsupported source_type"
func extendConfigInMemory(config map[string]string, sourceType, databaseName, namespaceName string) (*ExtendResult, error) {
	res := &ExtendResult{UpdatedConfig: config}
	switch sourceType {
	case "mongodb":
		_, res.DatabaseTierAdded = extendDatabaseList(config, "database.include.list", databaseName)
		_, res.CollectionTierAdded = extendDatabaseList(config, "collection.include.list", databaseName+"."+namespaceName)
	case "mysql", "mariadb":
		_, res.DatabaseTierAdded = extendDatabaseList(config, "database.include.list", databaseName)
		_, res.CollectionTierAdded = extendDatabaseList(config, "table.include.list", databaseName+"."+namespaceName)
	case "postgres", "postgresql":
		// PG single-database constraint — fail-fast nếu mismatch (per L-cascade-liability)
		if cur := strings.TrimSpace(config["database.dbname"]); cur != "" && cur != databaseName {
			return nil, fmt.Errorf("pg connector locked to db=%s, requested db=%s", cur, databaseName)
		}
		// namespaceName cho PG là "schema.table"
		_, res.CollectionTierAdded = extendDatabaseList(config, "table.include.list", namespaceName)
	default:
		return nil, fmt.Errorf("unsupported source_type: %s", sourceType)
	}
	return res, nil
}

// extendDebeziumInclude — GET current config → extend multi-tier per engine → PUT updated config.
// Returns *ExtendResult (DatabaseTierAdded, CollectionTierAdded) để caller emit warnings.
func (s *Server) extendDebeziumInclude(ctx context.Context, req RegisterSourceRequest) (*ExtendResult, error) {
	connector := connectorNameFor(req.SourceEngineType, req.SourceLocator)
	if connector == "" {
		return nil, fmt.Errorf("cannot derive connector name for engine %q", req.SourceEngineType)
	}
	url := fmt.Sprintf("%s/connectors/%s/config", s.deps.DebeziumBaseURL, connector)

	getReq, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("build get request: %w", err)
	}
	resp, err := http.DefaultClient.Do(getReq)
	if err != nil {
		return nil, fmt.Errorf("get connector config: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		b, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("get connector config %d: %s", resp.StatusCode, b)
	}

	var cfg map[string]string
	if err := json.NewDecoder(resp.Body).Decode(&cfg); err != nil {
		return nil, fmt.Errorf("decode connector config: %w", err)
	}

	databaseName := stringFromLocator(req.SourceLocator, "database")
	// collectionOrTable: tên object cụ thể dùng cho tier-thấp (không có db prefix).
	// extendConfigInMemory sẽ ghép databaseName+"."+collectionOrTable cho Mongo/MySQL.
	// Với PG: qualifiedSourceObjectName trả "schema.table" — đây chính là tier-thấp item.
	var collectionOrTable string
	switch req.SourceEngineType {
	case "mongodb":
		collectionOrTable = stringFromLocator(req.SourceLocator, "collection")
	case "mysql", "mariadb":
		collectionOrTable = req.SourceObjectName
	default: // postgresql
		collectionOrTable = qualifiedSourceObjectName(req) // "schema.table"
	}
	result, err := extendConfigInMemory(cfg, req.SourceEngineType, databaseName, collectionOrTable)
	if err != nil {
		return nil, err
	}

	// Idempotent: nếu cả 2 tiers đã tồn tại → skip PUT
	if !result.DatabaseTierAdded && !result.CollectionTierAdded {
		s.deps.Logger.Info("debezium include lists already contain both tiers, skipping PUT")
		return result, nil
	}

	body, _ := json.Marshal(cfg)
	putReq, err := http.NewRequestWithContext(ctx, "PUT", url, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("build put request: %w", err)
	}
	putReq.Header.Set("Content-Type", "application/json")
	putResp, err := http.DefaultClient.Do(putReq)
	if err != nil {
		return nil, fmt.Errorf("put connector config: %w", err)
	}
	defer putResp.Body.Close()
	if putResp.StatusCode >= 300 {
		b, _ := io.ReadAll(putResp.Body)
		return nil, fmt.Errorf("put connector config %d: %s", putResp.StatusCode, b)
	}
	return result, nil
}

// ──────────────────────────────────────────────
// Schema Registry preempt compat=NONE
// ──────────────────────────────────────────────

// preemptSchemaRegistry — PUT compat=NONE cho subject <topic>-value.
// 404 → subject chưa tồn tại → skip (global default applies khi message đầu xuất hiện).
func (s *Server) preemptSchemaRegistry(ctx context.Context, req RegisterSourceRequest) error {
	topic := topicNameFor(req)
	subject := topic + "-value"
	url := fmt.Sprintf("%s/config/%s", s.deps.SchemaRegistryURL, subject)

	body := []byte(`{"compatibility":"NONE"}`)
	putReq, err := http.NewRequestWithContext(ctx, "PUT", url, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("build schema registry request: %w", err)
	}
	putReq.Header.Set("Content-Type", "application/vnd.schemaregistry.v1+json")

	resp, err := http.DefaultClient.Do(putReq)
	if err != nil {
		return fmt.Errorf("put compat: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == 404 {
		// Subject chưa tồn tại — Schema Registry sẽ dùng global default compat.
		// Đây là expected behavior khi topic chưa có message.
		s.deps.Logger.Info("schema registry subject not found (404), skip compat preempt — global default applies",
			zap.String("subject", subject))
		return nil
	}
	if resp.StatusCode >= 300 {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("put compat %d: %s", resp.StatusCode, b)
	}
	return nil
}

// containsCSV checks nếu needle tồn tại trong CSV string.
func containsCSV(csv, needle string) bool {
	if csv == "" || needle == "" {
		return false
	}
	for _, p := range strings.Split(csv, ",") {
		if strings.TrimSpace(p) == strings.TrimSpace(needle) {
			return true
		}
	}
	return false
}
