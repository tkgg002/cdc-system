package admin

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

// startEmbeddedNATS spins up an in-process NATS server for unit tests.
func startEmbeddedNATS(t *testing.T) *nats.Conn {
	t.Helper()
	opts := &natsserver.Options{Port: -1, NoLog: true, NoSigs: true}
	s, err := natsserver.NewServer(opts)
	require.NoError(t, err)
	go s.Start()
	t.Cleanup(s.Shutdown)
	if ok := s.ReadyForConnections(3e9); !ok {
		t.Fatal("embedded NATS did not start in time")
	}
	nc, err := nats.Connect(s.ClientURL())
	require.NoError(t, err)
	t.Cleanup(func() { nc.Close() })
	return nc
}

// TestRegisterSource_HappyPath — mock Debezium, Schema Registry, sqlmock, embedded NATS.
func TestRegisterSource_HappyPath(t *testing.T) {
	// ── Mock Debezium ──────────────────────────────────────────────────────
	debezium := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "GET" {
			w.Header().Set("Content-Type", "application/json")
			w.Write([]byte(`{"collection.include.list":"goopay.payment_bills"}`))
			return
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{}`))
	}))
	defer debezium.Close()

	// ── Mock Schema Registry ───────────────────────────────────────────────
	schemaReg := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"compatibility":"NONE"}`))
	}))
	defer schemaReg.Close()

	// ── SQL mock ───────────────────────────────────────────────────────────
	sqlDB, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	require.NoError(t, err)
	defer sqlDB.Close()

	gormDB, err := gorm.Open(postgres.New(postgres.Config{Conn: sqlDB}), &gorm.Config{})
	require.NoError(t, err)

	mock.ExpectBegin()
	// 1a. lookup source connection
	mock.ExpectQuery(`SELECT id FROM cdc_system.connection_registry`).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(int64(8)))
	// 1b. lookup shadow connection
	mock.ExpectQuery(`SELECT id FROM cdc_system.connection_registry`).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(int64(5)))
	// 1c. insert source_object_registry
	mock.ExpectQuery(`INSERT INTO cdc_system.source_object_registry`).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(int64(100)))
	// 1d. insert shadow_binding
	mock.ExpectExec(`INSERT INTO cdc_system.shadow_binding`).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()
	// step5 mark active
	mock.ExpectExec(`UPDATE cdc_system.source_object_registry`).
		WillReturnResult(sqlmock.NewResult(0, 1))

	// ── Embedded NATS ──────────────────────────────────────────────────────
	nc := startEmbeddedNATS(t)

	// ── Build server ───────────────────────────────────────────────────────
	srv := NewServer(Deps{
		DB:                gormDB,
		NATS:              nc,
		DebeziumBaseURL:   debezium.URL,
		SchemaRegistryURL: schemaReg.URL,
		AuthToken:         "testtoken",
		Logger:            zap.NewNop(),
	})

	body := `{
		"object_code":         "test_smoke_happy",
		"source_engine_type":  "mongodb",
		"sync_engine":         "debezium",
		"source_object_name":  "smoke_collection",
		"source_locator":      {"database":"goopay","collection":"smoke_collection"},
		"target_master_table": "payment_bills_addtest",
		"notes":               "unit test happy path"
	}`
	req := httptest.NewRequest("POST", "/v2/sources/register", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer testtoken")
	rec := httptest.NewRecorder()

	srv.engine.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())
	var resp RegisterSourceResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	require.Equal(t, "active", resp.ProvisioningState)
	require.ElementsMatch(t, []string{
		"registry_insert",
		"debezium_include_extend",
		"schema_registry_preempt",
		"worker_signal",
	}, resp.StepsCompleted)
	require.Equal(t, int64(100), resp.SourceObjectID)

	require.NoError(t, mock.ExpectationsWereMet())
}

// TestRegisterSource_Unauthorized — valid token required.
func TestRegisterSource_Unauthorized(t *testing.T) {
	srv := NewServer(Deps{
		DB:                nil,
		NATS:              nil,
		DebeziumBaseURL:   "http://localhost",
		SchemaRegistryURL: "http://localhost",
		AuthToken:         "secret",
		Logger:            zap.NewNop(),
	})
	req := httptest.NewRequest("POST", "/v2/sources/register", strings.NewReader("{}"))
	req.Header.Set("Content-Type", "application/json")
	// Không set Authorization header
	rec := httptest.NewRecorder()
	srv.engine.ServeHTTP(rec, req)
	require.Equal(t, http.StatusUnauthorized, rec.Code)
}

// TestRegisterSource_BadRequest — thiếu required fields.
func TestRegisterSource_BadRequest(t *testing.T) {
	srv := NewServer(Deps{
		DB:                nil,
		NATS:              nil,
		DebeziumBaseURL:   "http://localhost",
		SchemaRegistryURL: "http://localhost",
		AuthToken:         "",
		Logger:            zap.NewNop(),
	})
	body := `{"object_code":"only_code"}` // thiếu source_engine_type v.v.
	req := httptest.NewRequest("POST", "/v2/sources/register", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	srv.engine.ServeHTTP(rec, req)
	require.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestRegisterSource_Step2Fail — Debezium trả 500 → 207 partial.
func TestRegisterSource_Step2Fail(t *testing.T) {
	debezium := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "connector not found", http.StatusInternalServerError)
	}))
	defer debezium.Close()

	schemaReg := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
	}))
	defer schemaReg.Close()

	sqlDB, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	require.NoError(t, err)
	defer sqlDB.Close()

	gormDB, err := gorm.Open(postgres.New(postgres.Config{Conn: sqlDB}), &gorm.Config{})
	require.NoError(t, err)

	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT id FROM cdc_system.connection_registry`).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(int64(8)))
	mock.ExpectQuery(`SELECT id FROM cdc_system.connection_registry`).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(int64(5)))
	mock.ExpectQuery(`INSERT INTO cdc_system.source_object_registry`).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(int64(200)))
	mock.ExpectExec(`INSERT INTO cdc_system.shadow_binding`).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()
	// markProvisioningFailed
	mock.ExpectExec(`UPDATE cdc_system.source_object_registry`).
		WillReturnResult(sqlmock.NewResult(0, 1))

	nc := startEmbeddedNATS(t)

	srv := NewServer(Deps{
		DB:                gormDB,
		NATS:              nc,
		DebeziumBaseURL:   debezium.URL,
		SchemaRegistryURL: schemaReg.URL,
		AuthToken:         "",
		Logger:            zap.NewNop(),
	})

	body := `{
		"object_code":         "test_step2fail",
		"source_engine_type":  "mongodb",
		"sync_engine":         "debezium",
		"source_object_name":  "fail_col",
		"source_locator":      {"database":"goopay","collection":"fail_col"},
		"target_master_table": "payment_bills_addtest"
	}`
	req := httptest.NewRequest("POST", "/v2/sources/register", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	srv.engine.ServeHTTP(rec, req)

	require.Equal(t, http.StatusMultiStatus, rec.Code, "body: %s", rec.Body.String())
	var resp RegisterSourceResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	require.Equal(t, "step2_failed", resp.ProvisioningState)
	require.Contains(t, resp.StepsCompleted, "registry_insert")
	require.NotEmpty(t, resp.LastStepError)
}

// TestHelpers_ContainsCSV — unit test cho containsCSV.
func TestHelpers_ContainsCSV(t *testing.T) {
	require.True(t, containsCSV("a,b,c", "b"))
	require.True(t, containsCSV("goopay.payment_bills , goopay.orders", "goopay.orders"))
	require.False(t, containsCSV("a,b,c", "d"))
	require.False(t, containsCSV("", "a"))
}

// TestHelpers_TopicNameFor — topic naming convention.
func TestHelpers_TopicNameFor(t *testing.T) {
	req := RegisterSourceRequest{
		SourceEngineType: "mongodb",
		SourceLocator:    map[string]interface{}{"database": "payment-bill-service", "collection": "payment-bills"},
		SourceObjectName: "payment-bills",
	}
	topic := topicNameFor(req)
	require.Equal(t, "cdc.goopay.payment-bill-service.payment-bills", topic)
}

// TestHelpers_ShadowSchemaFor — shadow schema naming.
func TestHelpers_ShadowSchemaFor(t *testing.T) {
	cases := []struct {
		engine string
		db     string
		want   string
	}{
		{"mongodb", "goopay", "shadow_goopay_mongo"},
		{"postgresql", "goopay_source", "shadow_goopay_source"},
		{"mariadb", "goopay_maria", "shadow_goopay_maria_mariadb"},
	}
	for _, tc := range cases {
		req := RegisterSourceRequest{
			SourceEngineType: tc.engine,
			SourceLocator:    map[string]interface{}{"database": tc.db},
		}
		got := shadowSchemaFor(req)
		require.Equal(t, tc.want, got, "engine=%s db=%s", tc.engine, tc.db)
	}
}

// ──────────────────────────────────────────────
// Tests for extendDatabaseList
// ──────────────────────────────────────────────

func TestExtendDatabaseList_NewValue(t *testing.T) {
	cfg := map[string]string{"database.include.list": "a,b"}
	val, added := extendDatabaseList(cfg, "database.include.list", "c")
	assert.Equal(t, "a,b,c", val)
	assert.True(t, added)
}

func TestExtendDatabaseList_AlreadyPresent(t *testing.T) {
	cfg := map[string]string{"database.include.list": "a,b"}
	val, added := extendDatabaseList(cfg, "database.include.list", "b")
	assert.Equal(t, "a,b", val)
	assert.False(t, added)
}

func TestExtendDatabaseList_EmptyConfig(t *testing.T) {
	cfg := map[string]string{}
	val, added := extendDatabaseList(cfg, "database.include.list", "a")
	assert.Equal(t, "a", val)
	assert.True(t, added)
}

// ──────────────────────────────────────────────
// Tests for extendConfigInMemory (multi-tier)
// ──────────────────────────────────────────────

func TestExtendDebeziumInclude_Mongo_BothTiers(t *testing.T) {
	cfg := map[string]string{
		"database.include.list":   "service-a",
		"collection.include.list": "service-a.col1",
	}
	// namespaceName = collection name only (no db prefix); extendConfigInMemory ghép db+"."+coll
	res, err := extendConfigInMemory(cfg, "mongodb", "service-b", "col2")
	assert.NoError(t, err)
	assert.True(t, res.DatabaseTierAdded)
	assert.True(t, res.CollectionTierAdded)
	assert.Equal(t, "service-a,service-b", cfg["database.include.list"])
	assert.Equal(t, "service-a.col1,service-b.col2", cfg["collection.include.list"])
}

func TestExtendDebeziumInclude_Mongo_DBExistsCollNew(t *testing.T) {
	cfg := map[string]string{
		"database.include.list":   "service-a",
		"collection.include.list": "service-a.col1",
	}
	// Same db, new collection
	res, err := extendConfigInMemory(cfg, "mongodb", "service-a", "col2")
	assert.NoError(t, err)
	assert.False(t, res.DatabaseTierAdded)
	assert.True(t, res.CollectionTierAdded)
}

func TestExtendDebeziumInclude_PG_DBLockMismatch(t *testing.T) {
	cfg := map[string]string{"database.dbname": "goopay_source"}
	_, err := extendConfigInMemory(cfg, "postgresql", "other_db", "public.tbl")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "pg connector locked to db=goopay_source")
}
