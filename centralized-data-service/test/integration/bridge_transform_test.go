// Integration test: Bridge + Transform E2E pipeline
// Requires: INTEGRATION_TEST_DB_URL env var pointing to a test Postgres instance
// Run: INTEGRATION_TEST_DB_URL="postgres://user:password@localhost:5432/test_cdc?sslmode=disable" go test ./test/integration/ -v -count=1

package integration

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"centralized-data-service/pkgs/idgen"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"
)

func getTestDB(t *testing.T) *pgxpool.Pool {
	dbURL := os.Getenv("INTEGRATION_TEST_DB_URL")
	if dbURL == "" {
		t.Skip("INTEGRATION_TEST_DB_URL not set, skipping integration test")
	}

	pool, err := pgxpool.New(context.Background(), dbURL)
	if err != nil {
		t.Fatalf("connect to test DB: %v", err)
	}

	// Verify connection
	if err := pool.Ping(context.Background()); err != nil {
		t.Fatalf("ping test DB: %v", err)
	}

	return pool
}

func TestBridgeTransformE2E(t *testing.T) {
	pool := getTestDB(t)
	defer pool.Close()
	ctx := context.Background()

	logger, _ := zap.NewDevelopment()
	idgen.Init(logger)

	tableName := fmt.Sprintf("test_e2e_%d", time.Now().UnixMilli())

	// 1. Setup: create Airbyte-like source table
	_, err := pool.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE "%s_airbyte" (
			_id VARCHAR PRIMARY KEY,
			name VARCHAR,
			email VARCHAR,
			amount NUMERIC,
			_airbyte_extracted_at TIMESTAMP DEFAULT NOW(),
			_airbyte_raw_id VARCHAR DEFAULT gen_random_uuid()::VARCHAR
		)`, tableName))
	if err != nil {
		t.Fatalf("create airbyte table: %v", err)
	}
	defer pool.Exec(ctx, fmt.Sprintf(`DROP TABLE IF EXISTS "%s_airbyte"`, tableName))

	// 2. Insert test data
	for i := 0; i < 100; i++ {
		_, err := pool.Exec(ctx, fmt.Sprintf(`
			INSERT INTO "%s_airbyte" (_id, name, email, amount)
			VALUES ($1, $2, $3, $4)`, tableName),
			fmt.Sprintf("id_%d", i),
			fmt.Sprintf("User %d", i),
			fmt.Sprintf("user%d@test.com", i),
			float64(i)*100.50,
		)
		if err != nil {
			t.Fatalf("insert test data: %v", err)
		}
	}

	// 3. Create CDC table (v1.12 schema with source_id)
	seqName := fmt.Sprintf("seq_%s_id", tableName)
	_, err = pool.Exec(ctx, fmt.Sprintf(`CREATE SEQUENCE IF NOT EXISTS "%s"`, seqName))
	if err != nil {
		t.Fatalf("create sequence: %v", err)
	}
	defer pool.Exec(ctx, fmt.Sprintf(`DROP SEQUENCE IF EXISTS "%s"`, seqName))

	_, err = pool.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE "%s" (
			id BIGINT PRIMARY KEY DEFAULT nextval('%s'),
			source_id VARCHAR(200) NOT NULL UNIQUE,
			_raw_data JSONB NOT NULL,
			_source VARCHAR(20) DEFAULT 'airbyte',
			_synced_at TIMESTAMP DEFAULT NOW(),
			_version BIGINT DEFAULT 1,
			_hash VARCHAR(64),
			_deleted BOOLEAN DEFAULT FALSE,
			_created_at TIMESTAMP DEFAULT NOW(),
			_updated_at TIMESTAMP DEFAULT NOW(),
			name VARCHAR,
			email VARCHAR,
			amount NUMERIC
		)`, tableName, seqName))
	if err != nil {
		t.Fatalf("create CDC table: %v", err)
	}
	defer pool.Exec(ctx, fmt.Sprintf(`DROP TABLE IF EXISTS "%s"`, tableName))

	// 4. Bridge: copy from Airbyte → CDC
	bridgeSQL := fmt.Sprintf(`
		INSERT INTO "%s" (source_id, _raw_data, _source, _synced_at, _hash, _version, _created_at, _updated_at)
		SELECT
			src._id,
			to_jsonb(src) - '_airbyte_raw_id' - '_airbyte_extracted_at',
			'airbyte',
			COALESCE(src._airbyte_extracted_at, NOW()),
			md5((to_jsonb(src))::text),
			1, NOW(), NOW()
		FROM "%s_airbyte" src
		ON CONFLICT (source_id) DO UPDATE SET
			_raw_data = EXCLUDED._raw_data,
			_hash = EXCLUDED._hash,
			_version = "%s"._version + 1,
			_updated_at = NOW()
		WHERE "%s"._hash IS DISTINCT FROM EXCLUDED._hash`,
		tableName, tableName, tableName, tableName)

	tag, err := pool.Exec(ctx, bridgeSQL)
	if err != nil {
		t.Fatalf("bridge exec: %v", err)
	}
	t.Logf("Bridge: %d rows affected", tag.RowsAffected())

	// 5. Verify bridge
	var cdcCount int
	pool.QueryRow(ctx, fmt.Sprintf(`SELECT COUNT(*) FROM "%s"`, tableName)).Scan(&cdcCount)
	if cdcCount != 100 {
		t.Fatalf("expected 100 rows in CDC, got %d", cdcCount)
	}

	var nullRawData int
	pool.QueryRow(ctx, fmt.Sprintf(`SELECT COUNT(*) FROM "%s" WHERE _raw_data IS NULL`, tableName)).Scan(&nullRawData)
	if nullRawData != 0 {
		t.Fatalf("expected 0 null _raw_data, got %d", nullRawData)
	}

	// 6. Transform: _raw_data → typed columns
	transformSQL := fmt.Sprintf(`
		UPDATE "%s" SET
			name = (_raw_data->>'name')::VARCHAR,
			email = (_raw_data->>'email')::VARCHAR,
			amount = (_raw_data->>'amount')::NUMERIC,
			_updated_at = NOW()
		WHERE _raw_data IS NOT NULL AND (name IS NULL OR email IS NULL OR amount IS NULL)`,
		tableName)

	tag, err = pool.Exec(ctx, transformSQL)
	if err != nil {
		t.Fatalf("transform exec: %v", err)
	}
	t.Logf("Transform: %d rows affected", tag.RowsAffected())

	// 7. Verify transform
	var transformedCount int
	pool.QueryRow(ctx, fmt.Sprintf(`SELECT COUNT(*) FROM "%s" WHERE name IS NOT NULL AND email IS NOT NULL AND amount IS NOT NULL`, tableName)).Scan(&transformedCount)
	if transformedCount != 100 {
		t.Fatalf("expected 100 transformed rows, got %d", transformedCount)
	}

	// 8. Verify data integrity
	var name, email string
	var amount float64
	pool.QueryRow(ctx, fmt.Sprintf(`SELECT name, email, amount FROM "%s" WHERE source_id = 'id_42'`, tableName)).Scan(&name, &email, &amount)
	if name != "User 42" {
		t.Errorf("expected name 'User 42', got '%s'", name)
	}
	if email != "user42@test.com" {
		t.Errorf("expected email 'user42@test.com', got '%s'", email)
	}
	if amount != 4221.0 {
		t.Errorf("expected amount 4221.0, got %f", amount)
	}

	// 9. Verify Sonyflake IDs are BIGINT (auto-generated from sequence)
	var minID, maxID int64
	pool.QueryRow(ctx, fmt.Sprintf(`SELECT MIN(id), MAX(id) FROM "%s"`, tableName)).Scan(&minID, &maxID)
	if minID <= 0 || maxID <= 0 {
		t.Errorf("expected positive IDs, got min=%d max=%d", minID, maxID)
	}
	t.Logf("ID range: %d - %d (sequence-generated)", minID, maxID)

	// 10. Bridge idempotency: run again, verify no duplicates
	tag, err = pool.Exec(ctx, bridgeSQL)
	if err != nil {
		t.Fatalf("re-bridge exec: %v", err)
	}
	t.Logf("Re-bridge: %d rows affected (should be 0 — data unchanged)", tag.RowsAffected())

	pool.QueryRow(ctx, fmt.Sprintf(`SELECT COUNT(*) FROM "%s"`, tableName)).Scan(&cdcCount)
	if cdcCount != 100 {
		t.Fatalf("expected 100 rows after re-bridge, got %d (duplicates!)", cdcCount)
	}

	t.Log("E2E test passed: bridge → transform → verify → idempotency")
}
