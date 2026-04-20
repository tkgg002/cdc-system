// Load test: measure bridge throughput for 5K rows/sec target
// Run: INTEGRATION_TEST_DB_URL="postgres://..." go test ./test/integration/ -run TestBridgeLoad -v -count=1

package integration

import (
	"context"
	"fmt"
	"testing"
	"time"

	"centralized-data-service/pkgs/idgen"

	"go.uber.org/zap"
)

func TestBridgeLoad(t *testing.T) {
	pool := getTestDB(t)
	defer pool.Close()
	ctx := context.Background()

	logger, _ := zap.NewDevelopment()
	idgen.Init(logger)

	const totalRows = 50000 // 50K rows for meaningful benchmark
	tableName := fmt.Sprintf("test_load_%d", time.Now().UnixMilli())

	// 1. Create Airbyte source table + insert bulk data
	_, err := pool.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE "%s_airbyte" (
			_id VARCHAR PRIMARY KEY,
			business_name VARCHAR,
			email VARCHAR,
			phone VARCHAR,
			status VARCHAR,
			amount NUMERIC,
			metadata JSONB DEFAULT '{}',
			_airbyte_extracted_at TIMESTAMP DEFAULT NOW(),
			_airbyte_raw_id VARCHAR DEFAULT gen_random_uuid()::VARCHAR
		)`, tableName))
	if err != nil {
		t.Fatalf("create table: %v", err)
	}
	defer pool.Exec(ctx, fmt.Sprintf(`DROP TABLE IF EXISTS "%s_airbyte"`, tableName))

	t.Logf("Inserting %d rows into Airbyte table...", totalRows)
	insertStart := time.Now()
	for batch := 0; batch < totalRows/1000; batch++ {
		tx, _ := pool.Begin(ctx)
		for i := 0; i < 1000; i++ {
			idx := batch*1000 + i
			tx.Exec(ctx, fmt.Sprintf(`
				INSERT INTO "%s_airbyte" (_id, business_name, email, phone, status, amount)
				VALUES ($1, $2, $3, $4, $5, $6)`, tableName),
				fmt.Sprintf("load_%d", idx),
				fmt.Sprintf("Business %d", idx),
				fmt.Sprintf("biz%d@load.com", idx),
				fmt.Sprintf("+84%010d", idx),
				"active",
				float64(idx)*1.5,
			)
		}
		tx.Commit(ctx)
	}
	t.Logf("Insert done: %s (%d rows/sec)", time.Since(insertStart), int(float64(totalRows)/time.Since(insertStart).Seconds()))

	// 2. Create CDC table (v1.12 schema)
	seqName := fmt.Sprintf("seq_%s_id", tableName)
	pool.Exec(ctx, fmt.Sprintf(`CREATE SEQUENCE IF NOT EXISTS "%s"`, seqName))
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
			_updated_at TIMESTAMP DEFAULT NOW()
		)`, tableName, seqName))
	if err != nil {
		t.Fatalf("create CDC table: %v", err)
	}
	defer pool.Exec(ctx, fmt.Sprintf(`DROP TABLE IF EXISTS "%s"`, tableName))

	// 3. BENCHMARK: Bridge (SQL-based, same as production)
	t.Logf("Bridging %d rows...", totalRows)
	bridgeStart := time.Now()

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
		t.Fatalf("bridge: %v", err)
	}
	bridgeElapsed := time.Since(bridgeStart)
	bridgeRPS := float64(tag.RowsAffected()) / bridgeElapsed.Seconds()
	t.Logf("Bridge: %d rows in %s (%.0f rows/sec)", tag.RowsAffected(), bridgeElapsed, bridgeRPS)

	if bridgeRPS < 5000 {
		t.Logf("WARNING: Bridge throughput %.0f rows/sec below 5K target", bridgeRPS)
	} else {
		t.Logf("PASS: Bridge throughput %.0f rows/sec meets 5K target", bridgeRPS)
	}

	// 4. Verify
	var count int
	pool.QueryRow(ctx, fmt.Sprintf(`SELECT COUNT(*) FROM "%s"`, tableName)).Scan(&count)
	if count != totalRows {
		t.Fatalf("expected %d rows, got %d", totalRows, count)
	}

	t.Logf("Load test complete: %d rows bridged at %.0f rows/sec", totalRows, bridgeRPS)
}
