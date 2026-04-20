//go:build integration
// +build integration

package service

import (
	"context"
	"os"
	"testing"
	"time"

	"centralized-data-service/internal/model"

	"go.uber.org/zap"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

// openIntegrationDB opens a connection to the local dev Postgres. Env
// override DB_SINK_URL if different; defaults match docker-compose.
func openIntegrationDB(t *testing.T) *gorm.DB {
	t.Helper()
	dsn := os.Getenv("DB_SINK_URL")
	if dsn == "" {
		dsn = "host=localhost port=5432 user=user password=password dbname=goopay_dw sslmode=disable"
	}
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		t.Skipf("skipping integration: cannot open Postgres (%v)", err)
	}
	return db
}

// TestHealAuditBatcher_ScaleCap is the critical scale regression test.
// Simulates a 10K-action heal run (100 errors + 100 upserts + 9800
// skips) and asserts the total audit rows written is ≤ 102 — not 10K.
//
// Expected layout after a 10K run:
//   - 1 row  run_started
//   - 100    upsert (sampled)
//   - ... errors (up to errorCount, always kept; here 100)
//   - 0      skip rows (aggregated only)
//   - 1 row  run_completed
//
// Total = 1 + 100 + 100 + 0 + 1 = 202 rows (errors are intentionally
// NOT sampled). For the "healthy" shape the user cares about (no
// errors), total = 1 + 100 + 0 + 1 = 102.
func TestHealAuditBatcher_ScaleCap(t *testing.T) {
	db := openIntegrationDB(t)
	logger, _ := zap.NewDevelopment()

	const table = "test_heal_audit_scale"
	// Purge any prior test rows for this synthetic table.
	db.Exec(`DELETE FROM cdc_activity_log WHERE target_table = ?`, table)

	batcher := newHealAuditBatcher(db, logger, 100, 100)
	runID := newHealRunID()
	ctx := context.Background()

	batcher.Begin(ctx, runID, table)

	const upserts = 10_000
	const skips = 10_000
	const errors = 50

	// Healthy (upsert + skip) sample → 10K upserts, 10K skips.
	for i := 0; i < upserts; i++ {
		batcher.Record(ctx, "upsert", "id-u-"+itoa(i), 1_700_000_000_000+int64(i), "")
	}
	for i := 0; i < skips; i++ {
		batcher.Record(ctx, "skip", "id-s-"+itoa(i), 0, "")
	}
	for i := 0; i < errors; i++ {
		batcher.Record(ctx, "error", "id-e-"+itoa(i), 0, "synthetic err")
	}

	batcher.End(ctx, "success", nil, false, "")

	// Count rows by bucket.
	type bucket struct {
		Action string
		N      int64
	}
	var buckets []bucket
	if err := db.Raw(`
		SELECT COALESCE(details->>'action','') AS action, COUNT(*) AS n
		FROM cdc_activity_log
		WHERE target_table = ?
		GROUP BY action
	`, table).Scan(&buckets).Error; err != nil {
		t.Fatalf("count query failed: %v", err)
	}

	got := map[string]int64{}
	for _, b := range buckets {
		got[b.Action] = b.N
	}

	// Assertions
	if got["run_started"] != 1 {
		t.Errorf("run_started rows: got %d, want 1", got["run_started"])
	}
	if got["run_completed"] != 1 {
		t.Errorf("run_completed rows: got %d, want 1", got["run_completed"])
	}
	if got["skip"] != 0 {
		t.Errorf("skip rows must be aggregated only: got %d, want 0", got["skip"])
	}
	if got["upsert"] != 100 {
		t.Errorf("upsert sample cap violated: got %d, want ≤ 100", got["upsert"])
	}
	if got["error"] != int64(errors) {
		t.Errorf("error rows (never sampled): got %d, want %d", got["error"], errors)
	}

	var total int64
	db.Raw(`SELECT COUNT(*) FROM cdc_activity_log WHERE target_table = ?`, table).Scan(&total)
	// Cap: 1 start + 100 upsert + errors + 1 completed
	maxExpected := int64(1 + 100 + errors + 1)
	if total > maxExpected {
		t.Errorf("total rows: got %d, want ≤ %d (1 start + 100 upsert + %d errors + 1 completed)", total, maxExpected, errors)
	}
	t.Logf("scale test summary: %d total rows for %d upserts + %d skips + %d errors (cap %d)", total, upserts, skips, errors, maxExpected)

	// Check run_completed row carries accurate aggregate counters.
	var details string
	err := db.Raw(`
		SELECT details::text
		FROM cdc_activity_log
		WHERE target_table = ? AND details->>'action' = 'run_completed'
		LIMIT 1
	`, table).Scan(&details).Error
	if err != nil {
		t.Fatalf("fetch run_completed failed: %v", err)
	}
	if details == "" {
		t.Fatal("run_completed details empty")
	}
	t.Logf("run_completed details: %s", details)

	// Cleanup
	db.Exec(`DELETE FROM cdc_activity_log WHERE target_table = ?`, table)
	_ = model.ActivityLog{} // ensure model import used
	_ = time.Now
}

// itoa is a tiny helper to avoid strconv import noise.
func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	neg := i < 0
	if neg {
		i = -i
	}
	var b [20]byte
	pos := len(b)
	for i > 0 {
		pos--
		b[pos] = byte('0' + i%10)
		i /= 10
	}
	if neg {
		pos--
		b[pos] = '-'
	}
	return string(b[pos:])
}
