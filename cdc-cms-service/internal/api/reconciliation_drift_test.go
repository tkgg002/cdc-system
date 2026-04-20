package api

import (
	"math"
	"testing"
)

// TestComputeDriftStatus exercises every branch of the drift-status state
// machine so regressions in the FE contract (workspace §2.2) get caught
// before they ship. Table-driven; each case documents the operator-visible
// outcome alongside the math.
func TestComputeDriftStatus(t *testing.T) {
	i64 := func(v int64) *int64 { return &v }
	tests := []struct {
		name       string
		src        *int64
		dst        int64
		errIn      string
		wantPct    float64
		wantStatus string
		wantCode   string
	}{
		// --- error paths ---------------------------------------------------
		{"worker error code preserved", i64(10), 10, "SRC_TIMEOUT", 0, "error", "SRC_TIMEOUT"},
		{"nil src becomes SRC_QUERY_FAILED", nil, 0, "", 0, "error", "SRC_QUERY_FAILED"},
		{"nil src with dst rows still error", nil, 42, "", 0, "error", "SRC_QUERY_FAILED"},
		// --- benign equal paths --------------------------------------------
		{"both zero is ok_empty", i64(0), 0, "", 0, "ok_empty", ""},
		{"equal non-zero counts", i64(100), 100, "", 0, "ok", ""},
		{"equal large counts", i64(1_000_000), 1_000_000, "", 0, "ok", ""},
		// --- catastrophic directional drift --------------------------------
		{"src rows, dst empty = dest_missing", i64(500), 0, "", 100, "dest_missing", ""},
		{"src empty, dst has rows = source_missing_or_stale", i64(0), 7, "", 100, "source_missing_or_stale", ""},
		// --- threshold edges -----------------------------------------------
		{"10% drift triggers drift status", i64(100), 90, "", 10, "drift", ""},
		{"50% drift triggers drift status", i64(100), 50, "", 50, "drift", ""},
		{"5% drift is the drift floor", i64(100), 95, "", 5, "drift", ""},
		{"0.5% drift → warning", i64(1000), 995, "", 0.5, "warning", ""},
		{"0.4% drift → ok (below warning floor)", i64(1000), 996, "", 0.4, "ok", ""},
		// --- asymmetric drift (dst grew past src) --------------------------
		{"dst grew 20% past src", i64(100), 120, "", 100.0 / 120.0 * 20, "drift", ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			pct, status, code := ComputeDriftStatus(tc.src, tc.dst, tc.errIn)
			if math.Abs(pct-tc.wantPct) > 0.01 {
				t.Errorf("pct: got %.4f want %.4f", pct, tc.wantPct)
			}
			if status != tc.wantStatus {
				t.Errorf("status: got %q want %q", status, tc.wantStatus)
			}
			if code != tc.wantCode {
				t.Errorf("code: got %q want %q", code, tc.wantCode)
			}
		})
	}
}

// TestErrorMessagesVICoverage ensures the VI translation table covers
// every error code the worker currently emits, so the FE never shows
// an empty error_message_vi for a known code.
func TestErrorMessagesVICoverage(t *testing.T) {
	required := []string{
		"SRC_TIMEOUT", "SRC_CONNECTION", "SRC_FIELD_MISSING", "SRC_EMPTY",
		"DST_MISSING_COLUMN", "DST_TIMEOUT", "CIRCUIT_OPEN", "AUTH_ERROR",
		"SRC_QUERY_FAILED", "UNKNOWN",
	}
	for _, code := range required {
		if _, ok := ErrorMessagesVI[code]; !ok {
			t.Errorf("missing VI translation for %s", code)
		}
	}
}
