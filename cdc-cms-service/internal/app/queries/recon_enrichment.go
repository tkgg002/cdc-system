package queries

import "strings"

// ErrorMessagesVI maps worker-emitted error_code values to Vietnamese
// operator-facing messages. Keys match the enum produced by
// centralized-data-service's recon_source_agent + recon_dest_agent (see
// workspace §2.2). Missing keys fall back to the "UNKNOWN" entry via
// lookup semantics — callers should always use map access and never fail
// hard on a new code.
//
// If you add a new code here: also add it in the worker's error_code
// catalog so CMS + Worker stay in sync.
var ErrorMessagesVI = map[string]string{
	"SRC_TIMEOUT":        "Nguồn phản hồi chậm (>5s) — Mongo có thể đang overload",
	"SRC_CONNECTION":     "Kết nối nguồn Mongo bị ngắt — sẽ retry tự động",
	"SRC_FIELD_MISSING":  "Field timestamp không tồn tại ở nguồn — chạy re-detect",
	"SRC_EMPTY":          "Nguồn trống trong window 7 ngày — bình thường cho data cũ",
	"DST_MISSING_COLUMN": "Destination thiếu cột _source_ts — chạy migration",
	"DST_TIMEOUT":        "Postgres phản hồi chậm",
	"CIRCUIT_OPEN":       "Circuit breaker đang mở, retry sau 60s",
	"AUTH_ERROR":         "Lỗi xác thực kết nối database",
	"SRC_QUERY_FAILED":   "Query nguồn thất bại",
	"UNKNOWN":            "Lỗi không xác định",
}

// ComputeDriftStatus derives (drift_pct, status, error_code) from the
// stored (source_count, dest_count, error_code) triple. Done on the read
// path so stored reports stay authoritative; the FE sees a single
// self-consistent view without re-running math.
//
// Contract (matches workspace §2.2):
//   - error path: any stored error_code or nil source_count => drift_pct=0,
//     status="error", code preserved (or SRC_QUERY_FAILED when src is nil).
//   - 0 vs 0: ok_empty (benign — no data either side).
//   - equal counts: ok.
//   - src>0 && dst==0: dest_missing (catastrophic — sync pipeline stalled).
//   - src==0 && dst>0: source_missing_or_stale (src probably down).
//   - otherwise drift_pct = |src-dst| / max(src,dst) * 100,
//     thresholds: drift >= 5%, warning >= 0.5%, else ok.
//
// Percent is unsigned so "src grew, dst fell" and "dst grew, src fell"
// both surface as the same magnitude.
func ComputeDriftStatus(sourceCount *int64, destCount int64, errorCode string) (float64, string, string) {
	if errorCode != "" {
		return 0, "error", errorCode
	}
	if sourceCount == nil {
		return 0, "error", "SRC_QUERY_FAILED"
	}
	src := *sourceCount
	if src == 0 && destCount == 0 {
		return 0, "ok_empty", ""
	}
	if src == destCount {
		return 0, "ok", ""
	}
	absDiff := src - destCount
	if absDiff < 0 {
		absDiff = -absDiff
	}
	maxVal := src
	if destCount > maxVal {
		maxVal = destCount
	}
	if maxVal < 1 {
		maxVal = 1
	}
	driftPct := float64(absDiff) / float64(maxVal) * 100

	status := "ok"
	switch {
	case src > 0 && destCount == 0:
		status = "dest_missing"
	case src == 0 && destCount > 0:
		status = "source_missing_or_stale"
	case driftPct >= 5:
		status = "drift"
	case driftPct >= 0.5:
		status = "warning"
	}
	return driftPct, status, ""
}

// DeriveSourceQueryMethod explains — in one short label — how the source
// count in the report was computed. Helps operators answer the question
// "why is source=0 when Mongo clearly has rows?" without reading Go code.
//
// Values:
//   - window_updated_at       — default path, Mongo filter on `updated_at`
//   - window_custom_field     — registry override (e.g. `lastUpdatedAt`)
//   - window_id_ts_fallback   — registry field missing AND collection
//     lacks the default, fallback to ObjectID time
//   - full_count              — legacy Tier-3-era `CountDocuments` path
func DeriveSourceQueryMethod(tsField *string, checkType string) string {
	if checkType == "bucket_hash" {
		return "full_count"
	}
	if tsField == nil || *tsField == "" || *tsField == "updated_at" {
		return "window_updated_at"
	}
	if *tsField == "_id" {
		return "window_id_ts_fallback"
	}
	return "window_custom_field"
}

// TrimReconValue is a helper to clean up inputs.
func TrimReconValue(v string) string {
	return strings.TrimSpace(v)
}

// StringOrNil returns nil interface for a nil pointer, else string.
func StringOrNil(v *string) interface{} {
	if v == nil {
		return nil
	}
	return *v
}
