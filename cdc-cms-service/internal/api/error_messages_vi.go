package api

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
