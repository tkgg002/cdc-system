export type ReconErrorCode =
  | 'SRC_TIMEOUT'
  | 'SRC_CONNECTION'
  | 'SRC_FIELD_MISSING'
  | 'SRC_EMPTY'
  | 'DST_MISSING_COLUMN'
  | 'DST_TIMEOUT'
  | 'CIRCUIT_OPEN'
  | 'AUTH_ERROR'
  | 'SRC_QUERY_FAILED'
  | 'UNKNOWN';

export const ERROR_MESSAGES_VI: Record<ReconErrorCode, string> = {
  SRC_TIMEOUT: 'Nguồn phản hồi chậm (>5s) — Mongo có thể đang overload',
  SRC_CONNECTION: 'Kết nối nguồn bị ngắt — sẽ retry tự động',
  SRC_FIELD_MISSING: 'Field timestamp không tồn tại — chạy re-detect',
  SRC_EMPTY: 'Nguồn trống trong window 7 ngày (bình thường cho data cũ)',
  DST_MISSING_COLUMN: 'Thiếu cột _source_ts — chạy migration',
  DST_TIMEOUT: 'Postgres phản hồi chậm',
  CIRCUIT_OPEN: 'Circuit breaker đang mở, retry sau 60s',
  AUTH_ERROR: 'Lỗi xác thực kết nối database',
  SRC_QUERY_FAILED: 'Query nguồn thất bại',
  UNKNOWN: 'Lỗi không xác định',
};

export type ReconErrorSeverity = 'critical' | 'warning' | 'info';

export const ERROR_SEVERITY: Record<ReconErrorCode, ReconErrorSeverity> = {
  SRC_TIMEOUT: 'warning',
  SRC_CONNECTION: 'warning',
  SRC_FIELD_MISSING: 'critical',
  SRC_EMPTY: 'info',
  DST_MISSING_COLUMN: 'critical',
  DST_TIMEOUT: 'warning',
  CIRCUIT_OPEN: 'warning',
  AUTH_ERROR: 'critical',
  SRC_QUERY_FAILED: 'critical',
  UNKNOWN: 'warning',
};

export function lookupReconError(code?: string | null): {
  message: string;
  severity: ReconErrorSeverity;
  known: boolean;
} {
  if (!code) {
    return { message: ERROR_MESSAGES_VI.UNKNOWN, severity: 'warning', known: false };
  }
  const known = code in ERROR_MESSAGES_VI;
  const key = (known ? code : 'UNKNOWN') as ReconErrorCode;
  return {
    message: known ? ERROR_MESSAGES_VI[key] : code,
    severity: ERROR_SEVERITY[key],
    known,
  };
}
