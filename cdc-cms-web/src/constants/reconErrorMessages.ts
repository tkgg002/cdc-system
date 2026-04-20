/**
 * Vietnamese translation + severity mapping for reconciliation error codes.
 *
 * Source of truth: ADR §2.3 (Error message categorization)
 *   agent/memory/workspaces/feature-cdc-integration/04_decisions_recon_systematic_v4.md
 *
 * Contract:
 *   - Backend (`centralized-data-service/internal/service/recon_core.go`) stamps
 *     one of the codes below on `cdc_reconciliation_report.error_code` whenever
 *     a recon run fails. Successful runs leave it NULL.
 *   - FE renders `ERROR_MESSAGES_VI[code]` instead of the generic "Lỗi nguồn"
 *     the old FE used. When backend invents a new code we don't know about,
 *     the UI falls back to the raw code string — noisy but diagnosable.
 *   - `ERROR_SEVERITY` drives tag color: critical = red, warning = orange,
 *     info = default (gray). Critical codes require operator action (config
 *     drift, schema mismatch, auth); warnings are usually transient and
 *     will self-heal on retry.
 *
 * IMPORTANT: Keep keys in sync with the Go enum in
 *   centralized-data-service/internal/service/recon_error_codes.go
 */

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
  SRC_TIMEOUT:        'Nguồn phản hồi chậm (>5s) — Mongo có thể đang overload',
  SRC_CONNECTION:     'Kết nối nguồn bị ngắt — sẽ retry tự động',
  SRC_FIELD_MISSING:  'Field timestamp không tồn tại — chạy re-detect',
  SRC_EMPTY:          'Nguồn trống trong window 7 ngày (bình thường cho data cũ)',
  DST_MISSING_COLUMN: 'Thiếu cột _source_ts — chạy migration',
  DST_TIMEOUT:        'Postgres phản hồi chậm',
  CIRCUIT_OPEN:       'Circuit breaker đang mở, retry sau 60s',
  AUTH_ERROR:         'Lỗi xác thực kết nối database',
  SRC_QUERY_FAILED:   'Query nguồn thất bại',
  UNKNOWN:            'Lỗi không xác định',
};

export type ReconErrorSeverity = 'critical' | 'warning' | 'info';

export const ERROR_SEVERITY: Record<ReconErrorCode, ReconErrorSeverity> = {
  SRC_TIMEOUT:        'warning',
  SRC_CONNECTION:     'warning',
  SRC_FIELD_MISSING:  'critical',
  SRC_EMPTY:          'info',
  DST_MISSING_COLUMN: 'critical',
  DST_TIMEOUT:        'warning',
  CIRCUIT_OPEN:       'warning',
  AUTH_ERROR:         'critical',
  SRC_QUERY_FAILED:   'critical',
  UNKNOWN:            'warning',
};

/**
 * Safe lookup — tolerates backend codes the FE hasn't shipped support for yet.
 * Returns `{ message, severity }` with sensible defaults.
 */
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
    message: known ? ERROR_MESSAGES_VI[key] : code, // show raw code if unknown
    severity: ERROR_SEVERITY[key],
    known,
  };
}
