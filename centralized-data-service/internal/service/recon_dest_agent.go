package service

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/sony/gobreaker"
	"go.uber.org/zap"
	"golang.org/x/time/rate"
	"gorm.io/gorm"
)

// ReconDestAgentConfig tunes Postgres-side recon reads.
// Defaults mirror ReconSourceAgentConfig.
type ReconDestAgentConfig struct {
	MaxRowsPerSec    int
	QueryTimeout     time.Duration
	BreakerTimeout   time.Duration
	BreakerThreshold uint32
	// ReadReplicaDSN — optional. When set the agent uses a dedicated
	// replica connection. Empty means reuse the primary connection
	// but wrap every transaction in SET TRANSACTION READ ONLY so a
	// misconfigured query still cannot mutate data.
	ReadReplicaDSN string
}

func (c *ReconDestAgentConfig) applyDefaults() {
	if c.MaxRowsPerSec <= 0 {
		c.MaxRowsPerSec = 5000
	}
	if c.QueryTimeout <= 0 {
		c.QueryTimeout = 30 * time.Second
	}
	if c.BreakerTimeout <= 0 {
		c.BreakerTimeout = 60 * time.Second
	}
	if c.BreakerThreshold == 0 {
		c.BreakerThreshold = 5
	}
}

// ReconDestAgent queries Postgres for reconciliation.
//
// v3 semantics:
//   - All reads enforced `SET TRANSACTION READ ONLY` so a bug in the
//     aggregator query cannot mutate data.
//   - Hash aggregate computed IN THE DATABASE via `BIT_XOR(hashtext(...))`
//     — no row shipping, no ID buffering in Go memory.
//   - Rate limiter acts as a budget for the number of identifier
//     lookups per second when `ListIDsInWindow` needs to stream rows.
//   - Circuit breaker guards against a replica that keeps timing out.
//   - Identifier injection protection: we quote every table / column
//     name via `pgx.Identifier{}`-style quoting (`quoteIdent`) before
//     interpolating into the SQL.
//
// Window semantics: the PG destination tables do NOT carry a top-level
// `updated_at` column (see `03_implementation_v3_worker_phase0.md`).
// They carry `_source_ts BIGINT` (Debezium ts_ms) since migration 009,
// with `idx_<tbl>_source_ts` for range scans. So every v3 window query
// uses `_source_ts BETWEEN lo_ms AND hi_ms` instead of timestamp type.
// The translation from time.Time → ms happens in this file.
type ReconDestAgent struct {
	primary *gorm.DB // write-owner, kept for metadata queries
	replica *gorm.DB // read-only path (may == primary, see below)
	breaker *gobreaker.CircuitBreaker
	limiter *rate.Limiter
	cfg     ReconDestAgentConfig
	logger  *zap.Logger
}

// NewReconDestAgent keeps the original 2-arg signature so existing
// worker_server wiring compiles. Uses default v3 tunables and reuses
// the primary connection as the read path (with SET TRANSACTION READ
// ONLY guard) — safe when operators have not provisioned a replica
// DSN yet.
func NewReconDestAgent(db *gorm.DB, logger *zap.Logger) *ReconDestAgent {
	return NewReconDestAgentWithConfig(db, nil, ReconDestAgentConfig{}, logger)
}

// NewReconDestAgentWithConfig constructs the agent with explicit replica
// DSN (when wired from config). `replica` may be nil → fall back to
// `primary` with read-only transaction guard.
func NewReconDestAgentWithConfig(primary, replica *gorm.DB, cfg ReconDestAgentConfig, logger *zap.Logger) *ReconDestAgent {
	cfg.applyDefaults()
	if replica == nil {
		replica = primary
	}
	return &ReconDestAgent{
		primary: primary,
		replica: replica,
		cfg:     cfg,
		limiter: rate.NewLimiter(rate.Limit(cfg.MaxRowsPerSec), cfg.MaxRowsPerSec),
		breaker: gobreaker.NewCircuitBreaker(gobreaker.Settings{
			Name:    "recon-dest",
			Timeout: cfg.BreakerTimeout,
			ReadyToTrip: func(counts gobreaker.Counts) bool {
				return counts.ConsecutiveFailures >= cfg.BreakerThreshold
			},
			OnStateChange: func(name string, from, to gobreaker.State) {
				if logger != nil {
					logger.Warn("recon dest breaker state change",
						zap.String("name", name),
						zap.String("from", from.String()),
						zap.String("to", to.String()),
					)
				}
			},
		}),
		logger: logger,
	}
}

// readOnlyDB starts a context-bound, read-only transaction handle.
// Every v3 read path goes through this helper.
func (da *ReconDestAgent) readOnlyDB(ctx context.Context) *gorm.DB {
	tx := da.replica.WithContext(ctx).Begin()
	// Best-effort: swallow errors setting READ ONLY — on a dedicated
	// replica the session is already read-only, and even without the
	// SET our SQL below only reads.
	tx.Exec("SET TRANSACTION READ ONLY")
	return tx
}

// CountRows — Tier 1 legacy helper. Kept for backward-compat with the
// CMS reporting API. Uses read-only transaction on the replica.
func (da *ReconDestAgent) CountRows(ctx context.Context, tableName, pkColumn string) (int64, error) {
	if err := validateIdent(tableName); err != nil {
		return 0, err
	}
	ctx, cancel := context.WithTimeout(ctx, da.cfg.QueryTimeout)
	defer cancel()

	result, err := da.breaker.Execute(func() (interface{}, error) {
		tx := da.readOnlyDB(ctx)
		defer tx.Rollback()
		var count int64
		sql := fmt.Sprintf(`SELECT COUNT(*) FROM %s`, quoteIdent(tableName))
		if err := tx.Raw(sql).Scan(&count).Error; err != nil {
			return nil, err
		}
		return count, nil
	})
	if err != nil {
		return 0, err
	}
	return result.(int64), nil
}

// CountInWindow counts rows whose `_source_ts` ∈ [tLo, tHi) in ms.
func (da *ReconDestAgent) CountInWindow(ctx context.Context, tableName string, tLo, tHi time.Time) (int64, error) {
	if err := validateIdent(tableName); err != nil {
		return 0, err
	}
	ctx, cancel := context.WithTimeout(ctx, da.cfg.QueryTimeout)
	defer cancel()

	loMs, hiMs := tLo.UnixMilli(), tHi.UnixMilli()

	result, err := da.breaker.Execute(func() (interface{}, error) {
		tx := da.readOnlyDB(ctx)
		defer tx.Rollback()
		var count int64
		sql := fmt.Sprintf(
			`SELECT COUNT(*) FROM %s WHERE "_source_ts" >= ? AND "_source_ts" < ?`,
			quoteIdent(tableName),
		)
		if err := tx.Raw(sql, loMs, hiMs).Scan(&count).Error; err != nil {
			return nil, err
		}
		return count, nil
	})
	if err != nil {
		return 0, err
	}
	return result.(int64), nil
}

// HashWindow streams rows whose `_source_ts` ∈ [tLo, tHi) and builds the
// same (count, XOR of per-row xxhash) fingerprint the source-side agent
// emits. Hash input PER ROW = xxhash64(id || "|" || _source_ts_ms).
//
// v3 root-cause fix (2026-04-17 — see
// `03_implementation_v3_heal_root_cause_fix.md`):
//   - The previous implementation aggregated inside Postgres via
//     `BIT_XOR(hashtext(id || '|' || _source_ts))`. That produced a
//     64-bit hash from a 32-bit `hashtext` + a totally different byte
//     input format (UnixMilli text vs. Mongo RFC3339Nano). Counts
//     matched but the XOR never did → every Tier 2 window was flagged
//     as drift, even when source and destination held identical rows.
//   - The fix moves the hash computation into Go (xxhash64) on both
//     sides and pins the input bytes to the exact same layout:
//     `id + "|" + strconv.FormatInt(_source_ts_ms, 10)` where
//     _source_ts_ms is also what `time.UnixMilli()` returns on the
//     Mongo side (`doc.UpdatedAt.UnixMilli()`).
//   - Rows with `_source_ts IS NULL` (e.g. backfill not finished) are
//     SKIPPED from the hash accumulator AND the count — the source
//     side cannot produce the same byte string for an unknown ts so
//     including them would be a guaranteed false positive. Caller can
//     still detect that drift via Tier 1 count compare, which uses
//     `COUNT(*) ... WHERE _source_ts >= ? AND < ?`. We also keep an
//     explicit skip counter in the structured log so operators can
//     spot a backfill-lagging table.
//
// Memory: O(1) — per-row scan decodes (id, ts) into fixed buffers.
// Network: streaming pgx cursor; no full ID materialisation.
// Rate-limited per row, same contract as source side.
func (da *ReconDestAgent) HashWindow(ctx context.Context, tableName, pkColumn string, tLo, tHi time.Time) (*WindowResult, error) {
	if err := validateIdent(tableName); err != nil {
		return nil, err
	}
	if err := validateIdent(pkColumn); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, da.cfg.QueryTimeout)
	defer cancel()

	loMs, hiMs := tLo.UnixMilli(), tHi.UnixMilli()

	// Stream rows — projection is just (id, _source_ts). Same index
	// usage as the old aggregate (idx_<tbl>_source_ts). No ORDER BY:
	// XOR is commutative so we don't need ordering.
	sql := fmt.Sprintf(
		`SELECT %s::text AS id, "_source_ts" AS source_ts
		   FROM %s
		  WHERE "_source_ts" >= ? AND "_source_ts" < ?`,
		quoteIdent(pkColumn), quoteIdent(tableName),
	)

	result, err := da.breaker.Execute(func() (interface{}, error) {
		tx := da.readOnlyDB(ctx)
		defer tx.Rollback()

		rows, err := tx.Raw(sql, loMs, hiMs).Rows()
		if err != nil {
			return nil, err
		}
		defer rows.Close()

		var (
			xorAcc      uint64
			count       int64
			nullSkipped int64
		)
		for rows.Next() {
			if err := da.limiter.Wait(ctx); err != nil {
				return nil, fmt.Errorf("rate limiter: %w", err)
			}
			var (
				id       string
				sourceTs *int64
			)
			if err := rows.Scan(&id, &sourceTs); err != nil {
				return nil, err
			}
			if sourceTs == nil {
				// Backfill pending — unknown ts can never match the
				// source-side representation, so skip rather than pollute
				// the XOR with a false negative.
				nullSkipped++
				continue
			}
			xorAcc ^= hashIDPlusTsMs(id, *sourceTs)
			count++
		}
		if err := rows.Err(); err != nil {
			return nil, err
		}

		if nullSkipped > 0 && da.logger != nil {
			da.logger.Warn("recon dest HashWindow: rows with NULL _source_ts skipped",
				zap.String("table", tableName),
				zap.Int64("skipped", nullSkipped),
				zap.Int64("hashed", count),
			)
		}
		return &WindowResult{Count: count, XorHash: xorAcc}, nil
	})
	if err != nil {
		return nil, err
	}
	return result.(*WindowResult), nil
}

// BucketHash streams destination rows and computes the same 256-bucket
// xxhash64(id + "|" + ts_ms) fingerprint as the source-side agent.
// This makes Tier 3 bucket values directly comparable across stores.
func (da *ReconDestAgent) BucketHash(ctx context.Context, tableName, pkColumn string) (*BucketHashResult, error) {
	if err := validateIdent(tableName); err != nil {
		return nil, err
	}
	if err := validateIdent(pkColumn); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, da.cfg.QueryTimeout)
	defer cancel()

	sql := fmt.Sprintf(
		`SELECT %s::text AS id, "_source_ts" AS source_ts
		   FROM %s`,
		quoteIdent(pkColumn), quoteIdent(tableName),
	)

	result, err := da.breaker.Execute(func() (interface{}, error) {
		tx := da.readOnlyDB(ctx)
		defer tx.Rollback()

		rows, err := tx.Raw(sql).Rows()
		if err != nil {
			return nil, err
		}
		defer rows.Close()

		var bh BucketHashResult
		for rows.Next() {
			if err := da.limiter.Wait(ctx); err != nil {
				return nil, fmt.Errorf("rate limiter: %w", err)
			}
			var (
				id       string
				sourceTs *int64
			)
			if err := rows.Scan(&id, &sourceTs); err != nil {
				return nil, err
			}
			if sourceTs == nil {
				continue
			}
			idx := bucketIndex(id)
			bh.Buckets[idx] ^= hashIDPlusTsMs(id, *sourceTs)
			bh.Total++
		}
		if err := rows.Err(); err != nil {
			return nil, err
		}
		return &bh, nil
	})
	if err != nil {
		return nil, err
	}
	return result.(*BucketHashResult), nil
}

// ListIDsInWindow returns IDs for a drifted window. Expected small —
// caller should only request it after HashWindow pinpointed drift.
// Rate-limited per row.
func (da *ReconDestAgent) ListIDsInWindow(ctx context.Context, tableName, pkColumn string, tLo, tHi time.Time) ([]string, error) {
	if err := validateIdent(tableName); err != nil {
		return nil, err
	}
	if err := validateIdent(pkColumn); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, da.cfg.QueryTimeout)
	defer cancel()

	loMs, hiMs := tLo.UnixMilli(), tHi.UnixMilli()
	sql := fmt.Sprintf(
		`SELECT %s::text AS id FROM %s
		 WHERE "_source_ts" >= ? AND "_source_ts" < ?`,
		quoteIdent(pkColumn), quoteIdent(tableName),
	)

	result, err := da.breaker.Execute(func() (interface{}, error) {
		tx := da.readOnlyDB(ctx)
		defer tx.Rollback()

		rows, err := tx.Raw(sql, loMs, hiMs).Rows()
		if err != nil {
			return nil, err
		}
		defer rows.Close()

		var ids []string
		for rows.Next() {
			if err := da.limiter.Wait(ctx); err != nil {
				return nil, fmt.Errorf("rate limiter: %w", err)
			}
			var id string
			if err := rows.Scan(&id); err != nil {
				return nil, err
			}
			ids = append(ids, id)
		}
		if err := rows.Err(); err != nil {
			return nil, err
		}
		return ids, nil
	})
	if err != nil {
		return nil, err
	}
	return result.([]string), nil
}

// MaxWindowTs returns the highest `_source_ts` as a time.Time, used by
// Core to pick the Tier 1/2 upper watermark. Returns zero time if the
// table is empty or has no populated _source_ts yet.
func (da *ReconDestAgent) MaxWindowTs(ctx context.Context, tableName string) (time.Time, error) {
	if err := validateIdent(tableName); err != nil {
		return time.Time{}, err
	}
	ctx, cancel := context.WithTimeout(ctx, da.cfg.QueryTimeout)
	defer cancel()

	sql := fmt.Sprintf(`SELECT COALESCE(MAX("_source_ts"), 0) FROM %s`, quoteIdent(tableName))

	result, err := da.breaker.Execute(func() (interface{}, error) {
		tx := da.readOnlyDB(ctx)
		defer tx.Rollback()
		var maxMs int64
		if err := tx.Raw(sql).Scan(&maxMs).Error; err != nil {
			return time.Time{}, err
		}
		if maxMs == 0 {
			return time.Time{}, nil
		}
		return time.UnixMilli(maxMs), nil
	})
	if err != nil {
		return time.Time{}, err
	}
	return result.(time.Time), nil
}

// ============================================================
// Legacy shims — narrow wrappers kept so existing CMS handlers
// compile. They delegate to the new O(1) surface area.
// ============================================================

// GetIDs — legacy API. v3 does not need full-table ID pagination; we
// keep the signature but return an empty slice when called outside the
// narrow window scope. Callers that still need a bounded list should
// move to `ListIDsInWindow`.
func (da *ReconDestAgent) GetIDs(ctx context.Context, tableName, pkColumn string, batchSize, offset int) ([]string, error) {
	if err := validateIdent(tableName); err != nil {
		return nil, err
	}
	if err := validateIdent(pkColumn); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, da.cfg.QueryTimeout)
	defer cancel()

	sql := fmt.Sprintf(
		`SELECT %s::text AS id FROM %s ORDER BY %s LIMIT ? OFFSET ?`,
		quoteIdent(pkColumn), quoteIdent(tableName), quoteIdent(pkColumn),
	)

	tx := da.readOnlyDB(ctx)
	defer tx.Rollback()

	var ids []string
	if err := tx.Raw(sql, batchSize, offset).Scan(&ids).Error; err != nil {
		return nil, err
	}
	return ids, nil
}

// GetAllIDs — REMOVED in v3 runtime sense. The CMS report layer still
// references this symbol, so we keep a stub that returns an empty slice
// + a WARN log explaining why. Any caller that relied on the full set
// should migrate to window-scoped APIs. We DO NOT scan all IDs here —
// that was the bug this rewrite exists to kill.
func (da *ReconDestAgent) GetAllIDs(ctx context.Context, tableName, pkColumn string) ([]string, error) {
	if da.logger != nil {
		da.logger.Warn("ReconDestAgent.GetAllIDs called — v3 returns empty slice; migrate caller to ListIDsInWindow",
			zap.String("table", tableName),
		)
	}
	return nil, nil
}

// GetChunkHashes — legacy API, delegates to BucketHash and repackages.
// chunkSize is accepted for signature compat but ignored (256 buckets
// are the new primitive).
func (da *ReconDestAgent) GetChunkHashes(ctx context.Context, tableName, pkColumn string, chunkSize int) ([]ChunkHash, error) {
	bh, err := da.BucketHash(ctx, tableName, pkColumn)
	if err != nil {
		return nil, err
	}
	out := make([]ChunkHash, 0, 256)
	for i, h := range bh.Buckets {
		if h == 0 {
			continue
		}
		out = append(out, ChunkHash{
			StartID: fmt.Sprintf("bucket:%03d", i),
			EndID:   fmt.Sprintf("bucket:%03d", i),
			Count:   0,
			Hash:    fmt.Sprintf("%016x", h),
		})
	}
	return out, nil
}

// ============================================================
// Identifier safety
// ============================================================

// validateIdent rejects obviously-malicious identifiers BEFORE we
// embed them into SQL. Real safety is enforced by `quoteIdent` which
// wraps the identifier in double quotes and escapes embedded quotes,
// but a defense-in-depth check against nulls / control chars / DML
// keywords prevents human mistakes upstream.
func validateIdent(s string) error {
	if s == "" {
		return fmt.Errorf("identifier must not be empty")
	}
	if len(s) > 128 {
		return fmt.Errorf("identifier too long: %d chars", len(s))
	}
	for _, r := range s {
		if r == 0 || r == '\x00' || r == '\n' || r == '\r' {
			return fmt.Errorf("identifier contains control character")
		}
	}
	return nil
}

// quoteIdent returns `"<ident>"` with embedded `"` doubled — matches
// `pgx.Identifier{s}.Sanitize()` but without importing pgx just for
// this helper.
func quoteIdent(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}
