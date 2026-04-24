package service

import (
	"context"
	"crypto/md5"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"centralized-data-service/pkgs/mongodb"

	"github.com/cespare/xxhash/v2"
	"github.com/sony/gobreaker"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"go.uber.org/zap"
	"golang.org/x/time/rate"
)

// ChunkHash — kept for backward compatibility with CMS callers that
// still reference the legacy Merkle-style result shape. New v3 code
// paths use WindowResult / BucketHashResult directly.
type ChunkHash struct {
	StartID string `json:"start_id"`
	EndID   string `json:"end_id"`
	Count   int    `json:"count"`
	Hash    string `json:"hash"`
}

// WindowResult is the fixed 16-byte streaming reconciliation unit.
// 8 bytes count + 8 bytes XOR hash. No ID set retained.
//
// Migration 017 / ADR v4 §2.4: Err / ErrorCode are optional structured
// error fields so callers can distinguish "query failed" from "0 results"
// without an out-of-band error signal. Legacy callers that only read
// Count/XorHash continue to work (zero-value Err = success).
type WindowResult struct {
	Count     int64  `json:"count"`
	XorHash   uint64 `json:"xor_hash"`
	Err       error  `json:"-"`
	ErrorCode string `json:"error_code,omitempty"`
}

// Recon error codes — ADR v4 §2.3. Kept as const strings so callers can
// compare/switch without importing a separate enum package.
const (
	ErrCodeSrcTimeout      = "SRC_TIMEOUT"
	ErrCodeSrcConnection   = "SRC_CONNECTION"
	ErrCodeSrcFieldMissing = "SRC_FIELD_MISSING"
	ErrCodeSrcEmpty        = "SRC_EMPTY"
	ErrCodeDstTimeout      = "DST_TIMEOUT"
	ErrCodeDstMissingCol   = "DST_MISSING_COLUMN"
	ErrCodeCircuitOpen     = "CIRCUIT_OPEN"
	ErrCodeAuthError       = "AUTH_ERROR"
	ErrCodeUnknown         = "UNKNOWN"
)

// classifyMongoError maps a raw Mongo/driver error to a structured recon
// error code. Order matters: check most-specific patterns first (timeout,
// auth) before falling into the generic transient bucket.
//
// Returns "" when err is nil so callers can unconditionally stash the
// result on WindowResult.ErrorCode.
func classifyMongoError(err error) string {
	if err == nil {
		return ""
	}
	s := err.Error()
	lower := strings.ToLower(s)
	switch {
	case strings.Contains(lower, "circuit breaker is open") ||
		strings.Contains(lower, "breaker") && strings.Contains(lower, "open"):
		return ErrCodeCircuitOpen
	case strings.Contains(lower, "timeout") ||
		strings.Contains(lower, "deadline exceeded") ||
		strings.Contains(lower, "i/o timeout"):
		return ErrCodeSrcTimeout
	case strings.Contains(lower, "unauthorized") ||
		strings.Contains(lower, "authentication failed") ||
		strings.Contains(lower, "auth fail"):
		return ErrCodeAuthError
	case strings.Contains(lower, "no field") ||
		strings.Contains(lower, "field does not exist") ||
		strings.Contains(lower, "missing field"):
		return ErrCodeSrcFieldMissing
	case isMongoTransient(err):
		return ErrCodeSrcConnection
	default:
		var cmdErr mongo.CommandError
		if errors.As(err, &cmdErr) {
			switch cmdErr.Code {
			case 13, 18:
				return ErrCodeAuthError
			case 50:
				return ErrCodeSrcTimeout
			}
		}
		return ErrCodeUnknown
	}
}

// BucketHashResult is a fixed-size 256-bucket XOR fingerprint for
// whole-table drift detection (Tier 3). 256 * 8 bytes = 2 KiB total.
type BucketHashResult struct {
	Buckets [256]uint64 `json:"buckets"`
	Total   int64       `json:"total"`
}

// ReconSourceAgentConfig holds tunables for MongoDB-side recon reads.
// All fields optional — sensible defaults supplied when zero-valued.
type ReconSourceAgentConfig struct {
	MaxDocsPerSec    int           // rate limit, default 5000
	QueryTimeout     time.Duration // per-query ctx deadline, default 30s
	BatchSize        int32         // Mongo cursor batchSize, default 1000
	BreakerTimeout   time.Duration // open-circuit cool-off, default 60s
	BreakerThreshold uint32        // consecutive failures before open, default 5
}

func (c *ReconSourceAgentConfig) applyDefaults() {
	if c.MaxDocsPerSec <= 0 {
		c.MaxDocsPerSec = 5000
	}
	if c.QueryTimeout <= 0 {
		c.QueryTimeout = 30 * time.Second
	}
	if c.BatchSize <= 0 {
		c.BatchSize = 1000
	}
	if c.BreakerTimeout <= 0 {
		c.BreakerTimeout = 60 * time.Second
	}
	if c.BreakerThreshold == 0 {
		c.BreakerThreshold = 5
	}
}

// ReconSourceAgent connects to MongoDB for reconciliation.
//
// v3 semantics (plan_data_integrity_v3 §2-§5):
//   - Streaming only. NEVER load a full ID set. All previous
//     GetAllIDs / GetChunkHashes code paths were scale bombs for
//     50M+ record collections (1.2 GB network, unbounded RAM).
//   - XOR-hash aggregate per time window keeps RAM/network O(1)
//     relative to the window size — the raw IDs are hashed on
//     the fly and only the 64-bit XOR accumulator is retained.
//   - Rate-limited via token-bucket so Mongo secondary CPU never
//     spikes.
//   - Circuit-broken per source URL — a flapping secondary opens
//     the breaker for `BreakerTimeout` rather than cascading.
//   - Read preference = secondary so the primary stays cold.
type ReconSourceAgent struct {
	defaultClient *mongo.Client
	clients       map[string]*mongo.Client
	breakers      map[string]*gobreaker.CircuitBreaker
	mu            sync.RWMutex
	cfg           ReconSourceAgentConfig
	limiter       *rate.Limiter
	logger        *zap.Logger
}

// NewReconSourceAgent constructs an agent with default v3 tunables.
func NewReconSourceAgent(defaultClient *mongo.Client, logger *zap.Logger) *ReconSourceAgent {
	return NewReconSourceAgentWithConfig(defaultClient, ReconSourceAgentConfig{}, logger)
}

// NewReconSourceAgentWithConfig allows callers to override rate limit /
// timeout / breaker settings.
func NewReconSourceAgentWithConfig(defaultClient *mongo.Client, cfg ReconSourceAgentConfig, logger *zap.Logger) *ReconSourceAgent {
	cfg.applyDefaults()
	return &ReconSourceAgent{
		defaultClient: defaultClient,
		clients:       make(map[string]*mongo.Client),
		breakers:      make(map[string]*gobreaker.CircuitBreaker),
		cfg:           cfg,
		limiter:       rate.NewLimiter(rate.Limit(cfg.MaxDocsPerSec), cfg.MaxDocsPerSec),
		logger:        logger,
	}
}

// getClient returns (or lazily creates) a Mongo client for a given
// source URL. Empty URL → default client.
func (sa *ReconSourceAgent) getClient(ctx context.Context, sourceURL string) (*mongo.Client, error) {
	if sourceURL == "" {
		return sa.defaultClient, nil
	}

	sa.mu.RLock()
	if c, ok := sa.clients[sourceURL]; ok {
		sa.mu.RUnlock()
		return c, nil
	}
	sa.mu.RUnlock()

	sa.mu.Lock()
	defer sa.mu.Unlock()
	if c, ok := sa.clients[sourceURL]; ok {
		return c, nil
	}

	client, err := mongodb.NewClient(ctx, mongodb.MongoConfig{URL: sourceURL}, sa.logger)
	if err != nil {
		return nil, fmt.Errorf("connect to source %s: %w", redactURL(sourceURL), err)
	}
	sa.clients[sourceURL] = client
	sa.logger.Info("new MongoDB source connected (recon)", zap.String("url", redactURL(sourceURL)))
	return client, nil
}

// getBreaker returns (or creates) a circuit breaker keyed by source URL.
func (sa *ReconSourceAgent) getBreaker(sourceURL string) *gobreaker.CircuitBreaker {
	key := sourceURL
	if key == "" {
		key = "_default"
	}
	sa.mu.RLock()
	if b, ok := sa.breakers[key]; ok {
		sa.mu.RUnlock()
		return b
	}
	sa.mu.RUnlock()

	sa.mu.Lock()
	defer sa.mu.Unlock()
	if b, ok := sa.breakers[key]; ok {
		return b
	}
	b := gobreaker.NewCircuitBreaker(gobreaker.Settings{
		Name:    "recon-source-" + redactURL(key),
		Timeout: sa.cfg.BreakerTimeout,
		ReadyToTrip: func(counts gobreaker.Counts) bool {
			return counts.ConsecutiveFailures >= sa.cfg.BreakerThreshold
		},
		OnStateChange: func(name string, from, to gobreaker.State) {
			sa.logger.Warn("recon source breaker state change",
				zap.String("name", name),
				zap.String("from", from.String()),
				zap.String("to", to.String()),
			)
		},
	})
	sa.breakers[key] = b
	return b
}

// selectOpts returns a FindOptions pre-wired with projection + batchSize.
// Read preference is applied at the Collection level via secondaryColl()
// — the Mongo driver v1.17 does not accept read-preference per Find().
func (sa *ReconSourceAgent) selectOpts(projection bson.M) *options.FindOptions {
	return options.Find().
		SetProjection(projection).
		SetBatchSize(sa.cfg.BatchSize)
}

// secondaryColl returns a Collection handle whose read preference is
// forced to secondary. The underlying connection pool is shared with
// the primary-reading handles so no extra network cost.
func (sa *ReconSourceAgent) secondaryColl(client *mongo.Client, database, collection string) *mongo.Collection {
	return client.Database(database).Collection(
		collection,
		options.Collection().SetReadPreference(readpref.Secondary()),
	)
}

// resolveTimestampField validates a Mongo timestamp field name and
// returns a safe default when the registry value is empty.
//
// Why a whitelist: the field name is interpolated into a BSON filter
// via bson.M which IS safe against injection (no string interpolation),
// BUT an operator accidentally passing `$where` or a dotted path could
// rewrite the query semantics. We accept only [A-Za-z_][A-Za-z0-9_]*
// up to 64 chars — the superset of every real Mongo field name we've
// seen in audit (`updated_at`, `updatedAt`, `createdAt`, `lastUpdatedAt`,
// `ts`, `_id`). Anything else falls back to the default.
func resolveTimestampField(tsField string) string {
	const def = "updated_at"
	if tsField == "" {
		return def
	}
	if len(tsField) > 64 {
		return def
	}
	for i, r := range tsField {
		if r == '_' || (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') {
			continue
		}
		if i > 0 && r >= '0' && r <= '9' {
			continue
		}
		return def
	}
	return tsField
}

// CountDocuments — Tier 1 legacy helper. Verifies the collection exists
// before counting (Mongo silently returns 0 on missing coll which would
// mask drift). Uses secondary read.
func (sa *ReconSourceAgent) CountDocuments(ctx context.Context, sourceURL, database, collection string) (int64, error) {
	ctx, cancel := context.WithTimeout(ctx, sa.cfg.QueryTimeout)
	defer cancel()

	client, err := sa.getClient(ctx, sourceURL)
	if err != nil {
		return 0, err
	}

	db := client.Database(database)
	names, err := db.ListCollectionNames(ctx, bson.M{"name": collection})
	if err != nil {
		return 0, fmt.Errorf("list collections failed on %s: %w", database, err)
	}
	if len(names) == 0 {
		return 0, fmt.Errorf("source collection not found: %s.%s", database, collection)
	}

	coll := sa.secondaryColl(client, database, collection)
	result, err := sa.getBreaker(sourceURL).Execute(func() (interface{}, error) {
		return coll.CountDocuments(ctx, bson.M{})
	})
	if err != nil {
		return 0, err
	}
	return result.(int64), nil
}

// CountInWindow counts documents with <timestampField> ∈ [tLo, tHi). Used by
// Tier 1 per-window comparison. `timestampField` may be empty — in that case
// the default `updated_at` is used. Added 2026-04-20 (Bug B) to support
// collections that use `lastUpdatedAt` / `createdAt` / other non-snake-case
// conventions. The field name is whitelist-validated in resolveTimestampField
// so a bad value falls back to default rather than producing an injection
// vector.
func (sa *ReconSourceAgent) CountInWindow(ctx context.Context, sourceURL, database, collection, timestampField string, tLo, tHi time.Time) (int64, error) {
	ctx, cancel := context.WithTimeout(ctx, sa.cfg.QueryTimeout)
	defer cancel()

	client, err := sa.getClient(ctx, sourceURL)
	if err != nil {
		return 0, err
	}
	coll := sa.secondaryColl(client, database, collection)

	tsField := resolveTimestampField(timestampField)
	filter := bson.M{tsField: bson.M{"$gte": tLo, "$lt": tHi}}
	var out int64
	err = sa.queryWithRetry(ctx, "CountInWindow", func() error {
		result, innerErr := sa.getBreaker(sourceURL).Execute(func() (interface{}, error) {
			return coll.CountDocuments(ctx, filter)
		})
		if innerErr != nil {
			return innerErr
		}
		out = result.(int64)
		return nil
	})
	if err != nil {
		return 0, err
	}
	return out, nil
}

// CountInWindowWithFallback first queries with `primaryField`. When the
// primary returns 0 AND no error, it probes each fallback candidate. The
// first non-zero result wins. Falls back to the primary result (0) when
// no candidate produces data — callers then decide whether to treat that
// as drift or as a genuinely-empty window.
//
// Security: `fallbackFields` entries are validated through
// resolveTimestampField (same whitelist as the primary) so a malicious
// registry JSONB cannot reshape the query.
//
// Cross-cutting: used by recon_core Tier 1 via CountInWindow — the
// fallback path is engaged only at runtime when primary=0, keeping the
// happy-path cost identical to the old single-field query.
func (sa *ReconSourceAgent) CountInWindowWithFallback(
	ctx context.Context,
	sourceURL, database, collection, primaryField string,
	tLo, tHi time.Time,
	fallbackFields []string,
) (count int64, fieldUsed string, err error) {
	primary := resolveTimestampField(primaryField)
	count, err = sa.CountInWindow(ctx, sourceURL, database, collection, primary, tLo, tHi)
	if err != nil || count > 0 {
		return count, primary, err
	}
	// Primary returned 0 with no error. Try fallbacks in order, stop on
	// the first candidate that yields data. We do NOT short-circuit on
	// the first fallback error — transient Mongo blips on one candidate
	// should not block the detector from checking the rest.
	for _, cand := range fallbackFields {
		if cand == "" {
			continue
		}
		if !candidateNameRE.MatchString(cand) {
			continue
		}
		if cand == primary {
			continue
		}
		c, e := sa.CountInWindow(ctx, sourceURL, database, collection, cand, tLo, tHi)
		if e != nil {
			continue
		}
		if c > 0 {
			if sa.logger != nil {
				sa.logger.Warn("primary timestamp field returned 0, fallback used",
					zap.String("collection", collection),
					zap.String("primary", primary),
					zap.String("fallback", cand),
					zap.Int64("count", c),
				)
			}
			return c, cand, nil
		}
	}
	return count, primary, nil
}

// HashWindow streams docs whose updated_at ∈ [tLo, tHi) and builds a
// 16-byte fingerprint = (count, XOR of per-doc xxhash). The cursor is
// read in 1000-doc batches, per-doc the limiter waits so we never
// exceed MaxDocsPerSec.
//
// Hash input per doc: `_id + "|" + UnixMilli(updated_at)`. The
// destination-side `HashWindow` uses the SAME byte layout (ms-epoch
// integer text) so XOR accumulators compare byte-for-byte when the two
// stores hold identical data — see the root-cause fix documented in
// `03_implementation_v3_heal_root_cause_fix.md` (2026-04-17).
//
// Memory: O(1) — count + xor accumulator + single decoded doc.
// Network: projection `{_id, updated_at}` ≈ 30 bytes/doc → 30 MB / 1M docs.
// Result: 16 bytes. Safe to ship over NATS or persist.
func (sa *ReconSourceAgent) HashWindow(ctx context.Context, sourceURL, database, collection, timestampField string, tLo, tHi time.Time) (*WindowResult, error) {
	ctx, cancel := context.WithTimeout(ctx, sa.cfg.QueryTimeout)
	defer cancel()

	client, err := sa.getClient(ctx, sourceURL)
	if err != nil {
		return nil, err
	}
	coll := sa.secondaryColl(client, database, collection)

	tsField := resolveTimestampField(timestampField)
	filter := bson.M{tsField: bson.M{"$gte": tLo, "$lt": tHi}}
	// Use BSON aliasing so the decoded doc always exposes UpdatedAt
	// regardless of whether the underlying field is `updated_at`,
	// `updatedAt`, `createdAt`, etc. We add a projection of the
	// actual ts field so Mongo only returns what we need.
	opts := sa.selectOpts(bson.M{"_id": 1, tsField: 1})

	var out *WindowResult
	err = sa.queryWithRetry(ctx, "HashWindow", func() error {
		result, innerErr := sa.getBreaker(sourceURL).Execute(func() (interface{}, error) {
			cursor, err := coll.Find(ctx, filter, opts)
			if err != nil {
				return nil, err
			}
			defer cursor.Close(ctx)

			var (
				xorAcc uint64
				count  int64
			)
			for cursor.Next(ctx) {
				// Rate-limit per document read. Unbypassable: ctx cancellation
				// is the only way out, and even that surfaces an error rather
				// than returning partial results (critical for hash correctness).
				if err := sa.limiter.Wait(ctx); err != nil {
					return nil, fmt.Errorf("rate limiter: %w", err)
				}
				var raw bson.M
				if err := cursor.Decode(&raw); err != nil {
					return nil, fmt.Errorf("decode: %w", err)
				}
				idStr := extractMongoID(raw["_id"])
				ts := extractTimestampMs(raw, tsField, idStr)
				// Identical byte layout to ReconDestAgent.HashWindow. Ms
				// epoch avoids the RFC3339 precision trap that caused the
				// old false-positive drift (fix 2026-04-17).
				xorAcc ^= hashIDPlusTsMs(idStr, ts)
				count++
			}
			if err := cursor.Err(); err != nil {
				return nil, err
			}
			return &WindowResult{Count: count, XorHash: xorAcc}, nil
		})
		if innerErr != nil {
			return innerErr
		}
		out = result.(*WindowResult)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// BucketHash streams the entire collection once and distributes docs
// into 256 buckets keyed by the FIRST BYTE of xxhash(_id). Each bucket
// accumulates XOR of xxhash64(id + "|" + ts_ms) so source and
// destination fingerprints are directly comparable bucket-by-bucket.
//
// Cost: O(N) read with rate limiter → ~200s for 1M @ 5K/s. Result is
// [256]uint64 = 2 KiB fixed memory, independent of N.
func (sa *ReconSourceAgent) BucketHash(ctx context.Context, sourceURL, database, collection, timestampField string) (*BucketHashResult, error) {
	ctx, cancel := context.WithTimeout(ctx, sa.cfg.QueryTimeout)
	defer cancel()

	client, err := sa.getClient(ctx, sourceURL)
	if err != nil {
		return nil, err
	}
	coll := sa.secondaryColl(client, database, collection)

	tsField := resolveTimestampField(timestampField)
	opts := sa.selectOpts(bson.M{"_id": 1, tsField: 1})
	var out *BucketHashResult
	err = sa.queryWithRetry(ctx, "BucketHash", func() error {
		result, innerErr := sa.getBreaker(sourceURL).Execute(func() (interface{}, error) {
			cursor, err := coll.Find(ctx, bson.M{}, opts)
			if err != nil {
				return nil, err
			}
			defer cursor.Close(ctx)

			var bh BucketHashResult
			for cursor.Next(ctx) {
				if err := sa.limiter.Wait(ctx); err != nil {
					return nil, fmt.Errorf("rate limiter: %w", err)
				}
				var raw bson.M
				if err := cursor.Decode(&raw); err != nil {
					return nil, fmt.Errorf("decode: %w", err)
				}
				idStr := extractMongoID(raw["_id"])
				ts := extractTimestampMs(raw, tsField, idStr)
				if ts == 0 {
					continue
				}
				bucket := bucketIndex(idStr)
				bh.Buckets[bucket] ^= hashIDPlusTsMs(idStr, ts)
				bh.Total++
			}
			if err := cursor.Err(); err != nil {
				return nil, err
			}
			return &bh, nil
		})
		if innerErr != nil {
			return innerErr
		}
		out = result.(*BucketHashResult)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// ListIDsInWindow returns the concrete IDs inside a drifted window.
// Expected to be called ONLY after HashWindow has flagged the window
// as mismatched — at that point the window is small (windows are
// 15-min slices, typically a few thousand docs). No pagination is
// needed; if the caller suspects a huge window they should narrow it
// first.
//
// Still rate-limited per document — if a caller hands us a mega-window
// (millions of docs) we bound the Mongo load rather than crash.
func (sa *ReconSourceAgent) ListIDsInWindow(ctx context.Context, sourceURL, database, collection, timestampField string, tLo, tHi time.Time) ([]string, error) {
	ctx, cancel := context.WithTimeout(ctx, sa.cfg.QueryTimeout)
	defer cancel()

	client, err := sa.getClient(ctx, sourceURL)
	if err != nil {
		return nil, err
	}
	coll := sa.secondaryColl(client, database, collection)

	tsField := resolveTimestampField(timestampField)
	filter := bson.M{tsField: bson.M{"$gte": tLo, "$lt": tHi}}
	opts := sa.selectOpts(bson.M{"_id": 1})

	result, err := sa.getBreaker(sourceURL).Execute(func() (interface{}, error) {
		cursor, err := coll.Find(ctx, filter, opts)
		if err != nil {
			return nil, err
		}
		defer cursor.Close(ctx)

		var ids []string
		for cursor.Next(ctx) {
			if err := sa.limiter.Wait(ctx); err != nil {
				return nil, fmt.Errorf("rate limiter: %w", err)
			}
			var doc struct {
				ID interface{} `bson:"_id"`
			}
			if err := cursor.Decode(&doc); err != nil {
				return nil, fmt.Errorf("decode: %w", err)
			}
			ids = append(ids, extractMongoID(doc.ID))
		}
		if err := cursor.Err(); err != nil {
			return nil, err
		}
		return ids, nil
	})
	if err != nil {
		return nil, err
	}
	return result.([]string), nil
}

// MaxWindowTs returns the highest updated_at on the source, used by the
// Core to pick the upper watermark for Tier 1 / Tier 2 window scan.
// Returns zero time if the collection is empty.
func (sa *ReconSourceAgent) MaxWindowTs(ctx context.Context, sourceURL, database, collection, timestampField string) (time.Time, error) {
	ctx, cancel := context.WithTimeout(ctx, sa.cfg.QueryTimeout)
	defer cancel()

	client, err := sa.getClient(ctx, sourceURL)
	if err != nil {
		return time.Time{}, err
	}
	coll := sa.secondaryColl(client, database, collection)

	tsField := resolveTimestampField(timestampField)
	opts := options.FindOne().
		SetProjection(bson.M{tsField: 1}).
		SetSort(bson.M{tsField: -1})

	var out time.Time
	err = sa.queryWithRetry(ctx, "MaxWindowTs", func() error {
		result, innerErr := sa.getBreaker(sourceURL).Execute(func() (interface{}, error) {
			var raw bson.M
			err := coll.FindOne(ctx, bson.M{}, opts).Decode(&raw)
			if err == mongo.ErrNoDocuments {
				return time.Time{}, nil
			}
			if err != nil {
				return time.Time{}, err
			}
			// extractTimestampMs returns 0 if field is absent; treat that
			// as "collection empty / has no ts field" — caller falls back
			// to dst.max or now-freezeMargin.
			idStr := extractMongoID(raw["_id"])
			ms := extractTimestampMs(raw, tsField, idStr)
			if ms == 0 {
				return time.Time{}, nil
			}
			return time.UnixMilli(ms), nil
		})
		if innerErr != nil {
			return innerErr
		}
		out = result.(time.Time)
		return nil
	})
	if err != nil {
		return time.Time{}, err
	}
	return out, nil
}

// ============================================================
// Legacy API shims — kept thin to not break the existing CMS
// report/history endpoints during the phased rollout. The CMS
// view layer presents reports that may predate v3 so we still
// return the old ChunkHash shape but compute it on-the-fly
// from BucketHash output rather than reloading full ID sets.
// ============================================================

// GetChunkHashes — legacy API. Under v3 we delegate to BucketHash and
// surface the 256 buckets as ChunkHash entries. chunkSize is accepted
// for API compat but ignored; bucketing is fixed at 256.
func (sa *ReconSourceAgent) GetChunkHashes(ctx context.Context, sourceURL, database, collection string, chunkSize int) ([]ChunkHash, error) {
	// Legacy callers didn't pass a timestamp field. Default "" resolves to
	// "updated_at" inside BucketHash via resolveTimestampField — preserves
	// prior behaviour for legacy Merkle-style callers.
	bh, err := sa.BucketHash(ctx, sourceURL, database, collection, "")
	if err != nil {
		return nil, err
	}
	out := make([]ChunkHash, 0, 256)
	for i, h := range bh.Buckets {
		if h == 0 {
			continue // empty bucket — skip to keep the payload compact
		}
		out = append(out, ChunkHash{
			StartID: fmt.Sprintf("bucket:%03d", i),
			EndID:   fmt.Sprintf("bucket:%03d", i),
			Count:   0, // per-bucket count not retained in v3 O(1) mode
			Hash:    fmt.Sprintf("%016x", h),
		})
	}
	return out, nil
}

// hashIDPlusTs computes xxhash of (idString || "|" || rfc3339nano(ts)).
// Kept for backward compatibility with older tests/helpers; cross-store
// reconciliation now standardizes on hashIDPlusTsMs.
func hashIDPlusTs(idStr string, ts time.Time) uint64 {
	var b strings.Builder
	b.Grow(len(idStr) + 1 + 32)
	b.WriteString(idStr)
	b.WriteByte('|')
	b.WriteString(ts.UTC().Format(time.RFC3339Nano))
	return xxhash.Sum64String(b.String())
}

// hashIDPlusTsMs is the cross-store HashWindow primitive: the input
// bytes are `id + "|" + formatInt(tsMs)` where tsMs is the ms-precision
// epoch shared by both source and destination. Used by
// ReconSourceAgent.HashWindow and ReconDestAgent.HashWindow so the XOR
// accumulators match on byte-for-byte equality when the two stores hold
// the same data.
//
// Why not RFC3339Nano: Postgres stores `_source_ts` as BIGINT ms from
// Debezium ts_ms. Round-tripping through RFC3339 would force a format
// step with tz/precision trap (Postgres timezone vs Mongo BSON datetime
// rounding). Staying in integer ms on both sides keeps the hash input
// byte-exact.
func hashIDPlusTsMs(idStr string, tsMs int64) uint64 {
	// strconv.AppendInt avoids an intermediate string allocation.
	var buf [32]byte
	num := strconv.AppendInt(buf[:0], tsMs, 10)
	var b strings.Builder
	b.Grow(len(idStr) + 1 + len(num))
	b.WriteString(idStr)
	b.WriteByte('|')
	b.Write(num)
	return xxhash.Sum64String(b.String())
}

// bucketIndex returns the first byte of xxhash(idStr) as bucket [0..255].
func bucketIndex(idStr string) uint8 {
	h := xxhash.Sum64String(idStr)
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], h)
	return buf[0]
}

// extractMongoID converts a MongoDB _id value to its stable string form.
func extractMongoID(v interface{}) string {
	if oid, ok := v.(primitive.ObjectID); ok {
		return oid.Hex()
	}
	return fmt.Sprintf("%v", v)
}

// extractTimestampMs reads the timestamp in ms from a decoded BSON document.
// Priority:
//  1. Try the configured field (tsField) — handles updated_at, updatedAt,
//     createdAt, lastUpdatedAt, etc. Supports primitive.DateTime, time.Time,
//     int32/int64 (ms epoch), and float64.
//  2. Fallback to ObjectID timestamp when the field is missing or zero —
//     preserves correctness for collections with inconsistent schema where
//     some documents lack the timestamp field. The fallback is explicit so
//     hash inputs remain deterministic across source/dest comparisons.
//
// Returns UnixMilli. Zero return means unresolvable — caller MUST treat
// as "no timestamp data" (skip from hash/filter).
func extractTimestampMs(raw bson.M, tsField, idHex string) int64 {
	if v, ok := raw[tsField]; ok && v != nil {
		switch t := v.(type) {
		case primitive.DateTime:
			return int64(t)
		case time.Time:
			if !t.IsZero() {
				return t.UnixMilli()
			}
		case int64:
			return t
		case int32:
			return int64(t)
		case float64:
			return int64(t)
		}
	}
	// Fallback: ObjectID timestamp. Every Mongo doc has _id; when _id is
	// an ObjectID the top 4 bytes encode the insertion unix-sec. Gives a
	// sensible approximate timestamp for collections that don't track
	// updated_at (e.g. export-jobs created-once, never mutated).
	if oid, err := primitive.ObjectIDFromHex(idHex); err == nil {
		return oid.Timestamp().UnixMilli()
	}
	return 0
}

// redactURL drops the userinfo/hostport from a Mongo URL so we don't
// leak credentials into logs or breaker names.
func redactURL(u string) string {
	if u == "" {
		return ""
	}
	// keep only scheme://<redacted>
	for _, sep := range []string{"://"} {
		if idx := strings.Index(u, sep); idx > 0 {
			return u[:idx+len(sep)] + "<redacted>"
		}
	}
	return "<redacted>"
}

// queryWithRetry wraps a Mongo-facing call in a small retry loop. Only
// errors that look like transient network blips (incomplete reads, EOF,
// connection resets) or specific driver CommandError codes (6, 7, 89, 91,
// 189 — host/network/timeout/shutdown) are retried. All other errors fail
// fast so the breaker still trips on genuinely bad sources.
//
// Max 3 attempts, linear backoff (0, 500ms, 1s). The caller's context
// bounds total wait time.
func (sa *ReconSourceAgent) queryWithRetry(ctx context.Context, op string, fn func() error) error {
	const maxAttempts = 3
	var lastErr error
	for attempt := 0; attempt < maxAttempts; attempt++ {
		if attempt > 0 {
			backoff := time.Duration(attempt) * 500 * time.Millisecond
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(backoff):
			}
			if sa.logger != nil {
				sa.logger.Warn("recon mongo transient error, retrying",
					zap.String("op", op),
					zap.Int("attempt", attempt+1),
					zap.Error(lastErr),
				)
			}
		}
		lastErr = fn()
		if lastErr == nil {
			return nil
		}
		if !isMongoTransient(lastErr) {
			return lastErr
		}
	}
	return fmt.Errorf("%s: after %d attempts: %w", op, maxAttempts, lastErr)
}

// isMongoTransient returns true if the error looks like a network blip or
// a transient driver-level failure that warrants retry. False for logical
// errors (bad filter, missing collection) so retry never masks real bugs.
func isMongoTransient(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF) {
		return true
	}
	s := err.Error()
	if strings.Contains(s, "incomplete read of message header") ||
		strings.Contains(s, "incomplete read") ||
		strings.Contains(s, "connection reset") ||
		strings.Contains(s, "connection refused") ||
		strings.Contains(s, "unexpected EOF") ||
		strings.Contains(s, "broken pipe") ||
		strings.Contains(s, "i/o timeout") {
		return true
	}
	var cmdErr mongo.CommandError
	if errors.As(err, &cmdErr) {
		switch cmdErr.Code {
		case 6, 7, 89, 91, 189, 262, 318, 9001:
			// 6 HostUnreachable, 7 HostNotFound, 89 NetworkTimeout,
			// 91 ShutdownInProgress, 189 PrimarySteppedDown,
			// 262 ExceededTimeLimit, 318 NoProgressMade, 9001 SocketException.
			return true
		}
	}
	// Driver-specific network error markers not all exposed via errors.Is.
	if strings.Contains(s, "server selection error") ||
		strings.Contains(s, "no reachable servers") {
		return true
	}
	return false
}

// buildLegacyChunkHash — unused now, kept for tests / callers that
// build an MD5 of a sorted ID slice. Not used by v3 agent; retained
// so recon_dest_agent.go (which imported MD5 helpers) compiles cleanly.
func buildLegacyChunkHash(ids []string) ChunkHash {
	sort.Strings(ids)
	concat := strings.Join(ids, "|")
	hash := fmt.Sprintf("%x", md5.Sum([]byte(concat)))
	return ChunkHash{
		StartID: ids[0],
		EndID:   ids[len(ids)-1],
		Count:   len(ids),
		Hash:    hash,
	}
}
