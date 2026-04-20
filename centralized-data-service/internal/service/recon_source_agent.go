package service

import (
	"context"
	"crypto/md5"
	"encoding/binary"
	"fmt"
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
type WindowResult struct {
	Count   int64  `json:"count"`
	XorHash uint64 `json:"xor_hash"`
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

// CountInWindow counts documents with updated_at ∈ [tLo, tHi). Used by
// Tier 1 per-window comparison.
func (sa *ReconSourceAgent) CountInWindow(ctx context.Context, sourceURL, database, collection string, tLo, tHi time.Time) (int64, error) {
	ctx, cancel := context.WithTimeout(ctx, sa.cfg.QueryTimeout)
	defer cancel()

	client, err := sa.getClient(ctx, sourceURL)
	if err != nil {
		return 0, err
	}
	coll := sa.secondaryColl(client, database, collection)

	filter := bson.M{"updated_at": bson.M{"$gte": tLo, "$lt": tHi}}
	result, err := sa.getBreaker(sourceURL).Execute(func() (interface{}, error) {
		return coll.CountDocuments(ctx, filter)
	})
	if err != nil {
		return 0, err
	}
	return result.(int64), nil
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
func (sa *ReconSourceAgent) HashWindow(ctx context.Context, sourceURL, database, collection string, tLo, tHi time.Time) (*WindowResult, error) {
	ctx, cancel := context.WithTimeout(ctx, sa.cfg.QueryTimeout)
	defer cancel()

	client, err := sa.getClient(ctx, sourceURL)
	if err != nil {
		return nil, err
	}
	coll := sa.secondaryColl(client, database, collection)

	filter := bson.M{"updated_at": bson.M{"$gte": tLo, "$lt": tHi}}
	opts := sa.selectOpts(bson.M{"_id": 1, "updated_at": 1})

	result, err := sa.getBreaker(sourceURL).Execute(func() (interface{}, error) {
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
			var doc struct {
				ID        interface{} `bson:"_id"`
				UpdatedAt time.Time   `bson:"updated_at"`
			}
			if err := cursor.Decode(&doc); err != nil {
				return nil, fmt.Errorf("decode: %w", err)
			}
			idStr := extractMongoID(doc.ID)
			// Identical byte layout to ReconDestAgent.HashWindow. Ms
			// epoch avoids the RFC3339 precision trap that caused the
			// old false-positive drift (fix 2026-04-17).
			xorAcc ^= hashIDPlusTsMs(idStr, doc.UpdatedAt.UnixMilli())
			count++
		}
		if err := cursor.Err(); err != nil {
			return nil, err
		}
		return &WindowResult{Count: count, XorHash: xorAcc}, nil
	})
	if err != nil {
		return nil, err
	}
	return result.(*WindowResult), nil
}

// BucketHash streams the entire collection once and distributes docs
// into 256 buckets keyed by the FIRST BYTE of xxhash(_id). Each bucket
// accumulates XOR of hash(id + updated_at_rfc3339) so insert / delete /
// update on any doc flips exactly 1 bucket value. Caller can diff
// buckets side-by-side to pinpoint the mismatched partition.
//
// Cost: O(N) read with rate limiter → ~200s for 1M @ 5K/s. Result is
// [256]uint64 = 2 KiB fixed memory, independent of N.
func (sa *ReconSourceAgent) BucketHash(ctx context.Context, sourceURL, database, collection string) (*BucketHashResult, error) {
	ctx, cancel := context.WithTimeout(ctx, sa.cfg.QueryTimeout)
	defer cancel()

	client, err := sa.getClient(ctx, sourceURL)
	if err != nil {
		return nil, err
	}
	coll := sa.secondaryColl(client, database, collection)

	opts := sa.selectOpts(bson.M{"_id": 1, "updated_at": 1})
	result, err := sa.getBreaker(sourceURL).Execute(func() (interface{}, error) {
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
			var doc struct {
				ID        interface{} `bson:"_id"`
				UpdatedAt time.Time   `bson:"updated_at"`
			}
			if err := cursor.Decode(&doc); err != nil {
				return nil, fmt.Errorf("decode: %w", err)
			}
			idStr := extractMongoID(doc.ID)
			bucket := bucketIndex(idStr)
			bh.Buckets[bucket] ^= hashIDPlusTs(idStr, doc.UpdatedAt)
			bh.Total++
		}
		if err := cursor.Err(); err != nil {
			return nil, err
		}
		return &bh, nil
	})
	if err != nil {
		return nil, err
	}
	return result.(*BucketHashResult), nil
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
func (sa *ReconSourceAgent) ListIDsInWindow(ctx context.Context, sourceURL, database, collection string, tLo, tHi time.Time) ([]string, error) {
	ctx, cancel := context.WithTimeout(ctx, sa.cfg.QueryTimeout)
	defer cancel()

	client, err := sa.getClient(ctx, sourceURL)
	if err != nil {
		return nil, err
	}
	coll := sa.secondaryColl(client, database, collection)

	filter := bson.M{"updated_at": bson.M{"$gte": tLo, "$lt": tHi}}
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
func (sa *ReconSourceAgent) MaxWindowTs(ctx context.Context, sourceURL, database, collection string) (time.Time, error) {
	ctx, cancel := context.WithTimeout(ctx, sa.cfg.QueryTimeout)
	defer cancel()

	client, err := sa.getClient(ctx, sourceURL)
	if err != nil {
		return time.Time{}, err
	}
	coll := sa.secondaryColl(client, database, collection)

	opts := options.FindOne().
		SetProjection(bson.M{"updated_at": 1}).
		SetSort(bson.M{"updated_at": -1})

	result, err := sa.getBreaker(sourceURL).Execute(func() (interface{}, error) {
		var doc struct {
			UpdatedAt time.Time `bson:"updated_at"`
		}
		err := coll.FindOne(ctx, bson.M{}, opts).Decode(&doc)
		if err == mongo.ErrNoDocuments {
			return time.Time{}, nil
		}
		if err != nil {
			return time.Time{}, err
		}
		return doc.UpdatedAt, nil
	})
	if err != nil {
		return time.Time{}, err
	}
	return result.(time.Time), nil
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
	bh, err := sa.BucketHash(ctx, sourceURL, database, collection)
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
// Commutative over XOR across the collection — insert order independent.
//
// Used by BucketHash (256-bucket fingerprint over the entire collection)
// where only one side (source) produces the buckets and we compare
// presence+position over time rather than byte equality across stores.
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
