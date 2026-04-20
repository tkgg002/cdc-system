package service

import (
	"context"
	"fmt"
	"regexp"
	"sort"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"go.uber.org/zap"
)

// TimestampDetector probes a Mongo collection to pick the best timestamp
// field for reconciliation window queries.
//
// Why it exists: scale 200+ collections means per-table manual
// configuration of `cdc_table_registry.timestamp_field` is O(N) human
// error. A detector samples N docs, counts per-candidate field presence,
// and ranks by coverage — same cost for any future collection.
//
// Algorithm:
//  1. Read up to `sampleSize` documents (default 100) from the secondary
//     replica. No filter — we want unconditional coverage.
//  2. For each candidate name, count presence. Non-null, non-empty values
//     only (an explicit `null` in the doc still counts as missing because
//     a null timestamp cannot serve $gte/$lt filters).
//  3. Rank by coverage DESC, break ties by candidate-chain order so that
//     `updated_at` wins over `createdAt` when both are present — preserves
//     the semantic "recency = updated" used everywhere else in recon.
//  4. Confidence bands: high (>=80%), medium (30-80%), low (<30%) or no
//     field at all → FallbackToID=true and field="_id".
type TimestampDetector struct {
	mongoClient *mongo.Client
	logger      *zap.Logger
}

// DetectionResult is the ranked outcome of a single DetectForCollection
// pass. Callers should persist Field + Confidence into registry; the
// Candidates map is kept for operator audit ("why did we pick X?").
type DetectionResult struct {
	Field        string         `json:"field"`
	Coverage     float64        `json:"coverage"` // 0.0 - 1.0
	Candidates   map[string]int `json:"candidates"`
	SampleSize   int            `json:"sample_size"`
	FallbackToID bool           `json:"fallback_to_id"`
	Confidence   string         `json:"confidence"` // high | medium | low
}

// defaultCandidateChain is the ordered fallback when registry has none.
// Order matters: we prefer `updated_at` over alternatives when coverage
// is tied because business semantics say "filter by last-touched time".
var defaultCandidateChain = []string{"updated_at", "updatedAt", "created_at", "createdAt"}

// candidateNameRE whitelists Mongo field names. Enforces the same
// contract as resolveTimestampField in recon_source_agent.go so a
// malicious JSONB entry in registry can't sneak through as a filter.
var candidateNameRE = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]{0,63}$`)

// NewTimestampDetector wires the detector to a Mongo client + logger.
// The client may be the shared worker client — the detector only reads
// and applies secondary read preference on each collection handle it
// opens so it never competes with oplog tailers on the primary.
func NewTimestampDetector(mc *mongo.Client, logger *zap.Logger) *TimestampDetector {
	return &TimestampDetector{mongoClient: mc, logger: logger}
}

// DetectForCollection samples up to `sampleSize` docs from
// `sourceDB.sourceColl` and returns the best-ranked timestamp field
// plus confidence metadata.
//
// Parameters:
//   - candidates: ordered candidate chain. nil/empty → defaultCandidateChain.
//   - sampleSize: 0 → 100 (good balance between detection accuracy and
//     Mongo read cost; tuneable by caller when scanning very large or very
//     small collections).
//
// Security: candidates are filtered through `candidateNameRE` before any
// filter operation. Invalid names are logged and dropped rather than
// failing the whole call (registry may carry legacy entries).
func (td *TimestampDetector) DetectForCollection(
	ctx context.Context,
	sourceDB, sourceColl string,
	candidates []string,
	sampleSize int,
) (*DetectionResult, error) {
	if td.mongoClient == nil {
		return nil, fmt.Errorf("timestamp detector: mongo client not configured")
	}
	if sampleSize <= 0 {
		sampleSize = 100
	}
	// Normalize + whitelist candidates. Dedup while preserving order so
	// ties fall through in the same priority the caller intended.
	seen := map[string]struct{}{}
	filtered := make([]string, 0, len(candidates))
	for _, c := range candidates {
		if c == "" {
			continue
		}
		if !candidateNameRE.MatchString(c) {
			if td.logger != nil {
				td.logger.Warn("timestamp detector: dropping invalid candidate name (security)",
					zap.String("db", sourceDB),
					zap.String("collection", sourceColl),
					zap.String("candidate", c),
				)
			}
			continue
		}
		if _, ok := seen[c]; ok {
			continue
		}
		seen[c] = struct{}{}
		filtered = append(filtered, c)
	}
	if len(filtered) == 0 {
		filtered = append(filtered, defaultCandidateChain...)
	}

	coll := td.mongoClient.
		Database(sourceDB).
		Collection(sourceColl, options.Collection().SetReadPreference(readpref.Secondary()))

	// Only project the candidate fields — avoids pulling heavy payloads
	// (documents can be 10s of KB each). Projection is safe because we
	// already whitelisted the names.
	projection := bson.M{"_id": 1}
	for _, c := range filtered {
		projection[c] = 1
	}

	findOpts := options.Find().
		SetProjection(projection).
		SetLimit(int64(sampleSize)).
		SetBatchSize(int32(sampleSize))

	cursor, err := coll.Find(ctx, bson.M{}, findOpts)
	if err != nil {
		return nil, fmt.Errorf("detect: find %s.%s: %w", sourceDB, sourceColl, err)
	}
	defer cursor.Close(ctx)

	counts := make(map[string]int, len(filtered))
	scanned := 0
	for cursor.Next(ctx) {
		var raw bson.M
		if err := cursor.Decode(&raw); err != nil {
			// Decode errors on an individual doc don't abort the pass —
			// schemas in the wild are inconsistent and we want coverage
			// numbers over the whole sample.
			if td.logger != nil {
				td.logger.Warn("timestamp detector: doc decode error",
					zap.String("collection", sourceColl), zap.Error(err))
			}
			continue
		}
		scanned++
		for _, c := range filtered {
			if v, ok := raw[c]; ok && !isZeroBSONValue(v) {
				counts[c]++
			}
		}
	}
	if err := cursor.Err(); err != nil {
		return nil, fmt.Errorf("detect: cursor %s.%s: %w", sourceDB, sourceColl, err)
	}

	result := &DetectionResult{
		SampleSize: scanned,
		Candidates: counts,
	}

	// Empty collection → cannot detect. Surface as low-confidence
	// ObjectID fallback so the rest of the pipeline continues — Mongo
	// ObjectIDs embed a unix-sec insertion timestamp in the top 4 bytes
	// and extractTimestampMs() already knows how to read it.
	if scanned == 0 {
		result.Field = "_id"
		result.FallbackToID = true
		result.Coverage = 0
		result.Confidence = "low"
		return result, nil
	}

	// Rank by coverage DESC, ties broken by position in candidate chain.
	// We use a stable sort on the index order of `filtered` to get
	// deterministic chain-priority tie-breaking.
	type ranked struct {
		name     string
		count    int
		chainPos int
	}
	ranks := make([]ranked, 0, len(counts))
	for i, c := range filtered {
		ranks = append(ranks, ranked{name: c, count: counts[c], chainPos: i})
	}
	sort.SliceStable(ranks, func(i, j int) bool {
		if ranks[i].count != ranks[j].count {
			return ranks[i].count > ranks[j].count
		}
		return ranks[i].chainPos < ranks[j].chainPos
	})

	top := ranks[0]
	cov := float64(top.count) / float64(scanned)
	result.Coverage = cov

	switch {
	case top.count == 0:
		// No candidate matched — fall back to _id ObjectID timestamp.
		result.Field = "_id"
		result.FallbackToID = true
		result.Confidence = "low"
	case cov >= 0.8:
		result.Field = top.name
		result.Confidence = "high"
	case cov >= 0.3:
		result.Field = top.name
		result.Confidence = "medium"
	default:
		result.Field = top.name
		result.Confidence = "low"
		// Low confidence + non-zero coverage: keep the detected name but
		// also note the fallback so operator UI can flag this table.
		result.FallbackToID = false
	}

	if td.logger != nil {
		td.logger.Info("timestamp detector result",
			zap.String("db", sourceDB),
			zap.String("collection", sourceColl),
			zap.Int("sample_size", scanned),
			zap.String("field", result.Field),
			zap.Float64("coverage", result.Coverage),
			zap.String("confidence", result.Confidence),
			zap.Bool("fallback_to_id", result.FallbackToID),
			zap.Any("candidates", counts),
		)
	}

	_ = time.Now() // reserved for future latency reporting
	return result, nil
}

// isZeroBSONValue returns true when a value is effectively "missing" for
// the purpose of window filtering. An explicit null or a zero-time value
// cannot drive $gte/$lt so we treat it the same as an absent key.
func isZeroBSONValue(v interface{}) bool {
	if v == nil {
		return true
	}
	switch t := v.(type) {
	case time.Time:
		return t.IsZero()
	case string:
		return t == ""
	}
	return false
}
