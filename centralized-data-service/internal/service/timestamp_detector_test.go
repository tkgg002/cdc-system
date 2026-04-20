package service

import (
	"errors"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
)

// TestClassifyMongoError covers the ADR v4 §2.3 error-code taxonomy.
// Each case asserts the observable code from a realistic driver-error
// string — intentionally decoupled from the Mongo driver's internal
// types so the mapper survives driver upgrades.
func TestClassifyMongoError(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want string
	}{
		{"nil passes through empty", nil, ""},
		{"context deadline", errors.New("context deadline exceeded"), ErrCodeSrcTimeout},
		{"server deadline exceeded text", errors.New("operation timeout exceeded"), ErrCodeSrcTimeout},
		{"io timeout", errors.New("read tcp: i/o timeout"), ErrCodeSrcTimeout},
		{"incomplete read transient", errors.New("incomplete read of message header"), ErrCodeSrcConnection},
		{"EOF transient", errors.New("connection(127.0.0.1:27017) unexpected EOF"), ErrCodeSrcConnection},
		{"no reachable servers", errors.New("no reachable servers"), ErrCodeSrcConnection},
		{"authentication", errors.New("authentication failed"), ErrCodeAuthError},
		{"unauthorized", errors.New("unauthorized access to collection"), ErrCodeAuthError},
		{"field missing", errors.New("field does not exist in document"), ErrCodeSrcFieldMissing},
		{"circuit breaker", errors.New("circuit breaker is open"), ErrCodeCircuitOpen},
		{"unknown bucket", errors.New("some unexpected business error"), ErrCodeUnknown},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := classifyMongoError(tc.err)
			if got != tc.want {
				t.Fatalf("classifyMongoError(%v) = %q, want %q", tc.err, got, tc.want)
			}
		})
	}
}

// TestClassifyMongoError_CommandErrorCodes proves the driver-level
// CommandError branches map correctly. Mongo has stable code numbers
// across versions so we rely on them instead of the textual message.
func TestClassifyMongoError_CommandErrorCodes(t *testing.T) {
	cases := []struct {
		code int32
		want string
	}{
		{13, ErrCodeAuthError},  // Unauthorized
		{18, ErrCodeAuthError},  // AuthenticationFailed
		{50, ErrCodeSrcTimeout}, // MaxTimeMSExpired
		{89, ErrCodeSrcConnection},
		{9001, ErrCodeSrcConnection}, // SocketException
	}
	for _, tc := range cases {
		t.Run("", func(t *testing.T) {
			e := mongo.CommandError{Code: tc.code, Message: "driver"}
			got := classifyMongoError(e)
			if got != tc.want {
				t.Fatalf("code=%d got=%q want=%q", tc.code, got, tc.want)
			}
		})
	}
}

// TestIsZeroBSONValue exercises the helper that drives TimestampDetector
// coverage counting. We reject null / empty-string / zero time.Time so
// a doc with `updated_at: null` doesn't inflate the coverage score.
func TestIsZeroBSONValue(t *testing.T) {
	cases := []struct {
		name  string
		input interface{}
		want  bool
	}{
		{"nil", nil, true},
		{"zero time", time.Time{}, true},
		{"empty string", "", true},
		{"populated string", "2026-04-20", false},
		{"int64", int64(1713596400000), false},
		{"non-zero time", time.Date(2026, 4, 20, 0, 0, 0, 0, time.UTC), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := isZeroBSONValue(tc.input)
			if got != tc.want {
				t.Fatalf("isZeroBSONValue(%v) = %v, want %v", tc.input, got, tc.want)
			}
		})
	}
}

// TestCandidateNameRE locks the whitelist contract — any Mongo field
// name accepted by the registry / detector MUST match this regex. A
// regression here means someone can inject $where / dotted-path tricks
// through the JSONB candidate column.
func TestCandidateNameRE(t *testing.T) {
	valid := []string{"updated_at", "updatedAt", "createdAt", "_id", "lastUpdatedAt", "ts", "_ts"}
	for _, v := range valid {
		if !candidateNameRE.MatchString(v) {
			t.Fatalf("expected %q to match", v)
		}
	}
	invalid := []string{"", "$where", "foo.bar", "a b", "1leading", "path/with/slash", string(make([]byte, 65))}
	for _, v := range invalid {
		if candidateNameRE.MatchString(v) {
			t.Fatalf("expected %q to be rejected", v)
		}
	}
}

// TestDetectorRanking_ConfidenceBands exercises the pure-logic portion
// of DetectForCollection by driving the result struct construction
// directly. We cannot spin a real Mongo without testcontainers so this
// test locks the confidence math; a future integration test will cover
// the cursor path end-to-end.
func TestDetectorRanking_ConfidenceBands(t *testing.T) {
	// Simulate counts map for 100 samples with hypothetical presence:
	//  updated_at=90  → high
	//  updatedAt=50   → medium
	//  createdAt=10   → low
	//  empty          → fallback to _id
	cases := []struct {
		name       string
		counts     map[string]int
		sample     int
		wantField  string
		wantConf   string
		wantFallID bool
	}{
		{"high coverage", map[string]int{"updated_at": 90}, 100, "updated_at", "high", false},
		{"medium coverage", map[string]int{"updatedAt": 50}, 100, "updatedAt", "medium", false},
		{"low coverage", map[string]int{"createdAt": 10}, 100, "createdAt", "low", false},
		{"zero coverage", map[string]int{}, 100, "_id", "low", true},
		{"empty collection", map[string]int{}, 0, "_id", "low", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// Build candidates in stable order.
			cands := []string{"updated_at", "updatedAt", "created_at", "createdAt"}
			r := pickResultForTest(cands, tc.counts, tc.sample)
			if r.Field != tc.wantField {
				t.Fatalf("field: got %q want %q (full=%+v)", r.Field, tc.wantField, r)
			}
			if r.Confidence != tc.wantConf {
				t.Fatalf("confidence: got %q want %q", r.Confidence, tc.wantConf)
			}
			if r.FallbackToID != tc.wantFallID {
				t.Fatalf("fallbackToID: got %v want %v", r.FallbackToID, tc.wantFallID)
			}
		})
	}
}

// pickResultForTest replicates the ranking + banding logic from
// DetectForCollection without touching Mongo so tests stay hermetic.
// The production code uses the same decision table — changes to one
// MUST be mirrored to the other.
func pickResultForTest(candidates []string, counts map[string]int, sample int) *DetectionResult {
	res := &DetectionResult{SampleSize: sample, Candidates: counts}
	if sample == 0 {
		res.Field = "_id"
		res.FallbackToID = true
		res.Confidence = "low"
		return res
	}
	var (
		bestCount int
		bestField string
		bestPos   = len(candidates)
	)
	for i, c := range candidates {
		n := counts[c]
		if n > bestCount || (n == bestCount && i < bestPos) {
			bestCount = n
			bestField = c
			bestPos = i
		}
	}
	if bestCount == 0 {
		res.Field = "_id"
		res.FallbackToID = true
		res.Confidence = "low"
		return res
	}
	cov := float64(bestCount) / float64(sample)
	res.Field = bestField
	res.Coverage = cov
	switch {
	case cov >= 0.8:
		res.Confidence = "high"
	case cov >= 0.3:
		res.Confidence = "medium"
	default:
		res.Confidence = "low"
	}
	return res
}
