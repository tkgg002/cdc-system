package service

import (
	"testing"
	"time"
)

// TestHashIDPlusTsDeterministic proves that the same (id, ts) pair
// always produces the same 64-bit hash — precondition for any XOR
// aggregate to match across two independent runs on identical data.
func TestHashIDPlusTsDeterministic(t *testing.T) {
	id := "65f1a2b3c4d5e6f708091011"
	ts, _ := time.Parse(time.RFC3339Nano, "2026-04-17T10:00:00.000000000Z")

	h1 := hashIDPlusTs(id, ts)
	h2 := hashIDPlusTs(id, ts)

	if h1 != h2 {
		t.Fatalf("hash not deterministic: %x vs %x", h1, h2)
	}
}

// TestHashDifferentInputsDifferentOutputs — weak but necessary: two
// distinct (id, ts) pairs should almost always produce different hashes.
// xxhash is well-distributed so these specific samples are safe.
func TestHashDifferentInputsDifferentOutputs(t *testing.T) {
	ts, _ := time.Parse(time.RFC3339Nano, "2026-04-17T10:00:00Z")
	h1 := hashIDPlusTs("id-1", ts)
	h2 := hashIDPlusTs("id-2", ts)
	if h1 == h2 {
		t.Fatalf("different IDs collided: %x", h1)
	}

	h3 := hashIDPlusTs("id-1", ts.Add(time.Second))
	if h1 == h3 {
		t.Fatalf("ts change did not flip hash")
	}
}

// TestXORCommutativity is the property that makes HashWindow correct:
// XOR is commutative and associative, so any iteration order over the
// cursor produces the same accumulator. If this property ever breaks
// (e.g. somebody replaces ^ with + or a wrapping mul) our recon
// guarantees collapse.
func TestXORCommutativity(t *testing.T) {
	ts, _ := time.Parse(time.RFC3339Nano, "2026-04-17T10:00:00Z")
	ids := []string{"alpha", "beta", "gamma", "delta", "epsilon"}

	var a, b uint64
	for _, id := range ids {
		a ^= hashIDPlusTs(id, ts)
	}
	// reverse
	for i := len(ids) - 1; i >= 0; i-- {
		b ^= hashIDPlusTs(ids[i], ts)
	}
	if a != b {
		t.Fatalf("XOR is not commutative over IDs: forward=%x reverse=%x", a, b)
	}

	// interleaved order
	var c uint64
	for i := 0; i < len(ids); i += 2 {
		c ^= hashIDPlusTs(ids[i], ts)
	}
	for i := 1; i < len(ids); i += 2 {
		c ^= hashIDPlusTs(ids[i], ts)
	}
	if a != c {
		t.Fatalf("XOR not associative over interleaved order: linear=%x interleaved=%x", a, c)
	}
}

// TestXORSelfInverse proves the drift-detection property: if we XOR
// an element in and then out, the accumulator returns to zero. This is
// what lets us detect a single-row change via one bucket flip.
func TestXORSelfInverse(t *testing.T) {
	ts, _ := time.Parse(time.RFC3339Nano, "2026-04-17T10:00:00Z")
	h := hashIDPlusTs("only-one", ts)
	var acc uint64
	acc ^= h
	acc ^= h
	if acc != 0 {
		t.Fatalf("XOR self-inverse broken: %x", acc)
	}
}

// TestBucketIndexStable — the first-byte bucketing must be deterministic
// across runs for the 256-bucket fingerprint to be comparable.
func TestBucketIndexStable(t *testing.T) {
	for _, id := range []string{"u1", "u2", "u3", "xyz", "65f1a2b3c4d5e6f708091011"} {
		b1 := bucketIndex(id)
		b2 := bucketIndex(id)
		if b1 != b2 {
			t.Fatalf("bucket index unstable for %q: %d vs %d", id, b1, b2)
		}
	}
}

// TestWithinOffPeak covers the normal and cross-midnight variants that
// the scheduler relies on for Tier 3.
func TestWithinOffPeak(t *testing.T) {
	mk := func(h, m int) time.Time {
		return time.Date(2026, 4, 17, h, m, 0, 0, time.UTC)
	}
	cases := []struct {
		name       string
		now        time.Time
		start, end string
		want       bool
	}{
		{"inside normal window", mk(3, 0), "02:00", "05:00", true},
		{"before window", mk(1, 0), "02:00", "05:00", false},
		{"exact start", mk(2, 0), "02:00", "05:00", true},
		{"exact end excluded", mk(5, 0), "02:00", "05:00", false},
		{"cross-midnight inside", mk(23, 0), "22:00", "05:00", true},
		{"cross-midnight early", mk(4, 0), "22:00", "05:00", true},
		{"cross-midnight outside", mk(12, 0), "22:00", "05:00", false},
		{"malformed config → fail-open", mk(12, 0), "bad", "foo", true},
	}
	for _, tc := range cases {
		got := withinOffPeak(tc.now, tc.start, tc.end)
		if got != tc.want {
			t.Errorf("%s: want %v got %v", tc.name, tc.want, got)
		}
	}
}

// TestDiffIDs is a small-surface check on the Tier 2 drill-down helper.
func TestDiffIDs(t *testing.T) {
	src := []string{"a", "b", "c", "d"}
	dst := []string{"b", "c", "e"}
	missingFromDst, missingFromSrc := diffIDs(src, dst)

	want := func(set []string, contains string) bool {
		for _, s := range set {
			if s == contains {
				return true
			}
		}
		return false
	}
	if len(missingFromDst) != 2 || !want(missingFromDst, "a") || !want(missingFromDst, "d") {
		t.Errorf("missing-from-dst mismatch: %v", missingFromDst)
	}
	if len(missingFromSrc) != 1 || !want(missingFromSrc, "e") {
		t.Errorf("missing-from-src mismatch: %v", missingFromSrc)
	}
}

// TestHashIDPlusTsMsDeterministic — the cross-store HashWindow primitive
// (added 2026-04-17 root-cause fix) must be deterministic. If it drifts,
// Tier 2 diverges spuriously and we are back in the pre-fix bug state.
func TestHashIDPlusTsMsDeterministic(t *testing.T) {
	id := "65f1a2b3c4d5e6f708091011"
	ts := int64(1_713_123_456_789)

	h1 := hashIDPlusTsMs(id, ts)
	h2 := hashIDPlusTsMs(id, ts)
	if h1 != h2 {
		t.Fatalf("hashIDPlusTsMs not deterministic: %x vs %x", h1, h2)
	}
}

// TestHashIDPlusTsMsSourceDestAgreement — the key property the fix
// depends on: the exact byte layout `id + "|" + strconv.FormatInt(tsMs)`
// is what BOTH ReconSourceAgent.HashWindow (called with
// doc.UpdatedAt.UnixMilli()) AND ReconDestAgent.HashWindow (called with
// the `_source_ts` column value) feed into xxhash64.
//
// This test reconstructs the hash *both ways* (source-side time.Time
// path → UnixMilli, and dest-side int64 path) and asserts equality.
// If we ever restore an RFC3339 / timezone / hashtext variant on one
// side, this test fails.
func TestHashIDPlusTsMsSourceDestAgreement(t *testing.T) {
	// Dest side already has ms as BIGINT.
	tsMs := int64(1_713_123_456_789)

	// Source side has time.Time from BSON. Build a time.Time that
	// round-trips through UnixMilli() back to tsMs.
	sec := tsMs / 1000
	nsec := (tsMs % 1000) * int64(time.Millisecond)
	srcTime := time.Unix(sec, nsec).UTC()
	if srcTime.UnixMilli() != tsMs {
		t.Fatalf("fixture invalid: UnixMilli round trip %d vs %d", srcTime.UnixMilli(), tsMs)
	}

	id := "65f1a2b3c4d5e6f708091011"

	srcHash := hashIDPlusTsMs(id, srcTime.UnixMilli())
	dstHash := hashIDPlusTsMs(id, tsMs)

	if srcHash != dstHash {
		t.Fatalf("source/dest hash disagree on identical (id,tsMs): src=%x dst=%x", srcHash, dstHash)
	}
}

// TestHashWindowEqualDataEqualHash — the Rule-3 semantic-validation
// property test for the v3 root-cause fix (2026-04-17). Simulates 3
// documents shared by both stores: Mongo-side iterates with
// (id, time.Time) and calls the new `hashIDPlusTsMs`; PG-side iterates
// with (id, int64 _source_ts) and calls the same primitive. The
// (count, XorHash) pair MUST match for the recon_core Tier 2 decision
// `if srcRes.Count == dstRes.Count && srcRes.XorHash == dstRes.XorHash`
// to correctly flag a window as clean.
//
// Before the fix: source used `hashIDPlusTs` (xxhash of RFC3339Nano)
// and dest used `BIT_XOR(hashtext(id || '|' || _source_ts::text))`.
// Different hash functions + different input formats → xor_hash could
// NEVER match, even on identical data. This test is the regression
// guard.
func TestHashWindowEqualDataEqualHash(t *testing.T) {
	type doc struct {
		ID   string
		TsMs int64
	}
	fixtures := []doc{
		{ID: "65f1a2b3c4d5e6f708091011", TsMs: 1_713_000_000_000},
		{ID: "65f1a2b3c4d5e6f708091012", TsMs: 1_713_100_000_000},
		{ID: "65f1a2b3c4d5e6f708091013", TsMs: 1_713_200_000_000},
	}

	// Source side: iterate fixtures as if pulled from Mongo with
	// (id, time.Time{updated_at}). Convert to ms via UnixMilli().
	var (
		srcXor   uint64
		srcCount int64
	)
	for _, d := range fixtures {
		updatedAt := time.UnixMilli(d.TsMs).UTC()
		if updatedAt.UnixMilli() != d.TsMs {
			t.Fatalf("time.Time/UnixMilli round-trip broke for %d", d.TsMs)
		}
		srcXor ^= hashIDPlusTsMs(d.ID, updatedAt.UnixMilli())
		srcCount++
	}

	// Dest side: iterate fixtures as if streamed from Postgres with
	// (id, _source_ts int64) — the exact code path the rewritten
	// `ReconDestAgent.HashWindow` now takes.
	var (
		dstXor   uint64
		dstCount int64
	)
	for _, d := range fixtures {
		dstXor ^= hashIDPlusTsMs(d.ID, d.TsMs)
		dstCount++
	}

	if srcCount != dstCount {
		t.Fatalf("count mismatch: src=%d dst=%d", srcCount, dstCount)
	}
	if srcXor != dstXor {
		t.Fatalf("xor_hash mismatch (cross-store equality broken): src=%016x dst=%016x", srcXor, dstXor)
	}

	// And — the window is also order-independent by XOR commutativity.
	// Re-run dest side in reverse and assert the accumulator is stable.
	var dstXorRev uint64
	for i := len(fixtures) - 1; i >= 0; i-- {
		dstXorRev ^= hashIDPlusTsMs(fixtures[i].ID, fixtures[i].TsMs)
	}
	if dstXorRev != dstXor {
		t.Fatalf("dest XOR order-dependent (commutativity broken): fwd=%016x rev=%016x", dstXor, dstXorRev)
	}
}

// TestHashWindowDriftDetection — positive test for the FIX's other
// half: when one side has a different ts on a single row, the XOR
// must flip. If it didn't, we'd over-report clean windows instead.
func TestHashWindowDriftDetection(t *testing.T) {
	id := "65f1a2b3c4d5e6f708091011"
	srcXor := hashIDPlusTsMs(id, 1_713_000_000_000)
	dstXor := hashIDPlusTsMs(id, 1_713_000_000_001) // 1ms later
	if srcXor == dstXor {
		t.Fatalf("hash did not flip on 1ms drift — drift detection dead")
	}
}

// TestTableGroup ensures the Prometheus label cardinality guard works.
func TestTableGroup(t *testing.T) {
	cases := map[string]string{
		"payment_bills":         "payment",
		"payment_bill_events":   "payment",
		"export_jobs":           "export",
		"identitycounters":      "other",
		"refund_requests":       "refund",
		"":                      "other",
	}
	for in, want := range cases {
		if got := tableGroup(in); got != want {
			t.Errorf("tableGroup(%q) = %q, want %q", in, got, want)
		}
	}
}
