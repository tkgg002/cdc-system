package service

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

// TestBackoffSchedule verifies the exponential retry schedule laid out
// in plan v3 §9. Schedule is a load-bearing operational contract — a
// regression would surprise on-call with either too-aggressive retries
// (DB hammer) or too-lazy retries (SLA breach).
func TestBackoffSchedule(t *testing.T) {
	cases := []struct {
		retryCount int
		want       time.Duration
	}{
		{1, 1 * time.Minute},
		{2, 5 * time.Minute},
		{3, 30 * time.Minute},
		{4, 2 * time.Hour},
		{5, 6 * time.Hour}, // ≥5 → clamp at 6h until MaxRetries trips dead_letter
		{6, 6 * time.Hour},
		{100, 6 * time.Hour},
	}
	for _, c := range cases {
		got := BackoffDelay(c.retryCount)
		if got != c.want {
			t.Errorf("BackoffDelay(%d) = %v, want %v", c.retryCount, got, c.want)
		}
	}
}

// TestBackoffMonotonic — schedule must be non-decreasing. Even when we
// cap at 6h we never want an accidental downshift (e.g. swapped case
// labels). This is the symmetric sibling of TestBackoffSchedule: if
// someone reorders the cases without updating values the exact-match
// test fails; if someone swaps return values within the same order the
// monotonicity test catches it.
func TestBackoffMonotonic(t *testing.T) {
	var prev time.Duration
	for i := 1; i <= 10; i++ {
		d := BackoffDelay(i)
		if d < prev {
			t.Errorf("backoff decreased at retry %d: %v < prev %v", i, d, prev)
		}
		prev = d
	}
}

// TestChunkStringsBatching — heal pipeline must never emit a chunk
// larger than BatchSize (plan v3 §6 limits Mongo $in to 500).
func TestChunkStringsBatching(t *testing.T) {
	ids := make([]string, 1234)
	for i := range ids {
		ids[i] = "id-" + string(rune('a'+i%26))
	}

	chunks := chunkStrings(ids, 500)
	if len(chunks) != 3 {
		t.Fatalf("expected 3 chunks for 1234 items with size 500, got %d", len(chunks))
	}
	if len(chunks[0]) != 500 || len(chunks[1]) != 500 || len(chunks[2]) != 234 {
		t.Errorf("chunk sizes wrong: %d/%d/%d", len(chunks[0]), len(chunks[1]), len(chunks[2]))
	}
}

// TestChunkStringsEmpty — zero-input path returns nothing (callers
// already short-circuit empty but defense in depth).
func TestChunkStringsEmpty(t *testing.T) {
	chunks := chunkStrings(nil, 500)
	if len(chunks) != 0 {
		t.Errorf("expected 0 chunks for nil input, got %d", len(chunks))
	}
}

// TestChunkStringsDeterministic — plan v3 §6: chunking must be stable
// across retries so audit logs are diffable.
func TestChunkStringsDeterministic(t *testing.T) {
	ids := []string{"b", "a", "d", "c", "e"}
	c1 := chunkStrings(ids, 3)
	c2 := chunkStrings(ids, 3)

	if len(c1) != len(c2) {
		t.Fatalf("non-deterministic chunk count: %d vs %d", len(c1), len(c2))
	}
	for i := range c1 {
		if len(c1[i]) != len(c2[i]) {
			t.Fatalf("chunk %d size mismatch", i)
		}
		for j := range c1[i] {
			if c1[i][j] != c2[i][j] {
				t.Errorf("chunk %d[%d] non-deterministic: %q vs %q", i, j, c1[i][j], c2[i][j])
			}
		}
	}
}

// TestTruncateRespectsMax — DLQ last_error goes into TEXT but we still
// truncate to bound log volume.
func TestTruncate(t *testing.T) {
	s := "hello world"
	if got := truncate(s, 100); got != s {
		t.Errorf("truncate under max changed string: %q", got)
	}
	if got := truncate(s, 5); got != "hello…" {
		t.Errorf("truncate over max: %q", got)
	}
	if got := truncate(s, 0); got != s {
		t.Errorf("truncate zero should pass through: %q", got)
	}
}

func TestDLQWorkerBuildRetryRawJSONMasksTopLevelFields(t *testing.T) {
	worker := &DLQWorker{}
	worker.SetMaskingService(NewMaskingService(nil, nil, "phone", "email"))

	raw := worker.buildRetryRawJSON("customer_profiles", map[string]interface{}{
		"phone": "0901234567",
		"email": "alice@example.com",
		"name":  "Alice",
	})

	payload := decodeDLQWorkerRawJSON(t, raw)
	if payload["phone"] != "***" {
		t.Fatalf("phone should be masked, got %#v", payload["phone"])
	}
	if payload["email"] != "***" {
		t.Fatalf("email should be masked, got %#v", payload["email"])
	}
	if payload["name"] != "Alice" {
		t.Fatalf("non-sensitive field changed unexpectedly, got %#v", payload["name"])
	}
}

func TestDLQWorkerBuildRetryRawJSONMasksNestedAndArrayFields(t *testing.T) {
	worker := &DLQWorker{}
	worker.SetMaskingService(NewMaskingService(nil, nil, "balance", "phone"))

	raw := worker.buildRetryRawJSON("wallet_accounts", map[string]interface{}{
		"metadata": map[string]interface{}{
			"user_info": map[string]interface{}{
				"balance": float64(125000),
				"tier":    "gold",
			},
		},
		"contacts": []interface{}{
			map[string]interface{}{"phone_number": "0901", "label": "home"},
		},
	})

	payload := decodeDLQWorkerRawJSON(t, raw)
	metadata := payload["metadata"].(map[string]interface{})
	userInfo := metadata["user_info"].(map[string]interface{})
	if userInfo["balance"] != "***" {
		t.Fatalf("nested balance should be masked, got %#v", userInfo["balance"])
	}
	contacts := payload["contacts"].([]interface{})
	first := contacts[0].(map[string]interface{})
	if first["phone_number"] != "***" {
		t.Fatalf("array phone_number should be masked, got %#v", first["phone_number"])
	}
	if first["label"] != "home" {
		t.Fatalf("non-sensitive array field changed unexpectedly, got %#v", first["label"])
	}
}

func TestDLQWorkerBuildRetryRawJSONHeuristicMaskingWithoutRegistry(t *testing.T) {
	worker := &DLQWorker{}
	worker.SetMaskingService(NewMaskingService(nil, nil))

	raw := worker.buildRetryRawJSON("orders", map[string]interface{}{
		"customer_secret":   "token-123",
		"remaining_balance": 5000,
		"altPhone":          "0902",
		"status":            "active",
	})

	payload := decodeDLQWorkerRawJSON(t, raw)
	for _, field := range []string{"customer_secret", "remaining_balance", "altPhone"} {
		if payload[field] != "***" {
			t.Fatalf("%s should be masked heuristically, got %#v", field, payload[field])
		}
	}
	if payload["status"] != "active" {
		t.Fatalf("heuristic masking touched non-sensitive field, got %#v", payload["status"])
	}
	if strings.Contains(string(raw), "0902") {
		t.Fatalf("raw retry JSON still contains unmasked phone: %s", string(raw))
	}
}

func decodeDLQWorkerRawJSON(t *testing.T, raw []byte) map[string]interface{} {
	t.Helper()

	var payload map[string]interface{}
	if err := json.Unmarshal(raw, &payload); err != nil {
		t.Fatalf("unmarshal dlq worker raw JSON: %v", err)
	}
	return payload
}
