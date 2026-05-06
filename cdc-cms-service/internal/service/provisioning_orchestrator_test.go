// provisioning_orchestrator_test.go — pure-fn guards for the
// orchestrator's helpers. The DB-bound CAS/publish path stays in
// deploy-time E2E (project convention).
package service

import (
	"context"
	"strings"
	"testing"

	"go.opentelemetry.io/otel/trace"
)

func TestNewProvisioningCorrelationID_FormatAndUniqueness(t *testing.T) {
	a := newProvisioningCorrelationID(42, "shadow_bind")
	if !strings.HasPrefix(a, "prov-42-shadow_bind-") {
		t.Errorf("prefix wrong: %q", a)
	}
	// Subsequent call has a different timestamp suffix.
	b := newProvisioningCorrelationID(42, "shadow_bind")
	if a == b {
		t.Errorf("collision in correlation id: %q == %q", a, b)
	}
	c := newProvisioningCorrelationID(99, "schedule_enable")
	if !strings.Contains(c, "-99-") || !strings.Contains(c, "-schedule_enable-") {
		t.Errorf("sourceID/step encoded wrong: %q", c)
	}
}

func TestInjectProvisioningTraceContext_NilPayloadIsNoop(t *testing.T) {
	// Must not panic.
	injectProvisioningTraceContext(context.Background(), nil)
}

func TestInjectProvisioningTraceContext_NoSpanLeavesPayloadUntouched(t *testing.T) {
	payload := map[string]any{"foo": "bar"}
	injectProvisioningTraceContext(context.Background(), payload)
	if len(payload) != 1 || payload["foo"] != "bar" {
		t.Errorf("payload mutated when no span present: %v", payload)
	}
}

func TestInjectProvisioningTraceContext_StampsValidSpan(t *testing.T) {
	tid, _ := trace.TraceIDFromHex("0102030405060708090a0b0c0d0e0f10")
	sid, _ := trace.SpanIDFromHex("1112131415161718")
	sc := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    tid,
		SpanID:     sid,
		TraceFlags: trace.FlagsSampled,
		Remote:     true,
	})
	ctx := trace.ContextWithSpanContext(context.Background(), sc)
	payload := map[string]any{}
	injectProvisioningTraceContext(ctx, payload)
	if payload["trace_id"] != "0102030405060708090a0b0c0d0e0f10" {
		t.Errorf("trace_id wrong: %v", payload["trace_id"])
	}
	if payload["span_id"] != "1112131415161718" {
		t.Errorf("span_id wrong: %v", payload["span_id"])
	}
}

func TestProvisioningEntryWithSpan_ValidSpanStamps(t *testing.T) {
	tid, _ := trace.TraceIDFromHex("a1a2a3a4a5a6a7a8a9aaababacadaeaf")
	sid, _ := trace.SpanIDFromHex("b1b2b3b4b5b6b7b8")
	sc := trace.NewSpanContext(trace.SpanContextConfig{TraceID: tid, SpanID: sid, TraceFlags: trace.FlagsSampled})
	ctx := trace.ContextWithSpanContext(context.Background(), sc)
	in := provisioningStepLogEntry{Step: "shadow_bind"}
	out := provisioningEntryWithSpan(ctx, in)
	if out.TraceID != "a1a2a3a4a5a6a7a8a9aaababacadaeaf" {
		t.Errorf("trace id: %v", out.TraceID)
	}
	if out.SpanID != "b1b2b3b4b5b6b7b8" {
		t.Errorf("span id: %v", out.SpanID)
	}
	if out.Step != "shadow_bind" {
		t.Errorf("input fields lost: %+v", out)
	}
}

func TestProvisioningEntryWithSpan_NoSpanLeavesEntryUntouched(t *testing.T) {
	in := provisioningStepLogEntry{Step: "x", TraceID: "preset"}
	out := provisioningEntryWithSpan(context.Background(), in)
	if out.TraceID != "preset" {
		t.Errorf("preset trace_id should not be overwritten when no span: %v", out.TraceID)
	}
}
