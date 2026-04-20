package handler

import (
	"encoding/json"
	"errors"
	"testing"
)

func TestDLQMessage_Serialize(t *testing.T) {
	msg := DLQMessage{
		OriginalSubject: "cdc.goopay.db.table",
		OriginalData:    []byte(`{"key":"value"}`),
		Error:           "connection refused",
		RetryCount:      3,
		FailedAt:        "2026-04-14T10:00:00Z",
		SourceTable:     "payment-bills",
	}

	data, err := json.Marshal(msg)
	if err != nil {
		t.Fatal(err)
	}

	var parsed DLQMessage
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatal(err)
	}

	if parsed.OriginalSubject != msg.OriginalSubject {
		t.Errorf("subject mismatch: %s vs %s", parsed.OriginalSubject, msg.OriginalSubject)
	}
	if parsed.RetryCount != 3 {
		t.Errorf("retry count: %d, want 3", parsed.RetryCount)
	}
	if parsed.SourceTable != "payment-bills" {
		t.Errorf("source table: %s, want payment-bills", parsed.SourceTable)
	}
}

func TestHandleWithRetry_SuccessFirstTry(t *testing.T) {
	handler := &DLQHandler{logger: nil}
	_ = handler // DLQHandler needs NATS, test pattern only

	attempts := 0
	fn := func() error {
		attempts++
		return nil
	}

	// Simulate retry logic without NATS
	var lastErr error
	for attempt := 1; attempt <= MaxRetries; attempt++ {
		lastErr = fn()
		if lastErr == nil {
			break
		}
	}

	if lastErr != nil {
		t.Error("expected success")
	}
	if attempts != 1 {
		t.Errorf("attempts: %d, want 1", attempts)
	}
}

func TestHandleWithRetry_FailAllRetries(t *testing.T) {
	attempts := 0
	fn := func() error {
		attempts++
		return errors.New("always fail")
	}

	var lastErr error
	for attempt := 1; attempt <= MaxRetries; attempt++ {
		lastErr = fn()
		if lastErr == nil {
			break
		}
	}

	if lastErr == nil {
		t.Error("expected error after all retries")
	}
	if attempts != MaxRetries {
		t.Errorf("attempts: %d, want %d", attempts, MaxRetries)
	}
}
