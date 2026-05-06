// nats_test.go — JetStream /jsz wire-contract tests.
package probes

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestNATS_OKMapsJsz(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"streams":3,"consumers":7,"messages":42,"junk":"ignored"}`))
	}))
	defer srv.Close()
	got := NATS(context.Background(), newProbeDeps(), srv.URL)
	if got["status"] != StatusUp {
		t.Errorf("status: %v", got["status"])
	}
	if got["streams"] != float64(3) || got["consumers"] != float64(7) || got["messages"] != float64(42) {
		t.Errorf("jsz fields not mapped: %+v", got)
	}
}

func TestNATS_EmptyBodyIsUnknown(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(``))
	}))
	defer srv.Close()
	got := NATS(context.Background(), newProbeDeps(), srv.URL)
	if got["status"] != StatusUnknown {
		t.Errorf("empty body must be unknown, got %v", got["status"])
	}
}

func TestNATS_HTTPErrorIsDown(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()
	got := NATS(context.Background(), newProbeDeps(), srv.URL)
	if got["status"] != StatusDown {
		t.Errorf("5xx must be down, got %v", got["status"])
	}
	if got["http_status"] != 500 {
		t.Errorf("http_status: %v", got["http_status"])
	}
}
