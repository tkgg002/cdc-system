// debezium_test.go — Connector status wire-contract.
//
// Critical invariant: FAILED task traces are truncated to 500 chars
// before they land in the snapshot — leaking unbounded stack traces
// to the FE banner is both a UX issue and a potential PII risk.
package probes

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestDebezium_RunningExtractsState(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.Contains(r.URL.Path, "/connectors/goopay-mongodb-cdc/status") {
			t.Errorf("URL path: %q", r.URL.Path)
		}
		_, _ = w.Write([]byte(`{
			"connector":{"state":"RUNNING"},
			"tasks":[{"id":0,"state":"RUNNING"}]
		}`))
	}))
	defer srv.Close()
	got := Debezium(context.Background(), newProbeDeps(), srv.URL, "goopay-mongodb-cdc")
	if got["status"] != "RUNNING" {
		t.Errorf("status: %v", got["status"])
	}
	if got["connector"] != "goopay-mongodb-cdc" {
		t.Errorf("connector echoed wrong: %v", got["connector"])
	}
	if tasks, ok := got["tasks"].([]map[string]any); !ok || len(tasks) != 1 {
		t.Errorf("tasks shape: %+v", got["tasks"])
	}
}

func TestDebezium_FailedTaskTraceTruncated(t *testing.T) {
	long := strings.Repeat("X", 1200) // > 500 budget
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{
			"connector":{"state":"RUNNING"},
			"tasks":[
				{"id":0,"state":"RUNNING"},
				{"id":1,"state":"FAILED","trace":"` + long + `"}
			]
		}`))
	}))
	defer srv.Close()
	got := Debezium(context.Background(), newProbeDeps(), srv.URL, "x")
	tasks, _ := got["tasks"].([]map[string]any)
	var failedTrace string
	for _, t := range tasks {
		if t["state"] == "FAILED" {
			failedTrace, _ = t["trace"].(string)
		}
	}
	if failedTrace == "" {
		t.Fatalf("FAILED task missing trace: %+v", tasks)
	}
	// Spec: 500 chars + "..." marker (3 chars).
	if len(failedTrace) != 503 {
		t.Errorf("trace truncation budget violated: len=%d (want 503)", len(failedTrace))
	}
	if !strings.HasSuffix(failedTrace, "...") {
		t.Errorf("trace missing truncation marker: ...%q", failedTrace[len(failedTrace)-10:])
	}
}

func TestDebezium_NotFoundIsDown(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()
	got := Debezium(context.Background(), newProbeDeps(), srv.URL, "missing-connector")
	if got["status"] != StatusDown {
		t.Errorf("404 must be down, got %v", got["status"])
	}
}

func TestDebezium_EmptyBodyIsUnknown(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(``))
	}))
	defer srv.Close()
	got := Debezium(context.Background(), newProbeDeps(), srv.URL, "x")
	if got["status"] != StatusUnknown {
		t.Errorf("empty body must be unknown, got %v", got["status"])
	}
}
