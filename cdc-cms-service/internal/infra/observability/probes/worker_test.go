// worker_test.go — HTTP shape tests via httptest stub.
// Covers the wire contract: status=up + latency_ms on 2xx, status=down
// + http_status on >=300, status=unknown + sanitized error on transport
// failure. The stub server avoids hitting the real worker /healthz.
package probes

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func newProbeDeps() HTTPDeps {
	return HTTPDeps{
		Client:       http.DefaultClient,
		ProbeTimeout: 1 * time.Second,
	}
}

func TestWorker_OK(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/healthz") {
			t.Errorf("want /healthz suffix, got %q", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"build":"abc123","uptime":42}`))
	}))
	defer srv.Close()

	got := Worker(context.Background(), newProbeDeps(), srv.URL)
	if got["status"] != StatusUp {
		t.Errorf("status: got %v want %v", got["status"], StatusUp)
	}
	if got["build"] != "abc123" {
		t.Errorf("body field merged: got %v", got["build"])
	}
	if _, ok := got["latency_ms"].(int64); !ok {
		t.Errorf("latency_ms missing or wrong type: %T", got["latency_ms"])
	}
}

func TestWorker_Down(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()
	got := Worker(context.Background(), newProbeDeps(), srv.URL)
	if got["status"] != StatusDown {
		t.Errorf("status: got %v want down", got["status"])
	}
	if got["http_status"] != 503 {
		t.Errorf("http_status: got %v", got["http_status"])
	}
}

func TestWorker_TransportError(t *testing.T) {
	// Non-routable URL ⇒ dial error.
	got := Worker(context.Background(), newProbeDeps(), "http://127.0.0.1:1/")
	if got["status"] != StatusUnknown {
		t.Errorf("status: got %v want unknown", got["status"])
	}
	errStr, _ := got["error"].(string)
	if errStr == "" {
		t.Errorf("error field missing")
	}
	// URL prefix (scheme://host:port/path) MUST be redacted. Note: the
	// bare `host:port` substring inside Go's dial error (e.g.
	// "dial tcp 127.0.0.1:1: connect: ...") is NOT a URL by SanitizeErr's
	// contract — it's a runtime artifact and stays. The redaction marker
	// presence confirms the URL itself was scrubbed.
	if !strings.Contains(errStr, "<scheme-redacted>") {
		t.Errorf("URL not redacted: %q", errStr)
	}
	if strings.Contains(errStr, "http://127.0.0.1:1") {
		t.Errorf("URL prefix leaked: %q", errStr)
	}
}

func TestWorker_UnparseableBodyStillUp(t *testing.T) {
	// Wire contract: 2xx with garbled body still reports up; the
	// merge degrades to an empty obj rather than an error.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("not-json"))
	}))
	defer srv.Close()
	got := Worker(context.Background(), newProbeDeps(), srv.URL)
	if got["status"] != StatusUp {
		t.Errorf("garbled body should still be up: %v", got)
	}
}
