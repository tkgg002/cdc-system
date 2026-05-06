// kafka_connect_test.go — KafkaConnect REST root probe.
package probes

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestKafkaConnect_OKMergesClusterMeta(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"version":"7.6.1","commit":"abcd","kafka_cluster_id":"cluster-x"}`))
	}))
	defer srv.Close()
	got := KafkaConnect(context.Background(), newProbeDeps(), srv.URL)
	if got["status"] != StatusUp {
		t.Errorf("status: %v", got["status"])
	}
	if got["version"] != "7.6.1" || got["kafka_cluster_id"] != "cluster-x" {
		t.Errorf("cluster meta not merged: %+v", got)
	}
}

func TestKafkaConnect_HTTPErrorIsDown(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
	}))
	defer srv.Close()
	got := KafkaConnect(context.Background(), newProbeDeps(), srv.URL)
	if got["status"] != StatusDown {
		t.Errorf("status: %v", got["status"])
	}
	if got["http_status"] != 502 {
		t.Errorf("http_status: %v", got["http_status"])
	}
}
