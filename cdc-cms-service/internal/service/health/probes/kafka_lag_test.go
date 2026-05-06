// kafka_lag_test.go — kafka-exporter scraper wire-contract.
//
// The HighConsumerLag alert reads total_lag through service.toFloat64,
// so an int64 produced here is what the alert pipeline expects.
// Negative values during rebalance must be skipped, otherwise total
// would oscillate around 0 and noise alerts.
package probes

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

const promBody = `# HELP kafka_consumergroup_lag Current Approximate Lag of a ConsumerGroup at Topic/Partition
# TYPE kafka_consumergroup_lag gauge
kafka_consumergroup_lag{consumergroup="g1",topic="cdc.goopay.orders",partition="0"} 7
kafka_consumergroup_lag{consumergroup="g1",topic="cdc.goopay.orders",partition="1"} 3
kafka_consumergroup_lag{consumergroup="g2",topic="cdc.goopay.users",partition="0"} 5
kafka_consumergroup_lag{consumergroup="g3",topic="other.topic",partition="0"} 11
kafka_consumergroup_lag{consumergroup="g4",topic="cdc.goopay.rebalancing",partition="0"} -1
`

func TestKafkaLag_EmptyURLIsUnknown(t *testing.T) {
	got := KafkaLag(context.Background(), newProbeDeps(), "", "cdc.goopay.")
	if got["status"] != StatusUnknown {
		t.Errorf("empty URL should be unknown, got %v", got["status"])
	}
	if got["error"] != "kafka_exporter_url not configured" {
		t.Errorf("error message: %v", got["error"])
	}
}

func TestKafkaLag_AggregatesAndFiltersByPrefix(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		_, _ = w.Write([]byte(promBody))
	}))
	defer srv.Close()
	got := KafkaLag(context.Background(), newProbeDeps(), srv.URL, "cdc.goopay.")
	if got["status"] != StatusOK {
		t.Fatalf("status: %v (err=%v)", got["status"], got["error"])
	}
	// total_lag sums across ALL groups/topics (excluding -1 rebalance).
	// 7+3+5+11 = 26 (the -1 is skipped per the rebalance guard).
	if total, ok := got["total_lag"].(int64); !ok || total != 26 {
		t.Errorf("total_lag: got %v want 26", got["total_lag"])
	}
	per, ok := got["per_topic"].(map[string]int64)
	if !ok {
		t.Fatalf("per_topic shape: %T", got["per_topic"])
	}
	// Filter applied — only cdc.goopay.* topics, not "other.topic".
	if per["cdc.goopay.orders"] != 10 {
		t.Errorf("orders aggregate: %v", per["cdc.goopay.orders"])
	}
	if per["cdc.goopay.users"] != 5 {
		t.Errorf("users: %v", per["cdc.goopay.users"])
	}
	if _, present := per["other.topic"]; present {
		t.Errorf("non-prefix topic leaked into per_topic: %v", per)
	}
}

func TestKafkaLag_AbsentMetricFamilyIsOKWithZero(t *testing.T) {
	// kafka-exporter up but no consumers yet — metric family missing.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("# nothing here\n"))
	}))
	defer srv.Close()
	got := KafkaLag(context.Background(), newProbeDeps(), srv.URL, "")
	if got["status"] != StatusOK {
		t.Errorf("missing metric family should be OK, got %v", got["status"])
	}
	if got["total_lag"] != int64(0) {
		t.Errorf("total_lag: %v", got["total_lag"])
	}
}

func TestKafkaLag_HTTPErrorIsDown(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()
	got := KafkaLag(context.Background(), newProbeDeps(), srv.URL, "")
	if got["status"] != StatusDown {
		t.Errorf("5xx should be down, got %v", got["status"])
	}
	if got["http_status"] != 500 {
		t.Errorf("http_status: %v", got["http_status"])
	}
}

func TestKafkaLag_ParseErrorIsUnknown(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("not-prometheus-format\n@@@\n"))
	}))
	defer srv.Close()
	got := KafkaLag(context.Background(), newProbeDeps(), srv.URL, "")
	if got["status"] != StatusUnknown {
		t.Errorf("parse fail should be unknown, got %v", got["status"])
	}
	errStr, _ := got["error"].(string)
	if !strings.Contains(errStr, "parse metrics") {
		t.Errorf("error should reference parse: %q", errStr)
	}
}

func TestKafkaLag_NoPrefixCollectsAllTopics(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(promBody))
	}))
	defer srv.Close()
	got := KafkaLag(context.Background(), newProbeDeps(), srv.URL, "")
	per, _ := got["per_topic"].(map[string]int64)
	if _, ok := per["other.topic"]; !ok {
		t.Errorf("empty prefix must collect all, missing other.topic: %v", per)
	}
}
