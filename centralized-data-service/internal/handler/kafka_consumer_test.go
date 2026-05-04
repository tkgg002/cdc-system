package handler

import (
	"context"
	"testing"

	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

// TestTopicSetEqual verifies the helper with 4 representative cases.
func TestTopicSetEqual(t *testing.T) {
	cases := []struct {
		name string
		a, b []string
		want bool
	}{
		{"equal unordered", []string{"a", "b"}, []string{"b", "a"}, true},
		{"different length", []string{"a", "b"}, []string{"a"}, false},
		{"different content", []string{"a", "b"}, []string{"a", "c"}, false},
		{"both empty", nil, nil, true},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			got := topicSetEqual(tc.a, tc.b)
			if got != tc.want {
				t.Fatalf("topicSetEqual(%v, %v) = %v, want %v", tc.a, tc.b, got, tc.want)
			}
		})
	}
}

// TestRefreshTopics_NoChange verifies that when discovered topics equal the
// current set, RefreshTopics returns nil and does NOT recreate the reader.
func TestRefreshTopics_NoChange(t *testing.T) {
	// Use a nil *kafka.Reader in the slice; RefreshTopics only Close()s readers
	// when the topic set changes, so this won't panic on NoChange path.
	kc := &KafkaConsumer{
		logger:        zap.NewNop(),
		currentTopics: []string{"a", "b"},
		readers:       []*kafka.Reader{nil}, // 1 entry, won't be closed
		batches:       make(map[string]*batchStats),
	}
	kc.discoverFunc = func(ctx context.Context) ([]string, error) {
		return []string{"a", "b"}, nil
	}

	err := kc.RefreshTopics(context.Background())
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if len(kc.readers) != 1 {
		t.Fatalf("expected readers len=1 (unchanged), got %d", len(kc.readers))
	}
	if !topicSetEqual(kc.currentTopics, []string{"a", "b"}) {
		t.Fatalf("expected currentTopics unchanged, got %v", kc.currentTopics)
	}
}

// TestRefreshTopics_AddTopic verifies that when topics change, RefreshTopics
// rebuilds the reader and updates currentTopics.
// buildReader only creates a kafka.Reader struct (no actual TCP connection),
// so this test works without a live broker.
func TestRefreshTopics_AddTopic(t *testing.T) {
	kc := &KafkaConsumer{
		logger:        zap.NewNop(),
		currentTopics: []string{"a"},
		readers:       []*kafka.Reader{}, // empty — nothing to close
		batches:       make(map[string]*batchStats),
		config: KafkaConsumerConfig{
			Brokers: []string{"localhost:9092"},
			GroupID: "test-group",
		},
	}
	kc.discoverFunc = func(ctx context.Context) ([]string, error) {
		return []string{"a", "b"}, nil
	}

	err := kc.RefreshTopics(context.Background())
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if !topicSetEqual(kc.currentTopics, []string{"a", "b"}) {
		t.Fatalf("expected currentTopics [a b], got %v", kc.currentTopics)
	}
	if len(kc.readers) != 1 {
		t.Fatalf("expected 1 new reader, got %d", len(kc.readers))
	}
}
