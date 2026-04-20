package idgen

import (
	"testing"

	"go.uber.org/zap"
)

func BenchmarkNextID(b *testing.B) {
	logger, _ := zap.NewDevelopment()
	Init(logger)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := NextID()
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkNextID_Parallel(b *testing.B) {
	logger, _ := zap.NewDevelopment()
	Init(logger)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, err := NextID()
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

// TestGenerate1M generates 1M IDs and reports stats
func TestGenerate1M(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping 1M ID generation in short mode")
	}

	logger, _ := zap.NewDevelopment()
	Init(logger)

	const count = 1_000_000
	ids := make([]uint64, 0, count)
	unique := make(map[uint64]bool, count)

	for i := 0; i < count; i++ {
		id, err := NextID()
		if err != nil {
			t.Fatalf("failed at %d: %v", i, err)
		}
		ids = append(ids, id)
		if unique[id] {
			t.Fatalf("duplicate at %d: %d", i, id)
		}
		unique[id] = true
	}

	// Stats
	minID, maxID := ids[0], ids[0]
	for _, id := range ids {
		if id < minID {
			minID = id
		}
		if id > maxID {
			maxID = id
		}
	}

	t.Logf("Generated %d unique IDs", len(ids))
	t.Logf("Min ID: %d, Max ID: %d", minID, maxID)
	t.Logf("Range: %d", maxID-minID)
	t.Logf("All unique: %v", len(unique) == count)
}
