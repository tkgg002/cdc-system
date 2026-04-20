package idgen

import (
	"sync"
	"testing"

	"go.uber.org/zap"
)

func TestInit(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	err := Init(logger)
	if err != nil {
		t.Fatalf("Init failed: %v", err)
	}
}

func TestNextID(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	Init(logger)

	id, err := NextID()
	if err != nil {
		t.Fatalf("NextID failed: %v", err)
	}
	if id == 0 {
		t.Fatal("NextID returned 0")
	}
}

func TestNextID_Uniqueness(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	Init(logger)

	const count = 10000
	ids := make(map[uint64]bool, count)
	for i := 0; i < count; i++ {
		id, err := NextID()
		if err != nil {
			t.Fatalf("NextID failed at iteration %d: %v", i, err)
		}
		if ids[id] {
			t.Fatalf("duplicate ID detected: %d at iteration %d", id, i)
		}
		ids[id] = true
	}
}

func TestNextID_Concurrent(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	Init(logger)

	const goroutines = 10
	const perGoroutine = 1000

	var mu sync.Mutex
	ids := make(map[uint64]bool, goroutines*perGoroutine)
	var wg sync.WaitGroup

	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perGoroutine; i++ {
				id, err := NextID()
				if err != nil {
					t.Errorf("NextID failed: %v", err)
					return
				}
				mu.Lock()
				if ids[id] {
					t.Errorf("duplicate ID in concurrent test: %d", id)
				}
				ids[id] = true
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	if len(ids) != goroutines*perGoroutine {
		t.Errorf("expected %d unique IDs, got %d", goroutines*perGoroutine, len(ids))
	}
}

func TestNextID_NotInitialized(t *testing.T) {
	// Reset for this test
	oldSF := sf
	sf = nil
	defer func() { sf = oldSF }()

	_, err := NextID()
	if err == nil {
		t.Fatal("expected error when not initialized, got nil")
	}
}
