package database

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"centralized-data-service/config"

	_ "github.com/jackc/pgx/v5/stdlib"
)

// Phase 01 split E2E (T-C3) — Registry safety contract.
//
// Pure-logic tests (rejection, cache, race-free init) need no live
// database. The pool-identity tests below dial the local 4-PG dev
// stack and skip cleanly if it's not reachable.

func devCfg() *config.AppConfig {
	cfg := &config.AppConfig{}
	cfg.DB.MaxOpenConn = 10
	cfg.DB.MaxIdleConn = 2
	cfg.DB.ConnMaxLifetime = time.Minute
	cfg.ControlPlane.URL = "postgres://gpay_admin:gpay_pass@localhost:5433/cdc_dw?sslmode=disable"
	cfg.MasterDB.DefaultKey = "default"
	cfg.MasterDB.URLs = map[string]string{"default": "postgres://gpay_admin:gpay_pass@localhost:5434/goopay_dest?sslmode=disable"}
	return cfg
}

func liveStackOrSkip(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	r := NewRegistry(devCfg())
	defer r.Close()
	if _, err := r.GetPgxPool(ctx, RoleControlPlane); err != nil {
		t.Skipf("local pg stack not reachable, skipping: %v", err)
	}
}

func TestRegistry_SeparatePoolsPerRole(t *testing.T) {
	liveStackOrSkip(t)
	r := NewRegistry(devCfg())
	defer r.Close()

	cdc, err := r.GetDB(RoleControlPlane)
	if err != nil {
		t.Fatalf("GetDB(cdc): %v", err)
	}
	dest, err := r.GetDB(RoleDestination)
	if err != nil {
		t.Fatalf("GetDB(dest): %v", err)
	}
	if fmt.Sprintf("%p", cdc) == fmt.Sprintf("%p", dest) {
		t.Fatal("control-plane and destination GORM handles must be distinct")
	}
}

func TestRegistry_GetDBIsCached(t *testing.T) {
	liveStackOrSkip(t)
	r := NewRegistry(devCfg())
	defer r.Close()

	first, err := r.GetDB(RoleControlPlane)
	if err != nil {
		t.Fatalf("first GetDB: %v", err)
	}
	second, err := r.GetDB(RoleControlPlane)
	if err != nil {
		t.Fatalf("second GetDB: %v", err)
	}
	if first != second {
		t.Fatal("repeated GetDB(cdc) must return the cached handle")
	}
}

func TestRegistry_ConcurrentGetDBOpensExactlyOnePool(t *testing.T) {
	liveStackOrSkip(t)
	r := NewRegistry(devCfg())
	defer r.Close()

	const N = 32
	var wg sync.WaitGroup
	results := make([]string, N)
	wg.Add(N)
	for i := 0; i < N; i++ {
		i := i
		go func() {
			defer wg.Done()
			db, err := r.GetDB(RoleControlPlane)
			if err != nil {
				t.Errorf("goroutine %d: GetDB: %v", i, err)
				return
			}
			results[i] = fmt.Sprintf("%p", db)
		}()
	}
	wg.Wait()

	first := results[0]
	for i, p := range results {
		if p != first {
			t.Fatalf("goroutine %d saw a different handle (%s vs %s) — registry built a duplicate pool",
				i, p, first)
		}
	}
}

func TestRegistry_GetPgxPoolIsCached(t *testing.T) {
	liveStackOrSkip(t)
	ctx := context.Background()
	r := NewRegistry(devCfg())
	defer r.Close()

	first, err := r.GetPgxPool(ctx, RoleControlPlane)
	if err != nil {
		t.Fatalf("first GetPgxPool: %v", err)
	}
	second, err := r.GetPgxPool(ctx, RoleControlPlane)
	if err != nil {
		t.Fatalf("second GetPgxPool: %v", err)
	}
	if first != second {
		t.Fatal("repeated GetPgxPool(cdc) must return the cached pool")
	}
	dest, err := r.GetPgxPool(ctx, RoleDestination)
	if err != nil {
		t.Fatalf("GetPgxPool(dest): %v", err)
	}
	if dest == first {
		t.Fatal("control-plane and destination pgx pools must be distinct")
	}
}

func TestRegistry_RejectsUnknownRole(t *testing.T) {
	r := NewRegistry(devCfg())
	defer r.Close()
	if _, err := r.GetDB("auth"); err == nil {
		t.Fatal("GetDB(\"auth\") must reject unknown role")
	}
	if _, err := r.GetDB(""); err == nil {
		t.Fatal("GetDB(\"\") must reject empty role")
	}
}
