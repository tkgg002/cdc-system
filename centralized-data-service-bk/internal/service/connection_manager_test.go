package service

import (
	"context"
	"fmt"
	"testing"
	"time"

	"centralized-data-service/config"
	"centralized-data-service/pkgs/database"

	"go.uber.org/zap"

	_ "github.com/jackc/pgx/v5/stdlib"
)

// Phase 01 split E2E (T-C5) — ConnectionManager routing contract.
//
// Scenarios covered (live stack required, otherwise skipped):
//   1. GetShadowDB("")                 → control-plane pool (cdc_dw)
//   2. GetShadowDB("default")          → control-plane pool (cdc_dw)
//   3. GetShadowDB("shadow_local_pg_cdc") with no URL override
//                                      → control-plane pool (fallback)
//   4. GetMasterDB("")                 → destination pool (goopay_dest)
//   5. GetMasterDB("master_local_pg_dest") with no URL override
//                                      → destination pool (fallback)

func cmTestCfg() *config.AppConfig {
	cfg := &config.AppConfig{}
	cfg.DB.MaxOpenConn = 5
	cfg.DB.MaxIdleConn = 1
	cfg.DB.ConnMaxLifetime = time.Minute
	cfg.ControlPlane.URL = "postgres://gpay_admin:gpay_pass@localhost:5433/cdc_dw?sslmode=disable"
	cfg.ShadowDB.DefaultKey = "default"
	cfg.ShadowDB.URLs = map[string]string{"default": cfg.ControlPlane.URL}
	cfg.MasterDB.DefaultKey = "default"
	cfg.MasterDB.URLs = map[string]string{"default": "postgres://gpay_admin:gpay_pass@localhost:5434/goopay_dest?sslmode=disable"}
	return cfg
}

func cmLiveOrSkip(t *testing.T, cm *ConnectionManager) {
	t.Helper()
	ctx := context.Background()
	if _, err := cm.Registry().GetPgxPool(ctx, database.RoleControlPlane); err != nil {
		t.Skipf("local pg stack not reachable, skipping: %v", err)
	}
}

func TestConnectionManager_DefaultKeysHitRegistryPools(t *testing.T) {
	cfg := cmTestCfg()
	cm := NewConnectionManager(cfg, zap.NewNop())
	cmLiveOrSkip(t, cm)

	cdc, err := cm.GetShadowDB(context.Background(), "")
	if err != nil {
		t.Fatalf("GetShadowDB(\"\"): %v", err)
	}
	regCDC, err := cm.Registry().GetDB(database.RoleControlPlane)
	if err != nil {
		t.Fatalf("Registry GetDB(cdc): %v", err)
	}
	if fmt.Sprintf("%p", cdc) != fmt.Sprintf("%p", regCDC) {
		t.Fatal("GetShadowDB(\"\") must reuse the control-plane registry pool")
	}

	defaultCDC, err := cm.GetShadowDB(context.Background(), "default")
	if err != nil {
		t.Fatalf("GetShadowDB(\"default\"): %v", err)
	}
	if fmt.Sprintf("%p", defaultCDC) != fmt.Sprintf("%p", regCDC) {
		t.Fatal("GetShadowDB(\"default\") must reuse the control-plane registry pool")
	}

	dest, err := cm.GetMasterDB(context.Background(), "")
	if err != nil {
		t.Fatalf("GetMasterDB(\"\"): %v", err)
	}
	regDest, err := cm.Registry().GetDB(database.RoleDestination)
	if err != nil {
		t.Fatalf("Registry GetDB(dest): %v", err)
	}
	if fmt.Sprintf("%p", dest) != fmt.Sprintf("%p", regDest) {
		t.Fatal("GetMasterDB(\"\") must reuse the destination registry pool")
	}
}

func TestConnectionManager_UnknownConnectionCodeFallsBackToRegistry(t *testing.T) {
	cfg := cmTestCfg()
	cm := NewConnectionManager(cfg, zap.NewNop())
	cmLiveOrSkip(t, cm)

	// Bootstrap connection_code that has no explicit URL override.
	shadow, err := cm.GetShadowDB(context.Background(), "shadow_local_pg_cdc")
	if err != nil {
		t.Fatalf("GetShadowDB(shadow_local_pg_cdc): %v", err)
	}
	regCDC, _ := cm.Registry().GetDB(database.RoleControlPlane)
	if fmt.Sprintf("%p", shadow) != fmt.Sprintf("%p", regCDC) {
		t.Fatal("unknown shadow connection_code must fall back to control-plane registry pool")
	}

	master, err := cm.GetMasterDB(context.Background(), "master_local_pg_dest")
	if err != nil {
		t.Fatalf("GetMasterDB(master_local_pg_dest): %v", err)
	}
	regDest, _ := cm.Registry().GetDB(database.RoleDestination)
	if fmt.Sprintf("%p", master) != fmt.Sprintf("%p", regDest) {
		t.Fatal("unknown master connection_code must fall back to destination registry pool")
	}
}
