package service

import (
	"testing"

	"centralized-data-service/internal/model"
)

// B3 fan-out — TestResolveSourceRoutes_FanOut.
// Master source X has two logical clones Y, Z.
// ResolveSourceRoutes(masterDB, masterTable) must return 3 routes: master + 2 clones.
func TestResolveSourceRoutes_FanOut(t *testing.T) {
	sourceDB := "goopay_source"
	masterSchema := "public"

	masterSrc := &model.SourceObjectRegistry{
		ID:               11,
		SourceDatabase:   &sourceDB,
		SourceSchema:     &masterSchema,
		SourceObjectName: "orders",
		SourceEngineType: "postgresql",
		PrimaryKeyField:  "id",
		SyncEngine:       "debezium",
		IsActive:         true,
		ProfileStatus:    "active",
	}
	cloneY := &model.SourceObjectRegistry{
		ID:                 29,
		SourceDatabase:     &sourceDB,
		SourceSchema:       &masterSchema,
		SourceObjectName:   "orders",
		SourceEngineType:   "postgresql",
		PrimaryKeyField:    "id",
		SyncEngine:         "debezium",
		IsActive:           true,
		ProfileStatus:      "active",
		SourceLocatorJSON:  []byte(`{"logical_clone_of":11,"fan_out_role":"clone"}`),
	}
	cloneZ := &model.SourceObjectRegistry{
		ID:                 30,
		SourceDatabase:     &sourceDB,
		SourceSchema:       &masterSchema,
		SourceObjectName:   "orders",
		SourceEngineType:   "postgresql",
		PrimaryKeyField:    "id",
		SyncEngine:         "debezium",
		IsActive:           true,
		ProfileStatus:      "active",
		SourceLocatorJSON:  []byte(`{"logical_clone_of":11,"fan_out_role":"clone"}`),
	}

	masterBinding := &model.ShadowBinding{ID: 100, BindingCode: "b_master", ShadowTable: "shadow_orders", DDLStatus: "created", IsActive: true}
	cloneYBinding := &model.ShadowBinding{ID: 101, BindingCode: "b_clone_y", ShadowTable: "shadow_orders_addtest", DDLStatus: "created", IsActive: true}
	cloneZBinding := &model.ShadowBinding{ID: 102, BindingCode: "b_clone_z", ShadowTable: "shadow_orders_addtest2", DDLStatus: "created", IsActive: true}

	masterCfg := synthesizeLegacyTableRegistry(masterSrc, masterBinding)
	cloneYCfg := synthesizeLegacyTableRegistry(cloneY, cloneYBinding)
	cloneZCfg := synthesizeLegacyTableRegistry(cloneZ, cloneZBinding)

	masterRoute := &ResolvedSourceRoute{SourceObject: masterSrc, ShadowBinding: masterBinding, TableConfig: masterCfg}
	cloneYRoute := &ResolvedSourceRoute{SourceObject: cloneY, ShadowBinding: cloneYBinding, TableConfig: cloneYCfg}
	cloneZRoute := &ResolvedSourceRoute{SourceObject: cloneZ, ShadowBinding: cloneZBinding, TableConfig: cloneZCfg}

	rs := &MetadataRegistryService{
		sourceCache: map[string]*model.TableRegistry{
			"orders":                    masterCfg,
			"goopay_source|orders":      masterCfg,
			"goopay_source|public.orders": masterCfg,
		},
		routeCache: map[string]*ResolvedSourceRoute{
			"orders":                    masterRoute,
			"goopay_source|orders":      masterRoute,
			"goopay_source|public.orders": masterRoute,
		},
		targetRouteMap: map[string]*ResolvedSourceRoute{
			masterCfg.TargetTable: masterRoute,
		},
		mappingCache: make(map[string][]model.MappingRule),
		targetCache:  map[string]*model.TableRegistry{masterCfg.TargetTable: masterCfg},
		cloneRoutes:  map[int64][]*ResolvedSourceRoute{11: {cloneYRoute, cloneZRoute}},
	}

	// ResolveSourceRoute (single) must still return master only.
	single := rs.ResolveSourceRoute("goopay_source", "orders")
	if single == nil {
		t.Fatal("single route: expected master route, got nil")
	}
	if single.TableConfig.TargetTable != "shadow_orders" {
		t.Fatalf("single route target = %q, want shadow_orders", single.TableConfig.TargetTable)
	}

	// ResolveSourceRoutes must return 3 routes: master + clone Y + clone Z.
	multi := rs.ResolveSourceRoutes("goopay_source", "orders")
	if len(multi) != 3 {
		t.Fatalf("fan-out routes len = %d, want 3", len(multi))
	}
	targetTables := map[string]bool{}
	for _, r := range multi {
		targetTables[r.TableConfig.TargetTable] = true
	}
	for _, want := range []string{"shadow_orders", "shadow_orders_addtest", "shadow_orders_addtest2"} {
		if !targetTables[want] {
			t.Errorf("missing route for target table %q in fan-out result %v", want, targetTables)
		}
	}
}

func TestMetadataRegistryServiceResolveSourceRoute(t *testing.T) {
	sourceDB := "wallet"
	sourceSchema := "public"
	primaryKeyType := "VARCHAR(24)"
	timestampField := "updated_at"

	src := &model.SourceObjectRegistry{
		ID:               1,
		SourceDatabase:   &sourceDB,
		SourceSchema:     &sourceSchema,
		SourceObjectName: "transactions",
		SourceEngineType: "mongodb",
		PrimaryKeyField:  "_id",
		PrimaryKeyType:   &primaryKeyType,
		TimestampField:   &timestampField,
		SyncEngine:       "debezium",
		IsActive:         true,
		ProfileStatus:    "active",
	}
	binding := &model.ShadowBinding{
		ID:          9,
		BindingCode: "shadow_wallet_tx",
		ShadowTable: "wallet_transactions_shadow",
		DDLStatus:   "created",
		IsActive:    true,
	}
	cfg := synthesizeLegacyTableRegistry(src, binding)
	route := &ResolvedSourceRoute{
		SourceObject:  src,
		ShadowBinding: binding,
		TableConfig:   cfg,
	}

	rs := &MetadataRegistryService{
		sourceCache: map[string]*model.TableRegistry{
			"wallet|transactions":        cfg,
			"wallet|public.transactions": cfg,
		},
		routeCache: map[string]*ResolvedSourceRoute{
			"wallet|transactions":        route,
			"wallet|public.transactions": route,
		},
		targetRouteMap: map[string]*ResolvedSourceRoute{
			cfg.TargetTable: route,
		},
		mappingCache: make(map[string][]model.MappingRule),
		targetCache:  map[string]*model.TableRegistry{cfg.TargetTable: cfg},
	}

	got := rs.ResolveSourceRoute("wallet", "transactions")
	if got == nil {
		t.Fatal("expected route, got nil")
	}
	if got.TableConfig.TargetTable != "wallet_transactions_shadow" {
		t.Fatalf("unexpected target table: %s", got.TableConfig.TargetTable)
	}
	if got.TableConfig.PrimaryKeyField != "_id" {
		t.Fatalf("unexpected pk field: %s", got.TableConfig.PrimaryKeyField)
	}

	gotQualified := rs.ResolveSourceRoute("wallet", "public.transactions")
	if gotQualified == nil {
		t.Fatal("expected qualified route, got nil")
	}
	if gotQualified.ShadowBinding.BindingCode != "shadow_wallet_tx" {
		t.Fatalf("unexpected binding code: %s", gotQualified.ShadowBinding.BindingCode)
	}

	gotTarget := rs.ResolveTargetRoute(cfg.TargetTable)
	if gotTarget == nil {
		t.Fatal("expected target route, got nil")
	}
	if gotTarget.ShadowBinding.BindingCode != "shadow_wallet_tx" {
		t.Fatalf("unexpected target binding code: %s", gotTarget.ShadowBinding.BindingCode)
	}
}
