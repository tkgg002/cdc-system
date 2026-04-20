package service

import (
	"testing"

	"centralized-data-service/internal/model"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

func TestRegistryService_GetMappingRules(t *testing.T) {
	logger := zap.NewNop()
	
	// Setup mock data
	entries := []model.TableRegistry{
		{SourceTable: "users_mongo", TargetTable: "cdc_users"},
		{SourceTable: "orders_mongo", TargetTable: "cdc_orders"},
	}
	
	rules := []model.MappingRule{
		{SourceTable: "users_mongo", SourceField: "name", TargetColumn: "full_name", IsActive: true},
		{SourceTable: "users_mongo", SourceField: "email", TargetColumn: "email", IsActive: true},
		{SourceTable: "orders_mongo", SourceField: "total", TargetColumn: "amount", IsActive: true},
	}

	// We'll use a real RegistryService with mock-ish repos if we had them, 
	// but let's just test the ReloadAll logic by manually populating if needed or mocking Repo.
	// For simplicity in this environment, I'll assume we can mock or I'll just test the core logic.
	
	rs := &RegistryService{
		logger:        logger,
		registryCache: make(map[string]*model.TableRegistry),
		mappingCache:  make(map[string][]model.MappingRule),
	}

	// Manually trigger the logic that was in ReloadAll but with fixed data
	rs.mu.Lock()
	rs.registryCache = make(map[string]*model.TableRegistry, len(entries))
	sourceToTarget := make(map[string]string, len(entries))
	for i := range entries {
		rs.registryCache[entries[i].TargetTable] = &entries[i]
		sourceToTarget[entries[i].SourceTable] = entries[i].TargetTable
	}

	rs.mappingCache = make(map[string][]model.MappingRule)
	for _, r := range rules {
		targetTable := sourceToTarget[r.SourceTable]
		if targetTable != "" {
			rs.mappingCache[targetTable] = append(rs.mappingCache[targetTable], r)
		}
	}
	rs.mu.Unlock()

	// Test case: Query by TargetTable
	userRules := rs.GetMappingRules("cdc_users")
	assert.Len(t, userRules, 2)
	assert.Equal(t, "full_name", userRules[0].TargetColumn)

	orderRules := rs.GetMappingRules("cdc_orders")
	assert.Len(t, orderRules, 1)
	assert.Equal(t, "amount", orderRules[0].TargetColumn)

	// Test case: Query non-existent
	none := rs.GetMappingRules("unknown")
	assert.Nil(t, none)
}
