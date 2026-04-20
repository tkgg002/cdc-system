package service

import (
	"centralized-data-service/internal/model"

	"go.uber.org/zap"
)

// EnrichmentService handles computed fields and business logic enrichment.
// Processes fields flagged IsEnriched in mapping rules.
type EnrichmentService struct {
	logger *zap.Logger
}

func NewEnrichmentService(logger *zap.Logger) *EnrichmentService {
	return &EnrichmentService{logger: logger}
}

// Enrich applies enrichment functions to enriched fields from MappedData.
// Returns additional columns to merge into the upsert record.
func (es *EnrichmentService) Enrich(targetTable string, enrichedData map[string]interface{}, rules []model.MappingRule) map[string]interface{} {
	result := make(map[string]interface{})

	for _, rule := range rules {
		if !rule.IsEnriched || rule.EnrichmentFunction == nil {
			continue
		}

		val, exists := enrichedData[rule.TargetColumn]
		if !exists {
			continue
		}

		fn := *rule.EnrichmentFunction
		enriched := es.applyFunction(fn, val, enrichedData)
		if enriched != nil {
			result[rule.TargetColumn] = enriched
		}
	}

	return result
}

// applyFunction executes an enrichment function by name
func (es *EnrichmentService) applyFunction(funcName string, value interface{}, context map[string]interface{}) interface{} {
	switch funcName {
	case "uppercase":
		if s, ok := value.(string); ok {
			return s // TODO: strings.ToUpper
		}
	case "calculate_balance":
		// Example: balance_after = balance_before + amount
		// TODO: implement business logic
	default:
		es.logger.Debug("unknown enrichment function", zap.String("function", funcName))
	}
	return value
}
