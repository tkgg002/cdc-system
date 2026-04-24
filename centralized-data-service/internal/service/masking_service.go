package service

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync"

	"go.uber.org/zap"
	"gorm.io/gorm"
)

var defaultSensitiveKeywords = []string{
	"phone",
	"email",
	"secret",
	"password",
	"token",
	"balance",
	"otp",
	"pin",
	"card",
	"account",
	"address",
	"ssn",
}

type MaskingService struct {
	db              *gorm.DB
	logger          *zap.Logger
	defaultMasks    []string
	sensitiveFields sync.Map // table -> map[string]struct{}
}

func NewMaskingService(db *gorm.DB, logger *zap.Logger, defaults ...string) *MaskingService {
	maskDefaults := append([]string{}, defaultSensitiveKeywords...)
	if len(defaults) > 0 {
		maskDefaults = normalizeMaskKeys(defaults)
	}
	return &MaskingService{
		db:           db,
		logger:       logger,
		defaultMasks: maskDefaults,
	}
}

func (ms *MaskingService) Invalidate(table string) {
	if table == "" {
		ms.sensitiveFields = sync.Map{}
		return
	}
	ms.sensitiveFields.Delete(table)
}

func (ms *MaskingService) MaskTableData(table string, data map[string]interface{}) map[string]interface{} {
	if len(data) == 0 {
		return data
	}
	mask := ms.resolveMaskSet(table)
	if len(mask) == 0 {
		return cloneAnyMap(data)
	}
	return ms.maskMapRecursive(table, data, mask)
}

func (ms *MaskingService) MaskJSONPayload(table string, data []byte) json.RawMessage {
	if len(data) == 0 {
		return nil
	}
	if !json.Valid(data) {
		wrapped, _ := json.Marshal(map[string]string{"raw": "***"})
		return json.RawMessage(wrapped)
	}

	var parsed interface{}
	if err := json.Unmarshal(data, &parsed); err != nil {
		wrapped, _ := json.Marshal(map[string]string{"raw": "***"})
		return json.RawMessage(wrapped)
	}
	masked := ms.maskAnyRecursive(table, parsed, ms.resolveMaskSet(table), "")
	out, err := json.Marshal(masked)
	if err != nil {
		ms.logWarn("marshal masked payload failed", zap.String("table", table), zap.Error(err))
		return json.RawMessage(data)
	}
	return json.RawMessage(out)
}

func (ms *MaskingService) MaskFieldSample(table, field string, value interface{}) interface{} {
	if ms.shouldMaskField(table, field, ms.resolveMaskSet(table)) {
		return "***"
	}
	return ms.maskAnyRecursive(table, value, ms.resolveMaskSet(table), field)
}

func (ms *MaskingService) resolveMaskSet(table string) map[string]struct{} {
	cacheKey := strings.ToLower(strings.TrimSpace(table))
	if cached, ok := ms.sensitiveFields.Load(cacheKey); ok {
		return cached.(map[string]struct{})
	}

	mask := make(map[string]struct{}, len(ms.defaultMasks))
	for _, item := range ms.defaultMasks {
		mask[item] = struct{}{}
	}

	if ms.db != nil && cacheKey != "" {
		var raw string
		err := ms.db.Raw(
			`SELECT sensitive_fields::text FROM cdc_table_registry WHERE target_table = ? LIMIT 1`,
			table,
		).Scan(&raw).Error
		if err == nil && raw != "" {
			var fields []string
			if err := json.Unmarshal([]byte(raw), &fields); err == nil {
				for _, field := range normalizeMaskKeys(fields) {
					mask[field] = struct{}{}
				}
			} else {
				ms.logWarn("parse sensitive_fields failed", zap.String("table", table), zap.Error(err))
			}
		}
	}

	ms.sensitiveFields.Store(cacheKey, mask)
	return mask
}

func (ms *MaskingService) maskMapRecursive(table string, data map[string]interface{}, mask map[string]struct{}) map[string]interface{} {
	out := make(map[string]interface{}, len(data))
	for key, value := range data {
		if ms.shouldMaskField(table, key, mask) {
			out[key] = "***"
			continue
		}
		out[key] = ms.maskAnyRecursive(table, value, mask, key)
	}
	return out
}

func (ms *MaskingService) maskAnyRecursive(table string, value interface{}, mask map[string]struct{}, currentField string) interface{} {
	switch typed := value.(type) {
	case map[string]interface{}:
		return ms.maskMapRecursive(table, typed, mask)
	case []interface{}:
		out := make([]interface{}, len(typed))
		for i, item := range typed {
			out[i] = ms.maskAnyRecursive(table, item, mask, currentField)
		}
		return out
	default:
		if currentField != "" && ms.shouldMaskField(table, currentField, mask) {
			return "***"
		}
		return value
	}
}

func (ms *MaskingService) shouldMaskField(table, field string, mask map[string]struct{}) bool {
	normalized := strings.ToLower(strings.TrimSpace(field))
	if normalized == "" {
		return false
	}
	if _, ok := mask[normalized]; ok {
		return true
	}
	for keyword := range mask {
		if keyword != "" && strings.Contains(normalized, keyword) {
			return true
		}
	}
	for _, keyword := range ms.defaultMasks {
		if keyword != "" && strings.Contains(normalized, keyword) {
			return true
		}
	}
	_ = table
	return false
}

func cloneAnyMap(in map[string]interface{}) map[string]interface{} {
	out := make(map[string]interface{}, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

func normalizeMaskKeys(items []string) []string {
	seen := make(map[string]struct{}, len(items))
	out := make([]string, 0, len(items))
	for _, item := range items {
		normalized := strings.ToLower(strings.TrimSpace(item))
		if normalized == "" {
			continue
		}
		if _, ok := seen[normalized]; ok {
			continue
		}
		seen[normalized] = struct{}{}
		out = append(out, normalized)
	}
	return out
}

func (ms *MaskingService) logWarn(msg string, fields ...zap.Field) {
	if ms.logger != nil {
		ms.logger.Warn(msg, fields...)
	}
}

func (ms *MaskingService) String() string {
	return fmt.Sprintf("MaskingService(default_masks=%d)", len(ms.defaultMasks))
}
