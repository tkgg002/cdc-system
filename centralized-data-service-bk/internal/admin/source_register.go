package admin

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
	"gorm.io/gorm"

	"centralized-data-service/internal/service"
)

// handleRegisterSource — POST /v2/sources/register
// 5 bước: DB insert → Debezium extend → Schema Registry preempt → NATS signal → mark active.
func (s *Server) handleRegisterSource(c *gin.Context) {
	var req RegisterSourceRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// Default source_object_type nếu không truyền
	if req.SourceObjectType == "" {
		req.SourceObjectType = sourceObjectTypeFor(req)
	}
	if req.PrimaryKeyField == "" {
		req.PrimaryKeyField = "_id"
		if req.SourceEngineType == "postgresql" {
			req.PrimaryKeyField = "id"
		}
	}

	var sourceID int64
	var stepsCompleted []string

	// ── Step 1: DB transaction (idempotent) ───────────────────────────────
	if err := s.step1InsertRegistry(c.Request.Context(), req, &sourceID); err != nil {
		s.deps.Logger.Error("step1 registry insert failed", zap.Error(err))
		c.JSON(http.StatusInternalServerError, gin.H{
			"error":  "step1 (registry insert) failed",
			"detail": service.SanitizeFreeformText(err.Error(), 200),
		})
		return
	}
	stepsCompleted = append(stepsCompleted, "registry_insert")

	// ── Step 2: Debezium include list extend ─────────────────────────────
	var respWarnings []string
	extResult, err := s.extendDebeziumInclude(c.Request.Context(), req)
	if err != nil {
		s.deps.Logger.Warn("step2 debezium extend failed", zap.Error(err))
		s.markProvisioningFailed(sourceID, "step2_failed", err)
		c.JSON(http.StatusMultiStatus, RegisterSourceResponse{
			SourceObjectID:    sourceID,
			ProvisioningState: "step2_failed",
			StepsCompleted:    stepsCompleted,
			LastStepError:     service.SanitizeFreeformText(err.Error(), 200),
		})
		return
	}
	if extResult.DatabaseTierAdded {
		db := stringFromLocator(req.SourceLocator, "database")
		respWarnings = append(respWarnings, fmt.Sprintf(
			"database '%s' was just added to debezium include — first event from new namespace may be delayed; connector task may need a moment to snapshot",
			db))
	}
	stepsCompleted = append(stepsCompleted, "debezium_include_extend")

	// ── Step 3: Schema Registry compat=NONE preempt ───────────────────────
	if err := s.preemptSchemaRegistry(c.Request.Context(), req); err != nil {
		s.deps.Logger.Warn("step3 schema registry preempt failed", zap.Error(err))
		s.markProvisioningFailed(sourceID, "step3_failed", err)
		c.JSON(http.StatusMultiStatus, RegisterSourceResponse{
			SourceObjectID:    sourceID,
			ProvisioningState: "step3_failed",
			StepsCompleted:    stepsCompleted,
			LastStepError:     service.SanitizeFreeformText(err.Error(), 200),
		})
		return
	}
	stepsCompleted = append(stepsCompleted, "schema_registry_preempt")

	// ── Step 4: NATS publish refresh-topics (non-fatal) ───────────────────
	if err := s.deps.NATS.Publish("cdc.cmd.kafka.refresh-topics", []byte("{}")); err != nil {
		s.deps.Logger.Warn("step4 nats publish refresh-topics failed (non-fatal)", zap.Error(err))
	}
	stepsCompleted = append(stepsCompleted, "worker_signal")

	// ── Step 5: mark provisioning_state=active ────────────────────────────
	s.deps.DB.Exec(`UPDATE cdc_system.source_object_registry
	                SET provisioning_state = 'active',
	                    last_step_error    = NULL,
	                    updated_at         = NOW()
	                WHERE id = ?`, sourceID)

	c.JSON(http.StatusOK, RegisterSourceResponse{
		SourceObjectID:    sourceID,
		ProvisioningState: "active",
		StepsCompleted:    stepsCompleted,
		Warnings:          respWarnings,
	})
}

// step1InsertRegistry — transactional INSERT source_object_registry + shadow_binding.
// Idempotent via ON CONFLICT (object_code) và ON CONFLICT (source_object_id, shadow_connection_id, shadow_schema, shadow_table).
func (s *Server) step1InsertRegistry(ctx context.Context, req RegisterSourceRequest, sourceID *int64) error {
	return s.deps.DB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		// 1a. Resolve source connection_id (lookup by engine_type + role_type=source)
		var srcConnID int64
		if err := tx.Raw(`SELECT id FROM cdc_system.connection_registry
		                  WHERE engine_type = ? AND role_type = 'source' AND status = 'active'
		                  ORDER BY id LIMIT 1`, req.SourceEngineType).
			Scan(&srcConnID).Error; err != nil {
			return fmt.Errorf("lookup source connection: %w", err)
		}
		if srcConnID == 0 {
			return fmt.Errorf("no active source connection for engine %q in connection_registry", req.SourceEngineType)
		}

		// 1b. Resolve shadow connection_id (postgresql shadow)
		var shadowConnID int64
		if err := tx.Raw(`SELECT id FROM cdc_system.connection_registry
		                  WHERE role_type = 'shadow' AND engine_type = 'postgresql' AND status = 'active'
		                  ORDER BY id LIMIT 1`).
			Scan(&shadowConnID).Error; err != nil {
			return fmt.Errorf("lookup shadow connection: %w", err)
		}
		if shadowConnID == 0 {
			return fmt.Errorf("no active shadow postgresql connection in connection_registry")
		}

		// 1c. INSERT source_object_registry (idempotent on object_code)
		locatorJSON, _ := json.Marshal(req.SourceLocator)
		normalizedKey := normalizedSourceKeyFor(req)
		db := stringFromLocator(req.SourceLocator, "database")

		if err := tx.Raw(`
			INSERT INTO cdc_system.source_object_registry
			    (object_code, source_connection_id, source_engine_type, sync_engine,
			     source_database, source_namespace, source_object_name, source_object_type,
			     source_locator_json, normalized_source_key, primary_key_field,
			     is_active, provisioning_mode, provisioning_state, notes,
			     created_at, updated_at)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?::jsonb, ?, ?, true, 'auto', 'pending', ?, NOW(), NOW())
			ON CONFLICT (object_code) DO UPDATE SET
			    source_locator_json   = EXCLUDED.source_locator_json,
			    provisioning_state    = 'pending',
			    last_step_error       = NULL,
			    updated_at            = NOW()
			RETURNING id`,
			req.ObjectCode, srcConnID, req.SourceEngineType, req.SyncEngine,
			db, db, req.SourceObjectName, req.SourceObjectType,
			string(locatorJSON), normalizedKey, req.PrimaryKeyField,
			req.Notes).
			Scan(sourceID).Error; err != nil {
			return fmt.Errorf("insert source_object_registry: %w", err)
		}
		if *sourceID == 0 {
			// ON CONFLICT UPDATE không trả RETURNING — fallback lookup
			if err := tx.Raw(`SELECT id FROM cdc_system.source_object_registry WHERE object_code = ?`,
				req.ObjectCode).Scan(sourceID).Error; err != nil {
				return fmt.Errorf("lookup source_object_id after conflict: %w", err)
			}
		}

		// 1d. INSERT shadow_binding (idempotent via UNIQUE (source_object_id, shadow_connection_id, shadow_schema, shadow_table))
		shadowSchema := shadowSchemaFor(req)
		shadowTable := req.SourceObjectName
		bindingCode := fmt.Sprintf("auto_%s_shadow", req.ObjectCode)
		physicalFQN := fmt.Sprintf("%s.%s", shadowSchema, shadowTable)

		if err := tx.Exec(`
			INSERT INTO cdc_system.shadow_binding
			    (binding_code, source_object_id, shadow_connection_id,
			     shadow_schema, shadow_table, physical_table_fqn,
			     namespace_strategy, write_mode, ddl_status, is_active,
			     created_at, updated_at)
			VALUES (?, ?, ?, ?, ?, ?, 'preserve', 'upsert', 'pending', true, NOW(), NOW())
			ON CONFLICT (source_object_id, shadow_connection_id, shadow_schema, shadow_table) DO UPDATE SET
			    is_active  = true,
			    ddl_status = 'pending',
			    updated_at = NOW()`,
			bindingCode, *sourceID, shadowConnID,
			shadowSchema, shadowTable, physicalFQN).Error; err != nil {
			return fmt.Errorf("insert shadow_binding: %w", err)
		}

		return nil
	})
}

// markProvisioningFailed — update provisioning_state + last_step_error.
func (s *Server) markProvisioningFailed(sourceID int64, step string, err error) {
	s.deps.DB.Exec(`UPDATE cdc_system.source_object_registry
	                SET provisioning_state = ?,
	                    last_step_error    = ?,
	                    updated_at         = NOW()
	                WHERE id = ?`, step, service.SanitizeFreeformText(err.Error(), 2000), sourceID)
}
