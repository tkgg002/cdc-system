package service

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"cdc-cms-service/internal/model"
	"cdc-cms-service/internal/repository"
	"cdc-cms-service/pkgs/airbyte"

	"go.uber.org/zap"
	"gorm.io/gorm"
)

type ReconciliationService struct {
	repo          *repository.RegistryRepo
	mappingRepo   *repository.MappingRuleRepo
	airbyteClient *airbyte.Client
	db            *gorm.DB
	logger        *zap.Logger
	interval      time.Duration
	stopCh        chan struct{}
}

func NewReconciliationService(
	repo *repository.RegistryRepo,
	mappingRepo *repository.MappingRuleRepo,
	airbyteClient *airbyte.Client,
	db *gorm.DB,
	logger *zap.Logger,
	interval time.Duration,
) *ReconciliationService {
	if interval == 0 {
		interval = 5 * time.Minute
	}
	return &ReconciliationService{
		repo:          repo,
		mappingRepo:   mappingRepo,
		airbyteClient: airbyteClient,
		db:            db,
		logger:        logger,
		interval:      interval,
		stopCh:        make(chan struct{}),
	}
}

func (s *ReconciliationService) Start(ctx context.Context) {
	s.logger.Info("Starting Airbyte Reconciliation Worker", zap.Duration("interval", s.interval))
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()

	s.reconcile(ctx)

	for {
		select {
		case <-ticker.C:
			s.reconcile(ctx)
		case <-ctx.Done():
			return
		case <-s.stopCh:
			return
		}
	}
}

func (s *ReconciliationService) Stop() {
	close(s.stopCh)
}

// logActivity writes to cdc_activity_log
func (s *ReconciliationService) logActivity(operation, targetTable, status, triggeredBy string, rowsAffected int64, details map[string]interface{}, errMsg string) {
	detailsJSON, _ := json.Marshal(details)
	now := time.Now()
	var errPtr *string
	if errMsg != "" {
		errPtr = &errMsg
	}
	dur := 0
	entry := &model.ActivityLog{
		Operation:    operation,
		TargetTable:  targetTable,
		Status:       status,
		RowsAffected: rowsAffected,
		DurationMs:   &dur,
		Details:      detailsJSON,
		ErrorMessage: errPtr,
		TriggeredBy:  triggeredBy,
		StartedAt:    now,
		CompletedAt:  &now,
	}
	s.db.Create(entry)
}

func (s *ReconciliationService) reconcile(ctx context.Context) {
	// Check if airbyte-sync schedule is enabled
	var schedule model.WorkerSchedule
	if err := s.db.Where("operation = ? AND target_table IS NULL", "airbyte-sync").First(&schedule).Error; err == nil {
		if !schedule.IsEnabled {
			s.logger.Debug("airbyte-sync schedule is disabled, skipping reconciliation")
			return
		}
	}

	s.logger.Info("Reconciliation cycle started: scanning Airbyte streams + checking mismatches...")
	startTime := time.Now()

	// === PHASE 1: Auto-discover new streams from Airbyte ===
	conns, err := s.airbyteClient.ListConnections(ctx, s.airbyteClient.GetWorkspaceID())
	if err != nil {
		s.logActivity("scan-airbyte-streams", "*", "error", "scheduler", 0, nil, "list connections: "+err.Error())
		return
	}

	// Get all sources + destinations for metadata
	sources, _ := s.airbyteClient.ListSources(ctx)
	sourceMap := make(map[string]airbyte.Source)
	for _, src := range sources {
		sourceMap[src.SourceID] = src
	}

	destinations, _ := s.airbyteClient.ListDestinations(ctx)
	destMap := make(map[string]airbyte.Destination)
	for _, d := range destinations {
		destMap[d.DestinationID] = d
	}

	// Get existing registry entries
	entries, _, _ := s.repo.GetAll(ctx, repository.RegistryFilter{PageSize: 1000})
	existingTables := make(map[string]bool)
	activeMap := make(map[string]bool)
	connIDs := make(map[string]bool)
	for _, e := range entries {
		existingTables[e.SourceTable] = true
		activeMap[e.SourceTable] = e.IsActive
		if e.AirbyteConnectionID != nil && *e.AirbyteConnectionID != "" {
			connIDs[*e.AirbyteConnectionID] = true
		}
	}

	totalStreams := 0
	activeStreams := 0
	nonActiveStreams := 0
	newStreams := 0
	newMappingRules := 0

	// Build selected map + alias map from ListConnections
	selectedMap := make(map[string]bool)
	aliasMap := make(map[string]string)    // stream name → airbyte table alias
	connMap := make(map[string]string)     // sourceID → connectionID
	for _, conn := range conns {
		connMap[conn.SourceID] = conn.ConnectionID
		for _, sc := range conn.SyncCatalog.Streams {
			selectedMap[sc.Stream.Name] = sc.Config.Selected
			if sc.Config.AliasName != "" {
				aliasMap[sc.Stream.Name] = sc.Config.AliasName
			} else {
				aliasMap[sc.Stream.Name] = strings.ReplaceAll(sc.Stream.Name, "-", "_")
			}
		}
	}

	// DiscoverSchema per source to get ALL streams (including non-selected + full JSONSchema)
	discoveredSources := make(map[string]bool)
	for _, connSummary := range conns {
		if discoveredSources[connSummary.SourceID] {
			continue
		}
		discoveredSources[connSummary.SourceID] = true

		src := sourceMap[connSummary.SourceID]
		sourceDB := src.Database
		if sourceDB == "" {
			sourceDB = src.Name
		}
		sourceType := "mongodb"
		if strings.Contains(strings.ToLower(src.SourceName), "mysql") {
			sourceType = "mysql"
		} else if strings.Contains(strings.ToLower(src.SourceName), "postgres") {
			sourceType = "postgresql"
		}

		// DiscoverSchema = ALL available streams from source (verified: returns 8 including non-selected)
		catalog, err := s.airbyteClient.DiscoverSchema(ctx, connSummary.SourceID)
		if err != nil {
			s.logActivity("discover-schema", src.Name, "error", "scheduler", 0, nil, err.Error())
			continue
		}

		for _, sc := range catalog.Streams {
			totalStreams++
			isSelected := selectedMap[sc.Stream.Name] // from ListConnections
			if isSelected {
				activeStreams++
			} else {
				nonActiveStreams++
			}

			streamStatus := "new"
			if existingTables[sc.Stream.Name] {
				streamStatus = "existing"
			}
			s.logActivity("scan-stream-detail", sc.Stream.Name, "success", "scheduler", 0, map[string]interface{}{
				"stream_status": streamStatus,
				"selected":      isSelected,
				"has_schema":    sc.Stream.JsonSchema != nil,
				"source_db":     sourceDB,
			}, "")

			if existingTables[sc.Stream.Name] {
				continue
			}

			// Auto-register — both active and non-active
			connID := connSummary.ConnectionID
			sourceID := connSummary.SourceID
			// Use Airbyte alias as raw table name (Airbyte replaces - with _ in actual table)
			rawTable := aliasMap[sc.Stream.Name]
			if rawTable == "" {
				rawTable = strings.ReplaceAll(sc.Stream.Name, "-", "_")
			}

			// DiscoverSchema streams don't have Config — use defaults
			syncMode := "incremental"
			destMode := "append"
			namespace := sc.Stream.Namespace
			cursorField := ""
			if len(sc.Stream.DefaultCursorField) > 0 {
				cursorField = sc.Stream.DefaultCursorField[0]
			}

			pkField := "id"
			if len(sc.Stream.SourceDefinedPrimaryKey) > 0 && len(sc.Stream.SourceDefinedPrimaryKey[0]) > 0 {
				pkField = sc.Stream.SourceDefinedPrimaryKey[0][0]
			}

			// Destination metadata
			destID := connSummary.DestID
			dest := destMap[destID]
			destName := dest.DestinationName
			if destName == "" {
				destName = dest.Name
			}

			entry := model.TableRegistry{
				SourceDB:                 sourceDB,
				SourceType:               sourceType,
				SourceTable:              sc.Stream.Name,
				TargetTable:              rawTable,
				SyncEngine:               "airbyte",
				Priority:                 "normal",
				PrimaryKeyField:          pkField,
				PrimaryKeyType:           "BIGINT",
				IsActive:                 isSelected,
				AirbyteConnectionID:      &connID,
				AirbyteSourceID:          &sourceID,
				AirbyteDestinationID:     &destID,
				AirbyteDestinationName:   &destName,
				AirbyteRawTable:          &rawTable,
				AirbyteSyncMode:          &syncMode,
				AirbyteDestinationSync:   &destMode,
				AirbyteNamespace:         &namespace,
				AirbyteCursorField:       &cursorField,
			}

			if err := s.repo.Create(ctx, &entry); err != nil {
				s.logger.Warn("auto-register stream failed", zap.String("stream", sc.Stream.Name), zap.Error(err))
				s.logActivity("auto-register-stream", sc.Stream.Name, "error", "scheduler", 0, nil, err.Error())
				continue
			}
			newStreams++

			// Auto-create mapping rules from JSONSchema (approved for active, pending for non-active)
			ruleStatus := "approved"
			if !isSelected {
				ruleStatus = "pending"
			}
			if sc.Stream.JsonSchema != nil {
				added := s.createMappingRulesFromSchemaWithStatus(ctx, sc.Stream.Name, sc.Stream.JsonSchema, ruleStatus)
				newMappingRules += added
				s.logActivity("auto-create-mapping-rules", sc.Stream.Name, "success", "scheduler", int64(added), map[string]interface{}{
					"fields_parsed": added,
					"rule_status":   ruleStatus,
				}, "")
			} else {
				s.logActivity("auto-create-mapping-rules", sc.Stream.Name, "skipped", "scheduler", 0, map[string]interface{}{
					"reason": "jsonSchema is nil from DiscoverSchema",
				}, "")
			}

			s.logActivity("auto-register-stream", sc.Stream.Name, "success", "scheduler", 1, map[string]interface{}{
				"source_db":   sourceDB,
				"source_type": sourceType,
				"is_active":   isSelected,
				"pk_field":    pkField,
				"sync_mode":   syncMode,
			}, "")
		}
	}

	// === PHASE 2: Check mismatches (existing logic) ===
	mismatches := 0
	for connID := range connIDs {
		conn, err := s.airbyteClient.GetConnection(ctx, connID)
		if err != nil {
			continue
		}

		registryTables := make([]string, 0)
		for t := range existingTables {
			registryTables = append(registryTables, t)
		}

		audits := airbyte.CompareCatalog(conn, registryTables, activeMap)
		for _, audit := range audits {
			if audit.Mismatch == airbyte.MismatchStatus {
				mismatches++
				s.repo.UpdateActiveStatusByTable(ctx, audit.StreamName, audit.AirbyteStatus)
				s.logActivity("auto-heal-mismatch", audit.StreamName, "success", "scheduler", 1, map[string]interface{}{
					"airbyte_selected": audit.AirbyteStatus,
					"cms_active":       audit.SystemStatus,
				}, "")
			}
		}
	}

	duration := time.Since(startTime)
	s.logActivity("scan-airbyte-streams", "*", "success", "scheduler", int64(newStreams), map[string]interface{}{
		"total_streams":      totalStreams,
		"active_streams":     activeStreams,
		"non_active_streams": nonActiveStreams,
		"new_registered":     newStreams,
		"new_mapping_rules":  newMappingRules,
		"mismatches_healed":  mismatches,
		"duration_ms":        duration.Milliseconds(),
	}, "")

	s.logger.Info("Reconciliation cycle completed",
		zap.Int("total_streams", totalStreams),
		zap.Int("active_streams", activeStreams),
		zap.Int("new_registered", newStreams),
		zap.Int("new_mapping_rules", newMappingRules),
		zap.Int("mismatches", mismatches),
		zap.Duration("duration", duration),
	)
}

func (s *ReconciliationService) createMappingRulesFromSchemaWithStatus(ctx context.Context, sourceTable string, jsonSchema interface{}, status string) int {
	schemaMap, ok := jsonSchema.(map[string]interface{})
	if !ok {
		return 0
	}
	properties, ok := schemaMap["properties"].(map[string]interface{})
	if !ok {
		return 0
	}

	added := 0
	for fieldName, fieldDef := range properties {
		if strings.HasPrefix(fieldName, "_airbyte_") {
			continue
		}
		dataType := inferDataType(fieldDef)
		isActive := status == "approved"
		rule := &model.MappingRule{
			SourceTable:  sourceTable,
			SourceField:  fieldName,
			TargetColumn: strings.ReplaceAll(fieldName, ".", "_"),
			DataType:     dataType,
			IsActive:     isActive,
			Status:       status,
			RuleType:     "auto-import",
		}
		if created, _ := s.mappingRepo.CreateIfNotExists(ctx, rule); created {
			added++
		}
	}
	return added
}

// createMappingRulesFromSchema parses Airbyte JSONSchema and creates approved mapping rules
func (s *ReconciliationService) createMappingRulesFromSchema(ctx context.Context, sourceTable string, jsonSchema interface{}) int {
	schemaMap, ok := jsonSchema.(map[string]interface{})
	if !ok {
		return 0
	}
	properties, ok := schemaMap["properties"].(map[string]interface{})
	if !ok {
		return 0
	}

	added := 0
	for fieldName, fieldDef := range properties {
		if strings.HasPrefix(fieldName, "_airbyte_") {
			continue
		}

		dataType := inferDataType(fieldDef)
		rule := &model.MappingRule{
			SourceTable:  sourceTable,
			SourceField:  fieldName,
			TargetColumn: strings.ReplaceAll(fieldName, ".", "_"),
			DataType:     dataType,
			IsActive:     true,
			Status:       "approved",
			RuleType:     "auto-import",
		}
		if created, _ := s.mappingRepo.CreateIfNotExists(ctx, rule); created {
			added++
		}
	}
	return added
}

func inferDataType(fieldDef interface{}) string {
	def, ok := fieldDef.(map[string]interface{})
	if !ok {
		return "TEXT"
	}

	typeVal := def["type"]
	var primaryType string
	switch t := typeVal.(type) {
	case string:
		primaryType = t
	case []interface{}:
		for _, v := range t {
			s, _ := v.(string)
			if s != "null" {
				primaryType = s
				break
			}
		}
	}

	format, _ := def["format"].(string)
	switch primaryType {
	case "integer":
		return "BIGINT"
	case "number":
		return "NUMERIC"
	case "boolean":
		return "BOOLEAN"
	case "string":
		if format == "date-time" || format == "date" {
			return "TIMESTAMP"
		}
		return "TEXT"
	case "object", "array":
		return "JSONB"
	default:
		return "TEXT"
	}
}
