package api

import (
	"strconv"
	"strings"
	"time"

	"cdc-cms-service/internal/app/queries"

	"github.com/gofiber/fiber/v2"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

// SourceObjectsHandler exposes a V2-native read model for source objects.
// It intentionally keeps write operations on the transitional /api/registry
// surface for now, because CMS operator-flow still relies on those mutations.
//
// Phase 2 v2 / P2 — List + GetMappingContext now delegate to CQRS Q-side
// handlers; raw SQL for those paths lives in
// `internal/infra/persistence/source_object_read_repo_gorm.go`. GetStats
// + ListShadowBindings stay on the legacy in-handler SQL until P2.T2.7
// migrates them.
type SourceObjectsHandler struct {
	db        *gorm.DB
	logger    *zap.Logger
	listQ     *queries.ListSourceObjectsHandler
	mappingCx *queries.GetSourceObjectMappingContextHandler
}

func NewSourceObjectsHandler(
	db *gorm.DB,
	logger *zap.Logger,
	listQ *queries.ListSourceObjectsHandler,
	mappingCx *queries.GetSourceObjectMappingContextHandler,
) *SourceObjectsHandler {
	return &SourceObjectsHandler{db: db, logger: logger, listQ: listQ, mappingCx: mappingCx}
}

// SourceObjectRow is the wire shape of one row in the V2 list response.
// Aliased to the Q-side read model so server-side callers + Swagger
// see the same type.
type SourceObjectRow = queries.SourceObjectListItem

type sourceObjectsListResponse struct {
	Data  []SourceObjectRow `json:"data"`
	Total int64             `json:"total"`
	Page  int               `json:"page"`
}

type SourceObjectStats struct {
	Total         int64          `json:"total"`
	BySourceDB    map[string]int `json:"by_source_db"`
	BySyncEngine  map[string]int `json:"by_sync_engine"`
	ByPriority    map[string]int `json:"by_priority"`
	TablesCreated int64          `json:"tables_created"`
}

type ShadowBindingRow struct {
	ID               int64      `json:"id"`
	BindingCode      string     `json:"binding_code"`
	SourceObjectID   int64      `json:"source_object_id"`
	ObjectCode       string     `json:"object_code"`
	RegistryID       *uint      `json:"registry_id,omitempty"`
	SourceDB         string     `json:"source_db"`
	SourceType       string     `json:"source_type"`
	SourceTable      string     `json:"source_table"`
	ShadowSchema     string     `json:"shadow_schema"`
	ShadowTable      string     `json:"shadow_table"`
	PhysicalTableFQN string     `json:"physical_table_fqn"`
	WriteMode        string     `json:"write_mode"`
	DDLStatus        string     `json:"ddl_status"`
	IsActive         bool       `json:"is_active"`
	ReconDrift       int64      `json:"recon_drift"`
	LastReconAt      *time.Time `json:"last_recon_at,omitempty"`
	CreatedAt        time.Time  `json:"created_at"`
	UpdatedAt        time.Time  `json:"updated_at"`
}

type shadowBindingsListResponse struct {
	Data  []ShadowBindingRow `json:"data"`
	Total int64              `json:"total"`
	Page  int                `json:"page"`
}

// SourceObjectMappingContext is the wire shape of GET
// /api/v1/source-objects/registry/{registry_id}. Aliased to the Q-side
// read model so the response goes straight through `c.JSON`.
type SourceObjectMappingContext = queries.SourceObjectMappingContextReadModel

// GetStats godoc
// @Summary      Get V2 source-object statistics
// @Description  Returns summary stats from cdc_system source objects enriched with transitional priority and shadow-DDL creation state.
// @Tags         Source Objects
// @Produce      json
// @Success      200 {object} SourceObjectStats
// @Failure      500 {object} map[string]string
// @Security     BearerAuth
// @Router       /api/v1/source-objects/stats [get]
func (h *SourceObjectsHandler) GetStats(c *fiber.Ctx) error {
	stats := &SourceObjectStats{
		BySourceDB:   make(map[string]int),
		BySyncEngine: make(map[string]int),
		ByPriority:   make(map[string]int),
	}

	baseWhere := `
		FROM cdc_system.source_object_registry so
		LEFT JOIN LATERAL (
			SELECT
				sb.shadow_table,
				sb.ddl_status,
				sb.updated_at
			FROM cdc_system.shadow_binding sb
			WHERE sb.source_object_id = so.id
			ORDER BY sb.is_active DESC, sb.updated_at DESC, sb.id DESC
			LIMIT 1
		) sb ON TRUE
		LEFT JOIN cdc_system.cdc_table_registry tr
		  ON tr.source_db = so.source_database
		 AND tr.source_table = so.source_object_name
		 AND (
		       (sb.shadow_table IS NOT NULL AND tr.target_table = sb.shadow_table)
		    OR (sb.shadow_table IS NULL AND tr.target_table = so.source_object_name)
		 )
		WHERE so.sync_engine = 'debezium'
	`

	type countRow struct {
		Total int64 `json:"total"`
	}
	type groupCount struct {
		Key   string `json:"key"`
		Count int    `json:"count"`
	}

	var totalRow countRow
	if err := h.db.WithContext(c.Context()).Raw(`SELECT COUNT(*) AS total ` + baseWhere).Scan(&totalRow).Error; err != nil {
		h.logger.Error("source object stats total failed", zap.Error(err))
		return c.Status(500).JSON(fiber.Map{"error": "internal_error"})
	}
	stats.Total = totalRow.Total

	var createdRow countRow
	if err := h.db.WithContext(c.Context()).Raw(`
		SELECT COUNT(*) AS total
	` + baseWhere + `
		  AND COALESCE(sb.ddl_status = 'created', tr.is_table_created, false)
	`).Scan(&createdRow).Error; err != nil {
		h.logger.Error("source object stats created failed", zap.Error(err))
		return c.Status(500).JSON(fiber.Map{"error": "internal_error"})
	}
	stats.TablesCreated = createdRow.Total

	var dbCounts []groupCount
	if err := h.db.WithContext(c.Context()).Raw(`
		SELECT so.source_database AS key, COUNT(*) AS count
	` + baseWhere + `
		GROUP BY so.source_database
	`).Scan(&dbCounts).Error; err != nil {
		h.logger.Error("source object stats by_source_db failed", zap.Error(err))
		return c.Status(500).JSON(fiber.Map{"error": "internal_error"})
	}
	for _, row := range dbCounts {
		stats.BySourceDB[row.Key] = row.Count
	}

	var engineCounts []groupCount
	if err := h.db.WithContext(c.Context()).Raw(`
		SELECT so.sync_engine AS key, COUNT(*) AS count
	` + baseWhere + `
		GROUP BY so.sync_engine
	`).Scan(&engineCounts).Error; err != nil {
		h.logger.Error("source object stats by_sync_engine failed", zap.Error(err))
		return c.Status(500).JSON(fiber.Map{"error": "internal_error"})
	}
	for _, row := range engineCounts {
		stats.BySyncEngine[row.Key] = row.Count
	}

	var priorityCounts []groupCount
	if err := h.db.WithContext(c.Context()).Raw(`
		SELECT COALESCE(tr.priority, 'normal') AS key, COUNT(*) AS count
	` + baseWhere + `
		GROUP BY COALESCE(tr.priority, 'normal')
	`).Scan(&priorityCounts).Error; err != nil {
		h.logger.Error("source object stats by_priority failed", zap.Error(err))
		return c.Status(500).JSON(fiber.Map{"error": "internal_error"})
	}
	for _, row := range priorityCounts {
		stats.ByPriority[row.Key] = row.Count
	}

	return c.JSON(stats)
}

// List godoc
// @Summary      List V2 source objects
// @Description  Returns source objects from cdc_system.source_object_registry enriched with active shadow binding, reconciliation status, and transitional registry bridge metadata.
// @Tags         Source Objects
// @Produce      json
// @Param        source_db    query string false "Filter by source database"
// @Param        is_active    query string false "Filter by active status" Enums(true, false)
// @Param        page         query int    false "Page number" default(1)
// @Param        page_size    query int    false "Page size"   default(20)
// @Success      200 {object} sourceObjectsListResponse
// @Failure      500 {object} map[string]string
// @Security     BearerAuth
// @Router       /api/v1/source-objects [get]
func (h *SourceObjectsHandler) List(c *fiber.Ctx) error {
	q := queries.ListSourceObjectsQuery{
		Filter:   queries.SourceObjectListFilter{SourceDB: strings.TrimSpace(c.Query("source_db"))},
		Page:     intQuery(c, "page", 1),
		PageSize: intQuery(c, "page_size", 20),
	}
	if raw := strings.TrimSpace(c.Query("is_active")); raw != "" {
		if active, err := strconv.ParseBool(raw); err == nil {
			q.Filter.IsActive = &active
		}
	}

	res, err := h.listQ.Handle(c.Context(), q)
	if err != nil {
		h.logger.Error("source objects list failed", zap.Error(err))
		return c.Status(500).JSON(fiber.Map{"error": "internal_error"})
	}

	return c.JSON(sourceObjectsListResponse{
		Data:  res.Data,
		Total: res.Total,
		Page:  res.Page,
	})
}

// ListShadowBindings godoc
// @Summary      List V2 shadow bindings
// @Description  Returns active and inactive shadow bindings from cdc_system.shadow_binding enriched with source-object metadata and latest reconciliation context.
// @Tags         Source Objects
// @Produce      json
// @Param        source_db    query string false "Filter by source database"
// @Param        is_active    query string false "Filter by active status" Enums(true, false)
// @Param        page         query int    false "Page number" default(1)
// @Param        page_size    query int    false "Page size"   default(20)
// @Success      200 {object} shadowBindingsListResponse
// @Failure      500 {object} map[string]string
// @Security     BearerAuth
// @Router       /api/v1/shadow-bindings [get]
func (h *SourceObjectsHandler) ListShadowBindings(c *fiber.Ctx) error {
	page := intQuery(c, "page", 1)
	pageSize := intQuery(c, "page_size", 20)
	if page <= 0 {
		page = 1
	}
	if pageSize <= 0 {
		pageSize = 20
	}
	if pageSize > 500 {
		pageSize = 500
	}

	sourceDB := strings.TrimSpace(c.Query("source_db"))
	isActiveRaw := strings.TrimSpace(c.Query("is_active"))

	type countRow struct {
		Total int64 `json:"total"`
	}

	baseWhere := `
		FROM cdc_system.shadow_binding sb
		JOIN cdc_system.source_object_registry so
		  ON so.id = sb.source_object_id
		LEFT JOIN cdc_system.cdc_table_registry tr
		  ON tr.source_db = so.source_database
		 AND tr.source_table = so.source_object_name
		 AND tr.target_table = sb.shadow_table
		LEFT JOIN LATERAL (
			SELECT
				rr.diff,
				rr.checked_at
			FROM cdc_system.cdc_reconciliation_report rr
			WHERE rr.target_table = sb.shadow_table
			ORDER BY rr.checked_at DESC
			LIMIT 1
		) rr ON TRUE
		WHERE so.sync_engine = 'debezium'
	`

	args := make([]interface{}, 0, 4)
	if sourceDB != "" {
		baseWhere += ` AND so.source_database = ?`
		args = append(args, sourceDB)
	}
	if isActiveRaw != "" {
		active, err := strconv.ParseBool(isActiveRaw)
		if err == nil {
			baseWhere += ` AND sb.is_active = ?`
			args = append(args, active)
		}
	}

	var totalRow countRow
	countQuery := `SELECT COUNT(*) AS total ` + baseWhere
	if err := h.db.WithContext(c.Context()).Raw(countQuery, args...).Scan(&totalRow).Error; err != nil {
		h.logger.Error("shadow bindings count failed", zap.Error(err))
		return c.Status(500).JSON(fiber.Map{"error": "internal_error"})
	}

	query := `
		SELECT
			sb.id,
			sb.binding_code,
			sb.source_object_id,
			so.object_code,
			tr.id AS registry_id,
			COALESCE(so.source_database, '') AS source_db,
			so.source_engine_type AS source_type,
			so.source_object_name AS source_table,
			sb.shadow_schema,
			sb.shadow_table,
			sb.physical_table_fqn,
			sb.write_mode,
			sb.ddl_status,
			sb.is_active,
			COALESCE(rr.diff, 0) AS recon_drift,
			rr.checked_at AS last_recon_at,
			sb.created_at,
			sb.updated_at
	` + baseWhere + `
		ORDER BY so.source_database, sb.shadow_schema, sb.shadow_table
		LIMIT ? OFFSET ?
	`

	queryArgs := append([]interface{}{}, args...)
	queryArgs = append(queryArgs, pageSize, (page-1)*pageSize)

	var rows []ShadowBindingRow
	if err := h.db.WithContext(c.Context()).Raw(query, queryArgs...).Scan(&rows).Error; err != nil {
		h.logger.Error("shadow bindings list failed", zap.Error(err))
		return c.Status(500).JSON(fiber.Map{"error": "internal_error"})
	}

	return c.JSON(shadowBindingsListResponse{
		Data:  rows,
		Total: totalRow.Total,
		Page:  page,
	})
}

// GetMappingContext godoc
// @Summary      Get mapping context by legacy registry bridge
// @Description  Returns a single source-object mapping context resolved from the current registry bridge, enriched with V2 source/shadow metadata for the mapping page.
// @Tags         Source Objects
// @Produce      json
// @Param        registry_id path int true "Legacy registry bridge ID"
// @Success      200 {object} SourceObjectMappingContext
// @Failure      404 {object} map[string]string
// @Failure      500 {object} map[string]string
// @Security     BearerAuth
// @Router       /api/v1/source-objects/registry/{registry_id} [get]
func (h *SourceObjectsHandler) GetMappingContext(c *fiber.Ctx) error {
	registryID, err := strconv.ParseUint(c.Params("registry_id"), 10, 64)
	if err != nil {
		return c.Status(400).JSON(fiber.Map{"error": "invalid_registry_id"})
	}
	row, err := h.mappingCx.Handle(c.Context(), queries.GetSourceObjectMappingContextQuery{RegistryID: registryID})
	if err != nil {
		h.logger.Error("mapping context failed", zap.Error(err))
		return c.Status(500).JSON(fiber.Map{"error": "internal_error"})
	}
	if row == nil {
		return c.Status(404).JSON(fiber.Map{"error": "not_found"})
	}
	return c.JSON(row)
}
