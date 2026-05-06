package api

import (
	"encoding/json"
	"errors"
	"strconv"
	"strings"

	"cdc-cms-service/internal/app/commands"
	"cdc-cms-service/internal/app/ports"
	"cdc-cms-service/internal/app/queries"
	"cdc-cms-service/internal/infra/messaging"
	"cdc-cms-service/internal/infra/persistence"
	"cdc-cms-service/internal/middleware"
	"cdc-cms-service/pkgs/natsconn"

	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
	"gorm.io/gorm"
)

// ComputeDriftStatus derives (drift_pct, status, error_code) from the
// stored (source_count, dest_count, error_code) triple. Done on the read
// path so stored reports stay authoritative; the FE sees a single
// self-consistent view without re-running math.
//
// Contract (matches workspace §2.2):
//   - error path: any stored error_code or nil source_count => drift_pct=0,
//     status="error", code preserved (or SRC_QUERY_FAILED when src is nil).
//   - 0 vs 0: ok_empty (benign — no data either side).
//   - equal counts: ok.
//   - src>0 && dst==0: dest_missing (catastrophic — sync pipeline stalled).
//   - src==0 && dst>0: source_missing_or_stale (src probably down).
//   - otherwise drift_pct = |src-dst| / max(src,dst) * 100,
//     thresholds: drift >= 5%, warning >= 0.5%, else ok.
//
// Percent is unsigned so "src grew, dst fell" and "dst grew, src fell"
// both surface as the same magnitude.
func ComputeDriftStatus(sourceCount *int64, destCount int64, errorCode string) (float64, string, string) {
	if errorCode != "" {
		return 0, "error", errorCode
	}
	if sourceCount == nil {
		return 0, "error", "SRC_QUERY_FAILED"
	}
	src := *sourceCount
	if src == 0 && destCount == 0 {
		return 0, "ok_empty", ""
	}
	if src == destCount {
		return 0, "ok", ""
	}
	absDiff := src - destCount
	if absDiff < 0 {
		absDiff = -absDiff
	}
	maxVal := src
	if destCount > maxVal {
		maxVal = destCount
	}
	if maxVal < 1 {
		maxVal = 1
	}
	driftPct := float64(absDiff) / float64(maxVal) * 100

	status := "ok"
	switch {
	case src > 0 && destCount == 0:
		status = "dest_missing"
	case src == 0 && destCount > 0:
		status = "source_missing_or_stale"
	case driftPct >= 5:
		status = "drift"
	case driftPct >= 0.5:
		status = "warning"
	}
	return driftPct, status, ""
}

type ReconciliationHandler struct {
	reader         queries.ReconReader
	nats           *natsconn.NatsClient
	bus            ports.CommandBus
	listLatestQ    *queries.ListLatestReportsHandler
	getHistoryQ    *queries.GetTableHistoryHandler
	listFailedQ    *queries.ListFailedLogsHandler
	activityLogger *persistence.ActivityLogger
}

func NewReconciliationHandler(
	reader queries.ReconReader,
	nats *natsconn.NatsClient,
	bus ports.CommandBus,
	listLatestQ *queries.ListLatestReportsHandler,
	getHistoryQ *queries.GetTableHistoryHandler,
	listFailedQ *queries.ListFailedLogsHandler,
	activityLogger *persistence.ActivityLogger,
) *ReconciliationHandler {
	return &ReconciliationHandler{
		reader:         reader,
		nats:           nats,
		bus:            bus,
		listLatestQ:    listLatestQ,
		getHistoryQ:    getHistoryQ,
		listFailedQ:    listFailedQ,
		activityLogger: activityLogger,
	}
}

// ReportRow + FailedLogRow are type aliases so external callers /
// Swagger docs continue to see the legacy names; the canonical
// definitions now live in `internal/app/queries/recon_read_models.go`.
type ReportRow = queries.LatestReportRow
type FailedLogRow = queries.FailedLogRow

type reconScopeRequest struct {
	Table           string `json:"table"`
	SourceDatabase  string `json:"source_database"`
	SourceSchema    string `json:"source_schema"`
	SourceNamespace string `json:"source_namespace"`
	SourceTable     string `json:"source_table"`
	ShadowSchema    string `json:"shadow_schema"`
	ShadowTable     string `json:"shadow_table"`
}

func trimReconValue(v string) string {
	return strings.TrimSpace(v)
}

func stringOrNil(v *string) interface{} {
	if v == nil {
		return nil
	}
	return *v
}

func (h *ReconciliationHandler) resolveTargetTable(c *fiber.Ctx, scope reconScopeRequest) (string, error) {
	if t := trimReconValue(scope.Table); t != "" {
		return t, nil
	}
	return h.reader.ResolveTargetTableByScope(c.UserContext(), queries.ReconScopeFilter{
		SourceDatabase:  scope.SourceDatabase,
		SourceSchema:    scope.SourceSchema,
		SourceNamespace: scope.SourceNamespace,
		SourceTable:     scope.SourceTable,
		ShadowSchema:    scope.ShadowSchema,
		ShadowTable:     scope.ShadowTable,
	})
}

// LatestReport godoc
// @Summary      List latest reconciliation reports
// @Description  Return latest reconciliation status per shadow target, enriched with source/shadow metadata when V2 bindings can be resolved.
// @Tags         reconciliation
// @Produce      json
// @Success      200  {object}  map[string]interface{}
// @Router       /api/v1/reconciliation/report [get]
//
// LatestReport delegates the SQL to queries.ListLatestReportsHandler
// (P2 / CQRS Q-side) and runs the enrichment loop here so the
// drift/VI/source-method helpers stay colocated with their unit tests
// (ComputeDriftStatus has 16 subtests in this package). The API
// surface stays byte-identical with the pre-refactor wire shape — JSON
// tags live on queries.LatestReportRow.
func (h *ReconciliationHandler) LatestReport(c *fiber.Ctx) error {
	res, err := h.listLatestQ.Handle(c.UserContext(), queries.ListLatestReportsQuery{})
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}
	rows := res.Data

	// Enrich each row: drift computation + VI error message
	// + source query method tooltip. Helpers live in this package so
	// the existing ComputeDriftStatus test matrix stays in place.
	for i := range rows {
		errCode := ""
		if rows[i].ErrorCode != nil {
			errCode = *rows[i].ErrorCode
		}
		driftPct, computed, finalCode := ComputeDriftStatus(rows[i].NullableSourceCount, rows[i].DestCount, errCode)
		rows[i].DriftPct = driftPct
		rows[i].ComputedStatus = computed
		if finalCode != "" {
			rows[i].ErrorMessageVI = ErrorMessagesVI[finalCode]
			// Ensure ErrorCode surfaces even when derived (e.g. nil src →
			// SRC_QUERY_FAILED). Keep existing pointer if worker set one.
			if rows[i].ErrorCode == nil {
				fc := finalCode
				rows[i].ErrorCode = &fc
			}
		}
		rows[i].SourceQueryMethod = deriveSourceQueryMethod(rows[i].TimestampField, rows[i].CheckType)
	}

	return c.JSON(fiber.Map{"data": rows, "total": len(rows)})
}

// deriveSourceQueryMethod explains — in one short label — how the source
// count in the report was computed. Helps operators answer the question
// "why is source=0 when Mongo clearly has rows?" without reading Go code.
//
// Values:
//   - window_updated_at       — default path, Mongo filter on `updated_at`
//   - window_custom_field     — registry override (e.g. `lastUpdatedAt`)
//   - window_id_ts_fallback   — registry field missing AND collection
//     lacks the default, fallback to ObjectID time
//   - full_count              — legacy Tier-3-era `CountDocuments` path
func deriveSourceQueryMethod(tsField *string, checkType string) string {
	if checkType == "bucket_hash" {
		return "full_count"
	}
	if tsField == nil || *tsField == "" || *tsField == "updated_at" {
		return "window_updated_at"
	}
	if *tsField == "_id" {
		return "window_id_ts_fallback"
	}
	return "window_custom_field"
}

// TableHistory godoc
// @Summary      Get reconciliation history for a shadow target
// @Description  Return historical reconciliation checks for one shadow target identified by legacy target_table path.
// @Tags         reconciliation
// @Produce      json
// @Param        table      path   string  true   "Shadow target table (legacy compatibility key)"
// @Param        page       query  int     false  "Page number"
// @Param        page_size  query  int     false  "Page size"
// @Success      200  {object}  map[string]interface{}
// @Router       /api/v1/reconciliation/report/{table} [get]
//
// TableHistory returns reconciliation history for a specific table.
// Delegates the SQL+pagination to queries.GetTableHistoryHandler.
func (h *ReconciliationHandler) TableHistory(c *fiber.Ctx) error {
	page, _ := strconv.Atoi(c.Query("page", "1"))
	pageSize, _ := strconv.Atoi(c.Query("page_size", "20"))
	res, err := h.getHistoryQ.Handle(c.UserContext(), queries.GetTableHistoryQuery{
		Table:    c.Params("table"),
		Page:     page,
		PageSize: pageSize,
	})
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}
	return c.JSON(fiber.Map{"data": res.Data, "total": res.Total, "page": res.Page})
}

// TriggerCheck godoc
// @Summary      Trigger reconciliation check for one scope
// @Description  Dispatch a reconciliation check for a single shadow target. Supports legacy `:table` path or body scope (`source_database`, `source_table`, `shadow_schema`, `shadow_table`).
// @Tags         reconciliation
// @Accept       json
// @Produce      json
// @Param        table  path      string  false  "Shadow target table (legacy compatibility key)"
// @Param        tier   query     string  false  "Tier number"
// @Param        body   body      object  false  "Optional source/shadow scope payload"
// @Success      202    {object}  map[string]interface{}
// @Failure      404    {object}  map[string]interface{}
// @Failure      409    {object}  map[string]interface{}
// @Router       /api/v1/reconciliation/check/{table} [post]
//
// TriggerCheck dispatches reconciliation check via NATS
func (h *ReconciliationHandler) TriggerCheck(c *fiber.Ctx) error {
	tier := c.Query("tier", "1")
	table := strings.TrimSpace(c.Params("table"))
	if table == "" {
		var scope reconScopeRequest
		_ = c.BodyParser(&scope)
		resolved, err := h.resolveTargetTable(c, scope)
		if err != nil {
			if errors.Is(err, queries.ErrAmbiguousScope) {
				return c.Status(409).JSON(fiber.Map{"error": "ambiguous_reconciliation_scope"})
			}
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return c.Status(404).JSON(fiber.Map{"error": "reconciliation_scope_not_found"})
			}
			return c.Status(500).JSON(fiber.Map{"error": err.Error()})
		}
		table = resolved
	}

	cmd := commands.ReconCheckCommand{Tier: tier, Table: table}
	user := middleware.GetUsername(c)
	ctx := messaging.WithMetadata(c.UserContext(), user, c.Get("X-Correlation-Id"), c.Get("Idempotency-Key"))

	res, err := h.bus.Dispatch(ctx, cmd)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}

	// Log activity (audit trail mirrors the cdc_jobs row).
	payload := map[string]any{}
	if raw, err := json.Marshal(cmd); err == nil {
		_ = json.Unmarshal(raw, &payload)
	}
	h.activityLogger.LogAsync(persistence.ActivityEntry{
		Operation: "recon-check", TargetTable: table, Status: "success", Details: payload,
	})

	return c.Status(202).JSON(fiber.Map{
		"message": "reconciliation check dispatched",
		"tier":    tier,
		"table":   table,
		"job_id":  res.JobID,
	})
}

// TriggerCheckAll godoc
// @Summary      Trigger reconciliation check for all or one scoped target
// @Description  Without body scope, dispatch Tier 1 reconciliation for all shadow targets. When body scope is supplied, resolve and dispatch a single scoped check.
// @Tags         reconciliation
// @Accept       json
// @Produce      json
// @Param        tier  query     string  false  "Tier number"
// @Param        body  body      object  false  "Optional source/shadow scope payload"
// @Success      202   {object}  map[string]interface{}
// @Failure      404   {object}  map[string]interface{}
// @Failure      409   {object}  map[string]interface{}
// @Router       /api/v1/reconciliation/check [post]
//
// TriggerCheckAll dispatches Tier 1 check for all tables
func (h *ReconciliationHandler) TriggerCheckAll(c *fiber.Ctx) error {
	var scope reconScopeRequest
	_ = c.BodyParser(&scope)
	tier := c.Query("tier", "1")
	user := middleware.GetUsername(c)
	ctx := messaging.WithMetadata(c.UserContext(), user, c.Get("X-Correlation-Id"), c.Get("Idempotency-Key"))

	table, err := h.resolveTargetTable(c, scope)
	if err == nil && table != "" {
		if _, derr := h.bus.Dispatch(ctx, commands.ReconCheckCommand{Tier: tier, Table: table}); derr != nil {
			return c.Status(500).JSON(fiber.Map{"error": derr.Error()})
		}
		return c.Status(202).JSON(fiber.Map{"message": "reconciliation check dispatched", "tier": tier, "table": table})
	}

	if _, derr := h.bus.Dispatch(ctx, commands.ReconCheckCommand{Tier: "1", Table: "*"}); derr != nil {
		return c.Status(500).JSON(fiber.Map{"error": derr.Error()})
	}
	return c.Status(202).JSON(fiber.Map{"message": "tier 1 check dispatched for all tables"})
}

// TriggerHeal godoc
// @Summary      Trigger reconciliation heal for one scope
// @Description  Dispatch a heal for a single shadow target. Supports legacy `:table` path or body scope (`source_database`, `source_table`, `shadow_schema`, `shadow_table`).
// @Tags         reconciliation
// @Accept       json
// @Produce      json
// @Param        table  path      string  false  "Shadow target table (legacy compatibility key)"
// @Param        body   body      object  false  "Optional source/shadow scope payload"
// @Success      202    {object}  map[string]interface{}
// @Failure      404    {object}  map[string]interface{}
// @Failure      409    {object}  map[string]interface{}
// @Router       /api/v1/reconciliation/heal [post]
// @Router       /api/v1/reconciliation/heal/{table} [post]
//
// TriggerHeal dispatches heal for a specific table
func (h *ReconciliationHandler) TriggerHeal(c *fiber.Ctx) error {
	table := strings.TrimSpace(c.Params("table"))
	if table == "" {
		var scope reconScopeRequest
		_ = c.BodyParser(&scope)
		resolved, err := h.resolveTargetTable(c, scope)
		if err != nil {
			if errors.Is(err, queries.ErrAmbiguousScope) {
				return c.Status(409).JSON(fiber.Map{"error": "ambiguous_reconciliation_scope"})
			}
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return c.Status(404).JSON(fiber.Map{"error": "reconciliation_scope_not_found"})
			}
			return c.Status(500).JSON(fiber.Map{"error": err.Error()})
		}
		table = resolved
	}

	user := middleware.GetUsername(c)
	ctx := messaging.WithMetadata(c.UserContext(), user, c.Get("X-Correlation-Id"), c.Get("Idempotency-Key"))
	res, derr := h.bus.Dispatch(ctx, commands.ReconHealCommand{Table: table})
	if derr != nil {
		return c.Status(500).JSON(fiber.Map{"error": derr.Error()})
	}

	h.activityLogger.LogAsync(persistence.ActivityEntry{
		Operation: "recon-heal-trigger", TargetTable: table, Status: "success",
	})

	return c.Status(202).JSON(fiber.Map{"message": "heal dispatched", "table": table, "job_id": res.JobID})
}

// ListFailedLogs godoc
// @Summary      List failed sync logs
// @Description  Return paginated failed sync logs, enriched with source/shadow metadata when the shadow binding can be resolved.
// @Tags         reconciliation
// @Produce      json
// @Param        target_table  query  string  false  "Shadow target table"
// @Param        status        query  string  false  "Log status"
// @Param        error_type    query  string  false  "Error type"
// @Param        page          query  int     false  "Page number"
// @Param        page_size     query  int     false  "Page size"
// @Success      200  {object}  map[string]interface{}
// @Router       /api/v1/failed-sync-logs [get]
//
// ListFailedLogs returns failed sync logs (paginated, filterable).
// Delegates the SQL+pagination to queries.ListFailedLogsHandler.
func (h *ReconciliationHandler) ListFailedLogs(c *fiber.Ctx) error {
	page, _ := strconv.Atoi(c.Query("page", "1"))
	pageSize, _ := strconv.Atoi(c.Query("page_size", "30"))
	res, err := h.listFailedQ.Handle(c.UserContext(), queries.ListFailedLogsQuery{
		Filter: queries.FailedLogFilter{
			TargetTable: c.Query("target_table"),
			Status:      c.Query("status"),
			ErrorType:   c.Query("error_type"),
		},
		Page:     page,
		PageSize: pageSize,
	})
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}
	return c.JSON(fiber.Map{"data": res.Data, "total": res.Total, "page": res.Page})
}

// RetryFailedLog retries a single failed record
// RetryFailedLog godoc
// @Summary      Retry a failed sync log
// @Description  Retries one failed sync log by canonical log ID. The ID remains the primary identity; response and downstream payload are enriched with source/shadow scope when metadata can be resolved.
// @Tags         reconciliation
// @Accept       json
// @Produce      json
// @Param        id    path      int     true   "Failed log ID"
// @Param        body  body      object  false  "Optional audit reason payload"
// @Success      202   {object}  map[string]interface{}
// @Failure      404   {object}  map[string]interface{}
// @Router       /api/v1/failed-sync-logs/{id}/retry [post]
//
// RetryFailedLog retries a single failed record
func (h *ReconciliationHandler) RetryFailedLog(c *fiber.Ctx) error {
	id, _ := c.ParamsInt("id")

	log, err := h.reader.GetFailedLogByID(c.UserContext(), int64(id))
	if err != nil {
		return c.Status(404).JSON(fiber.Map{"error": "record not found"})
	}

	scope, _ := h.reader.GetRetryScopeByLogID(c.UserContext(), int64(id))

	// Dispatch retry via CommandBus.
	user := middleware.GetUsername(c)
	ctx := messaging.WithMetadata(c.UserContext(), user, c.Get("X-Correlation-Id"), c.Get("Idempotency-Key"))
	cmd := commands.RetryFailedCommand{
		FailedLogID:    log.ID,
		TargetTable:    log.TargetTable,
		RecordID:       log.RecordID,
		RawJSON:        string(log.RawJSON),
		SourceDatabase: scope.SourceDatabase,
		SourceTable:    scope.ResolvedSourceTable,
		ShadowSchema:   scope.ShadowSchema,
		ShadowTable:    scope.ShadowTable,
		ScopeAmbiguous: scope.ScopeAmbiguous,
	}
	res, derr := h.bus.Dispatch(ctx, cmd)
	if derr != nil {
		return c.Status(500).JSON(fiber.Map{"error": derr.Error()})
	}

	// Mark UI projection via bus so the cdc_jobs audit row carries the
	// retry-mark intent alongside the retry dispatch above. Best-effort
	// — failure here doesn't reverse the dispatch the operator already
	// triggered, just leaves the FE badge stale until next poll.
	//
	// Idempotency-Key suffix `:mark` so the mark row doesn't collide
	// with the retry-failed Dispatch row on the UNIQUE(idempotency_key)
	// constraint when the operator supplied the same client key.
	markIdem := c.Get("Idempotency-Key")
	if markIdem != "" {
		markIdem += ":mark"
	}
	markCtx := messaging.WithMetadata(c.UserContext(), user, c.Get("X-Correlation-Id"), markIdem)
	markCmd := commands.MarkFailedLogRetryingCommand{FailedLogID: log.ID, UpdatedBy: user}
	_, _ = h.bus.Execute(markCtx, markCmd)

	return c.Status(202).JSON(fiber.Map{
		"message": "retry dispatched",
		"id":      id,
		"job_id":  res.JobID,
		"scope": fiber.Map{
			"source_database": stringOrNil(scope.SourceDatabase),
			"source_table":    stringOrNil(scope.ResolvedSourceTable),
			"shadow_schema":   stringOrNil(scope.ShadowSchema),
			"shadow_table":    stringOrNil(scope.ShadowTable),
			"scope_ambiguous": scope.ScopeAmbiguous,
		},
	})
}

// Tools: Reset Debezium offset via signal
func (h *ReconciliationHandler) ResetDebeziumOffset(c *fiber.Ctx) error {
	var body struct {
		Database   string `json:"database"`
		Collection string `json:"collection"`
	}
	if err := c.BodyParser(&body); err != nil {
		return c.Status(400).JSON(fiber.Map{"error": err.Error()})
	}

	user := middleware.GetUsername(c)
	ctx := messaging.WithMetadata(c.UserContext(), user, c.Get("X-Correlation-Id"), c.Get("Idempotency-Key"))
	cmd := commands.DebeziumSignalCommand{
		Type_:      "signal-snapshot",
		Database:   body.Database,
		Collection: body.Collection,
	}
	res, derr := h.bus.Dispatch(ctx, cmd)
	if derr != nil {
		return c.Status(500).JSON(fiber.Map{"error": derr.Error()})
	}
	return c.Status(202).JSON(fiber.Map{"message": "debezium signal dispatched", "job_id": res.JobID})
}

// TriggerBackfillSourceTs dispatches the _source_ts backfill job.
//
// POST /api/recon/backfill-source-ts
// Body: {"table": "refund_requests"}  (optional — empty = all tables)
//
// The handler generates a run_id UUID (not to be confused with the
// per-table recon_runs.id produced by the worker) so the CMS can
// correlate the status poll without knowing worker-side IDs.
func (h *ReconciliationHandler) TriggerBackfillSourceTs(c *fiber.Ctx) error {
	var body struct {
		Table     string `json:"table"`
		BatchSize int    `json:"batch_size"`
	}
	if err := c.BodyParser(&body); err != nil {
		// Body is optional — fall through to default {} behaviour.
		body = struct {
			Table     string `json:"table"`
			BatchSize int    `json:"batch_size"`
		}{}
	}

	runID := uuid.NewString()
	user := middleware.GetUsername(c)
	ctx := messaging.WithMetadata(c.UserContext(), user, c.Get("X-Correlation-Id"), c.Get("Idempotency-Key"))
	cmd := commands.ReconBackfillSourceTsCommand{
		Table:     body.Table,
		RunID:     runID,
		BatchSize: body.BatchSize,
	}
	res, derr := h.bus.Dispatch(ctx, cmd)
	if derr != nil {
		return c.Status(500).JSON(fiber.Map{"error": derr.Error()})
	}

	// Audit details mirror the on-wire payload (raw command marshal).
	details := map[string]any{}
	if raw, err := json.Marshal(cmd); err == nil {
		_ = json.Unmarshal(raw, &details)
	}
	h.activityLogger.LogAsync(persistence.ActivityEntry{
		Operation: "recon-backfill-source-ts", TargetTable: body.Table, Status: "dispatched", Details: details,
	})

	return c.Status(202).JSON(fiber.Map{
		"message":    "backfill dispatched",
		"run_id":     runID,
		"table":      body.Table,
		"job_id":     res.JobID,
		"status_url": "/api/recon/backfill-source-ts/status",
	})
}

// BackfillSourceTsStatus returns recent tier=4 recon_runs rows. The
// worker writes one row per table per run; the CMS page composes a
// progress table by joining on started_at.
//
// Query params:
//
//	?table=refund_requests     — filter to one table
//	?run_id=<uuid>             — filter to rows produced by one trigger
//	                              (worker encodes run_id in instance_id as
//	                              `backfill:<run_id>`)
func (h *ReconciliationHandler) BackfillSourceTsStatus(c *fiber.Ctx) error {
	rows, err := h.reader.ListBackfillRuns(c.UserContext(), c.Query("table"), c.Query("run_id"), 30)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}

	// Enrich with per-table total + remaining so FE can compute progress
	// without an extra trip. Bound by input table names to keep it cheap.
	type enriched struct {
		queries.BackfillRunRow
		TotalRows     int64   `json:"total_rows"`
		NullRemaining int64   `json:"null_remaining"`
		PercentDone   float64 `json:"percent_done"`
	}
	out := make([]enriched, 0, len(rows))
	seenTable := map[string]struct{}{}
	totals := map[string]int64{}
	remain := map[string]int64{}
	for _, r := range rows {
		if _, ok := seenTable[r.TableName]; ok {
			continue
		}
		seenTable[r.TableName] = struct{}{}
		// Safe: r.TableName comes from recon_runs row we just wrote.
		total, nul, _ := h.reader.CountTableRows(c.UserContext(), r.TableName)
		totals[r.TableName] = total
		remain[r.TableName] = nul
	}
	for _, r := range rows {
		total := totals[r.TableName]
		nul := remain[r.TableName]
		pct := 0.0
		if total > 0 {
			pct = float64(total-nul) / float64(total) * 100.0
		}
		out = append(out, enriched{
			BackfillRunRow: r,
			TotalRows:      total,
			NullRemaining:  nul,
			PercentDone:    pct,
		})
	}

	return c.JSON(fiber.Map{"data": out, "total": len(out)})
}

// Tools: Trigger snapshot for a table
func (h *ReconciliationHandler) TriggerSnapshot(c *fiber.Ctx) error {
	table := c.Params("table")
	user := middleware.GetUsername(c)
	ctx := messaging.WithMetadata(c.UserContext(), user, c.Get("X-Correlation-Id"), c.Get("Idempotency-Key"))
	res, derr := h.bus.Dispatch(ctx, commands.DebeziumSnapshotCommand{Table: table})
	if derr != nil {
		return c.Status(500).JSON(fiber.Map{"error": derr.Error()})
	}
	return c.Status(202).JSON(fiber.Map{"message": "snapshot signal dispatched", "table": table, "job_id": res.JobID})
}
