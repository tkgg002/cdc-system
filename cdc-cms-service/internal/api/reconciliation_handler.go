package api

import (
	"encoding/json"
	"strconv"
	"time"

	"cdc-cms-service/internal/model"
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
	db   *gorm.DB
	nats *natsconn.NatsClient
}

func NewReconciliationHandler(db *gorm.DB, nats *natsconn.NatsClient) *ReconciliationHandler {
	return &ReconciliationHandler{db: db, nats: nats}
}

// LatestReport returns the latest reconciliation report per table, enriched
// with registry metadata (sync_engine, source_type, timestamp_field) so the
// FE can render a Sync Engine column + tooltip without a second round-trip.
//
// Bug C/D fix (2026-04-20): earlier version did `SELECT *` on just the
// report table — FE had no way to distinguish debezium-driven vs
// rows for tables that have been removed from the registry still appear
// (with NULL sync_engine) rather than disappearing.
func (h *ReconciliationHandler) LatestReport(c *fiber.Ctx) error {
	// Row shape includes fields produced by migrations that may not exist on
	// older DBs (timestamp_field_source, full_source_count, error_code, …).
	// We use to_regclass + has_column tricks via COALESCE on column-not-found
	// level — here we just LEFT JOIN and allow NULLs to flow through. GORM's
	// Scan tolerates missing output columns because we bind via `gorm:"column:"`.
	//
	// For forward-compat with worker migration 017 we pull:
	//   - error_code               (per-check code — populated by worker, NULL on success)
	//   - timestamp_field_source   (auto | manual | override — from registry)
	//   - timestamp_field_confidence (high | medium | low)
	//   - full_source_count / full_dest_count / full_count_at — daily aggregate
	//
	// The to_jsonb(r.*) ->> 'error_code' trick is unnecessary now that we own
	// the SELECT list — we project the column names directly and COALESCE to
	// empty string / zero when absent via LEFT JOIN (worker writes into
	// cdc_reconciliation_report). If worker hasn't shipped migration 017 the
	// SELECT will error — handled by fallback query below.
	type reportRow struct {
		model.ReconciliationReport
		SyncEngine               *string    `gorm:"column:sync_engine" json:"sync_engine"`
		SourceType               *string    `gorm:"column:source_type" json:"source_type"`
		TimestampField           *string    `gorm:"column:timestamp_field" json:"timestamp_field"`
		TimestampFieldSource     *string    `gorm:"column:timestamp_field_source" json:"timestamp_field_source,omitempty"`
		TimestampFieldConfidence *string    `gorm:"column:timestamp_field_confidence" json:"timestamp_field_confidence,omitempty"`
		FullSourceCount          *int64     `gorm:"column:full_source_count" json:"full_source_count,omitempty"`
		FullDestCount            *int64     `gorm:"column:full_dest_count" json:"full_dest_count,omitempty"`
		FullCountAt              *time.Time `gorm:"column:full_count_at" json:"full_count_at,omitempty"`
		// NULLable source_count — when worker hits SRC_TIMEOUT/SRC_CONNECTION
		// we don't want to conflate "no rows" with "query failed". The base
		// model's SourceCount is int64 (non-null by default=0); we overlay
		// NullableSourceCount by reading the same column into a pointer.
		NullableSourceCount *int64  `gorm:"column:nullable_source_count" json:"nullable_source_count,omitempty"`
		ErrorCode           *string `gorm:"column:error_code" json:"error_code,omitempty"`
		ErrorMessageVI      string  `gorm:"-" json:"error_message_vi,omitempty"`
		DriftPct            float64 `gorm:"-" json:"drift_pct"`
		ComputedStatus      string  `gorm:"-" json:"computed_status"`
		SourceQueryMethod   string  `gorm:"-" json:"source_query_method"`
	}

	var rows []reportRow
	// Primary query — expects worker migration 017 (new fields). LEFT JOIN
	// keeps tables present even if registry row is stale.
	primary := `
		SELECT r.*,
		       r.source_count AS nullable_source_count,
		       reg.sync_engine, reg.source_type, reg.timestamp_field,
		       reg.timestamp_field_source, reg.timestamp_field_confidence,
		       reg.full_source_count, reg.full_dest_count, reg.full_count_at,
		       r.error_code
		  FROM (
			SELECT DISTINCT ON (target_table) *
			  FROM cdc_reconciliation_report
			 ORDER BY target_table, checked_at DESC
		  ) r
		  LEFT JOIN cdc_table_registry reg ON reg.target_table = r.target_table
		 ORDER BY r.target_table
	`
	if err := h.db.Raw(primary).Scan(&rows).Error; err != nil {
		// Migration 017 not applied yet — fall back to legacy SELECT. Keeps
		// the endpoint up on older envs. New fields come out nil/0 which the
		// FE already knows to render as "—".
		rows = rows[:0]
		legacy := `
			SELECT r.*, r.source_count AS nullable_source_count,
			       reg.sync_engine, reg.source_type, reg.timestamp_field
			  FROM (
				SELECT DISTINCT ON (target_table) *
				  FROM cdc_reconciliation_report
				 ORDER BY target_table, checked_at DESC
			  ) r
			  LEFT JOIN cdc_table_registry reg ON reg.target_table = r.target_table
			 ORDER BY r.target_table
		`
		h.db.Raw(legacy).Scan(&rows)
	}

	// Enrich each row: drift computation (FIX 2) + VI error message (FIX 5)
	// + source query method tooltip (legacy).
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

// TableHistory returns reconciliation history for a specific table
func (h *ReconciliationHandler) TableHistory(c *fiber.Ctx) error {
	table := c.Params("table")
	page, _ := strconv.Atoi(c.Query("page", "1"))
	pageSize, _ := strconv.Atoi(c.Query("page_size", "20"))
	if page < 1 { page = 1 }
	if pageSize < 1 || pageSize > 100 { pageSize = 20 }

	var reports []model.ReconciliationReport
	var total int64
	h.db.Model(&model.ReconciliationReport{}).Where("target_table = ?", table).Count(&total)
	h.db.Where("target_table = ?", table).Order("checked_at DESC").
		Offset((page-1)*pageSize).Limit(pageSize).Find(&reports)

	return c.JSON(fiber.Map{"data": reports, "total": total, "page": page})
}

// TriggerCheck dispatches reconciliation check via NATS
func (h *ReconciliationHandler) TriggerCheck(c *fiber.Ctx) error {
	tier := c.Query("tier", "1")
	table := c.Params("table")

	payload, _ := json.Marshal(map[string]string{
		"tier":  tier,
		"table": table,
	})

	subject := "cdc.cmd.recon-check"
	if err := h.nats.Conn.Publish(subject, payload); err != nil {
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}

	// Log activity
	now := time.Now()
	h.db.Create(&model.ActivityLog{
		Operation:   "recon-check",
		TargetTable: table,
		Status:      "success",
		Details:     payload,
		TriggeredBy: "manual",
		StartedAt:   now,
		CompletedAt: &now,
	})

	return c.Status(202).JSON(fiber.Map{"message": "reconciliation check dispatched", "tier": tier, "table": table})
}

// TriggerCheckAll dispatches Tier 1 check for all tables
func (h *ReconciliationHandler) TriggerCheckAll(c *fiber.Ctx) error {
	payload := []byte(`{"tier":"1","table":"*"}`)
	if err := h.nats.Conn.Publish("cdc.cmd.recon-check", payload); err != nil {
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}
	return c.Status(202).JSON(fiber.Map{"message": "tier 1 check dispatched for all tables"})
}

// TriggerHeal dispatches heal for a specific table
func (h *ReconciliationHandler) TriggerHeal(c *fiber.Ctx) error {
	table := c.Params("table")
	payload, _ := json.Marshal(map[string]string{"table": table})

	if err := h.nats.Conn.Publish("cdc.cmd.recon-heal", payload); err != nil {
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}

	now := time.Now()
	h.db.Create(&model.ActivityLog{
		Operation:   "recon-heal-trigger",
		TargetTable: table,
		Status:      "success",
		TriggeredBy: "manual",
		StartedAt:   now,
		CompletedAt: &now,
	})

	return c.Status(202).JSON(fiber.Map{"message": "heal dispatched", "table": table})
}

// ListFailedLogs returns failed sync logs (paginated, filterable)
func (h *ReconciliationHandler) ListFailedLogs(c *fiber.Ctx) error {
	page, _ := strconv.Atoi(c.Query("page", "1"))
	pageSize, _ := strconv.Atoi(c.Query("page_size", "30"))
	if page < 1 { page = 1 }
	if pageSize < 1 || pageSize > 200 { pageSize = 30 }

	query := h.db.Model(&model.FailedSyncLog{}).Order("created_at DESC")
	if t := c.Query("target_table"); t != "" {
		query = query.Where("target_table = ?", t)
	}
	if s := c.Query("status"); s != "" {
		query = query.Where("status = ?", s)
	}
	if et := c.Query("error_type"); et != "" {
		query = query.Where("error_type = ?", et)
	}

	var total int64
	query.Count(&total)

	var logs []model.FailedSyncLog
	query.Offset((page-1)*pageSize).Limit(pageSize).Find(&logs)

	return c.JSON(fiber.Map{"data": logs, "total": total, "page": page})
}

// RetryFailedLog retries a single failed record
func (h *ReconciliationHandler) RetryFailedLog(c *fiber.Ctx) error {
	id, _ := c.ParamsInt("id")

	var log model.FailedSyncLog
	if err := h.db.First(&log, id).Error; err != nil {
		return c.Status(404).JSON(fiber.Map{"error": "record not found"})
	}

	// Dispatch retry via NATS
	payload, _ := json.Marshal(map[string]interface{}{
		"failed_log_id": log.ID,
		"target_table":  log.TargetTable,
		"record_id":     log.RecordID,
		"raw_json":      string(log.RawJSON),
	})

	if err := h.nats.Conn.Publish("cdc.cmd.retry-failed", payload); err != nil {
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}

	// Update status
	now := time.Now()
	h.db.Model(&log).Updates(map[string]interface{}{
		"status":       "retrying",
		"retry_count":  gorm.Expr("retry_count + 1"),
		"last_retry_at": now,
	})

	return c.Status(202).JSON(fiber.Map{"message": "retry dispatched", "id": id})
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

	payload, _ := json.Marshal(map[string]string{
		"type":       "signal-snapshot",
		"database":   body.Database,
		"collection": body.Collection,
	})
	h.nats.Conn.Publish("cdc.cmd.debezium-signal", payload)

	return c.Status(202).JSON(fiber.Map{"message": "debezium signal dispatched"})
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
	payload, _ := json.Marshal(map[string]interface{}{
		"table":      body.Table,
		"run_id":     runID,
		"batch_size": body.BatchSize,
	})

	subject := "cdc.cmd.recon-backfill-source-ts"
	if err := h.nats.Conn.Publish(subject, payload); err != nil {
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}

	now := time.Now()
	h.db.Create(&model.ActivityLog{
		Operation:   "recon-backfill-source-ts",
		TargetTable: body.Table,
		Status:      "dispatched",
		Details:     payload,
		TriggeredBy: "manual",
		StartedAt:   now,
		CompletedAt: &now,
	})

	return c.Status(202).JSON(fiber.Map{
		"message":    "backfill dispatched",
		"run_id":     runID,
		"table":      body.Table,
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
	type runRow struct {
		ID              string     `gorm:"column:id" json:"id"`
		TableName       string     `gorm:"column:table_name" json:"table_name"`
		Tier            int        `gorm:"column:tier" json:"tier"`
		Status          string     `gorm:"column:status" json:"status"`
		StartedAt       time.Time  `gorm:"column:started_at" json:"started_at"`
		FinishedAt      *time.Time `gorm:"column:finished_at" json:"finished_at"`
		DocsScanned    int64      `gorm:"column:docs_scanned" json:"docs_scanned"`
		HealActions    int64      `gorm:"column:heal_actions" json:"heal_actions"`
		ErrorMessage   *string    `gorm:"column:error_message" json:"error_message"`
		InstanceID     *string    `gorm:"column:instance_id" json:"instance_id"`
	}
	var rows []runRow

	q := h.db.Table("recon_runs").
		Where("tier = ?", 4).
		Order("started_at DESC").
		Limit(30)
	if t := c.Query("table"); t != "" {
		q = q.Where("table_name = ?", t)
	}
	if rid := c.Query("run_id"); rid != "" {
		q = q.Where("instance_id = ?", "backfill:"+rid)
	}
	q.Scan(&rows)

	// Enrich with per-table total + remaining so FE can compute progress
	// without an extra trip. Bound by input table names to keep it cheap.
	type enriched struct {
		runRow
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
		var total, nul int64
		// Safe: r.TableName comes from recon_runs row we just wrote.
		h.db.Raw("SELECT COUNT(*) FROM " + pgIdent(r.TableName)).Scan(&total)
		h.db.Raw("SELECT COUNT(*) FROM " + pgIdent(r.TableName) + " WHERE _source_ts IS NULL").Scan(&nul)
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
			runRow:        r,
			TotalRows:     total,
			NullRemaining: nul,
			PercentDone:   pct,
		})
	}

	return c.JSON(fiber.Map{"data": out, "total": len(out)})
}

// pgIdent quotes a Postgres identifier safely. Only alphanumerics +
// underscore allowed; anything else returns an empty-string-quoted
// identifier which will fail the SQL parser deliberately.
func pgIdent(name string) string {
	if name == "" {
		return `""`
	}
	for _, r := range name {
		if !(r == '_' || (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9')) {
			return `""`
		}
	}
	return `"` + name + `"`
}

// Tools: Trigger snapshot for a table
func (h *ReconciliationHandler) TriggerSnapshot(c *fiber.Ctx) error {
	table := c.Params("table")
	payload, _ := json.Marshal(map[string]string{"table": table})
	h.nats.Conn.Publish("cdc.cmd.debezium-snapshot", payload)
	return c.Status(202).JSON(fiber.Map{"message": "snapshot signal dispatched", "table": table})
}
