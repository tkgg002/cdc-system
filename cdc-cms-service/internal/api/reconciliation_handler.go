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

type ReconciliationHandler struct {
	db   *gorm.DB
	nats *natsconn.NatsClient
}

func NewReconciliationHandler(db *gorm.DB, nats *natsconn.NatsClient) *ReconciliationHandler {
	return &ReconciliationHandler{db: db, nats: nats}
}

// LatestReport returns the latest reconciliation report per table
func (h *ReconciliationHandler) LatestReport(c *fiber.Ctx) error {
	var reports []model.ReconciliationReport
	h.db.Raw(`
		SELECT DISTINCT ON (target_table) *
		FROM cdc_reconciliation_report
		ORDER BY target_table, checked_at DESC
	`).Scan(&reports)

	return c.JSON(fiber.Map{"data": reports, "total": len(reports)})
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
