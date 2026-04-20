package handler

import (
	"context"
	"crypto/md5"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"centralized-data-service/pkgs/idgen"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nats-io/nats.go"
	"github.com/tidwall/gjson"
	"go.uber.org/zap"
)

type cdcRow struct {
	id       uint64
	sourceID string
	rawData  []byte
	syncedAt time.Time
	hash     string
}

// BridgeBatchHandler handles high-throughput Airbyte → CDC bridge using pgx.CopyFrom.
// Uses Sonyflake IDs + gjson for v1.12 schema tables.
type BridgeBatchHandler struct {
	pool   *pgxpool.Pool
	logger *zap.Logger
}

func NewBridgeBatchHandler(pool *pgxpool.Pool, logger *zap.Logger) *BridgeBatchHandler {
	return &BridgeBatchHandler{pool: pool, logger: logger}
}

// HandleAirbyteBridgeBatch processes bridge using Go-based batch pipeline:
// 1. Keyset Pagination read from Airbyte table
// 2. gjson parse for source ID extraction
// 3. Sonyflake ID generation
// 4. pgx.CopyFrom for maximum insert throughput
// Subject: "cdc.cmd.bridge-airbyte-batch"
func (h *BridgeBatchHandler) HandleAirbyteBridgeBatch(msg *nats.Msg) {
	var payload struct {
		TargetTable     string `json:"target_table"`
		AirbyteRawTable string `json:"airbyte_raw_table"`
		PrimaryKeyField string `json:"primary_key_field"`
		LastBridgeAt    string `json:"last_bridge_at"`
		BatchSize       int    `json:"batch_size"`
	}
	if err := json.Unmarshal(msg.Data, &payload); err != nil {
		h.respond(msg, "error", 0, "invalid payload: "+err.Error())
		return
	}

	if payload.BatchSize <= 0 {
		payload.BatchSize = 1000
	}

	airbyteTable := payload.AirbyteRawTable
	if airbyteTable == "" {
		airbyteTable = strings.ReplaceAll(payload.TargetTable, "-", "_")
	}

	pkField := payload.PrimaryKeyField
	if pkField == "" {
		pkField = "_id"
	}

	ctx := context.Background()
	start := time.Now()

	// 1. Read from Airbyte table using Keyset Pagination
	var whereClause string
	if payload.LastBridgeAt != "" {
		whereClause = fmt.Sprintf("WHERE _airbyte_extracted_at > '%s'", payload.LastBridgeAt)
	}

	query := fmt.Sprintf(
		`SELECT to_jsonb(src.*) AS row_data, src._airbyte_extracted_at
		FROM "%s" src %s
		ORDER BY _airbyte_extracted_at ASC`,
		airbyteTable, whereClause,
	)

	rows, err := h.pool.Query(ctx, query)
	if err != nil {
		h.respond(msg, "error", 0, "query airbyte table: "+err.Error())
		return
	}
	defer rows.Close()

	// 2. Collect rows + generate Sonyflake IDs + extract source IDs via gjson
	var batch []cdcRow
	totalRows := 0

	for rows.Next() {
		var rowData []byte
		var extractedAt *time.Time
		if err := rows.Scan(&rowData, &extractedAt); err != nil {
			h.logger.Warn("scan row failed", zap.Error(err))
			continue
		}

		// Extract source ID using gjson
		sourceID := gjson.GetBytes(rowData, pkField).String()
		if sourceID == "" {
			sourceID = gjson.GetBytes(rowData, "_id.\\$oid").String()
		}
		if sourceID == "" {
			sourceID = gjson.GetBytes(rowData, "id").String()
		}
		if sourceID == "" {
			continue // Skip rows without identifiable PK
		}

		// Generate Sonyflake ID
		sfID, err := idgen.NextID()
		if err != nil {
			h.logger.Warn("sonyflake ID generation failed", zap.Error(err))
			continue
		}

		syncedAt := time.Now()
		if extractedAt != nil {
			syncedAt = *extractedAt
		}

		// Strip Airbyte internal columns from raw data
		cleanJSON := stripAirbyteColumns(rowData)

		batch = append(batch, cdcRow{
			id:       sfID,
			sourceID: sourceID,
			rawData:  cleanJSON,
			syncedAt: syncedAt,
			hash:     fmt.Sprintf("%x", md5.Sum(cleanJSON)),
		})
		totalRows++

		// Flush batch
		if len(batch) >= payload.BatchSize {
			if err := h.flushBatch(ctx, payload.TargetTable, batch); err != nil {
				h.respond(msg, "error", totalRows, "flush batch: "+err.Error())
				return
			}
			batch = batch[:0]
		}
	}

	// Flush remaining
	if len(batch) > 0 {
		if err := h.flushBatch(ctx, payload.TargetTable, batch); err != nil {
			h.respond(msg, "error", totalRows, "flush final batch: "+err.Error())
			return
		}
	}

	elapsed := time.Since(start)
	rps := float64(totalRows) / elapsed.Seconds()
	h.logger.Info("batch bridge completed",
		zap.String("table", payload.TargetTable),
		zap.Int("rows", totalRows),
		zap.Duration("elapsed", elapsed),
		zap.Float64("rows_per_sec", rps),
	)
	h.respond(msg, "success", totalRows, "")
}

// flushBatch writes rows to CDC table using pgx upsert (ON CONFLICT source_id)
func (h *BridgeBatchHandler) flushBatch(ctx context.Context, targetTable string, rows []cdcRow) error {
	if len(rows) == 0 {
		return nil
	}

	// Build batch upsert
	batch := &pgx.Batch{}
	upsertSQL := fmt.Sprintf(
		`INSERT INTO "%s" (id, source_id, _raw_data, _source, _synced_at, _hash, _version, _created_at, _updated_at)
		VALUES ($1, $2, $3, 'airbyte', $4, $5, 1, NOW(), NOW())
		ON CONFLICT (source_id) DO UPDATE SET
			_raw_data = EXCLUDED._raw_data,
			_synced_at = EXCLUDED._synced_at,
			_hash = EXCLUDED._hash,
			_version = "%s"._version + 1,
			_updated_at = NOW()
		WHERE "%s"._hash IS DISTINCT FROM EXCLUDED._hash`,
		targetTable, targetTable, targetTable,
	)

	for _, row := range rows {
		batch.Queue(upsertSQL, row.id, row.sourceID, row.rawData, row.syncedAt, row.hash)
	}

	br := h.pool.SendBatch(ctx, batch)
	defer br.Close()

	for range rows {
		if _, err := br.Exec(); err != nil {
			return fmt.Errorf("batch exec: %w", err)
		}
	}

	return nil
}

func (h *BridgeBatchHandler) respond(msg *nats.Msg, status string, rows int, errMsg string) {
	result := CommandResult{
		Command:      "bridge-airbyte-batch",
		RowsAffected: rows,
		Status:       status,
		Error:        errMsg,
	}
	data, _ := json.Marshal(result)
	if msg.Reply != "" {
		msg.Respond(data)
	}
}

// stripAirbyteColumns removes Airbyte internal columns from JSONB
func stripAirbyteColumns(data []byte) []byte {
	parsed := gjson.ParseBytes(data)
	if !parsed.IsObject() {
		return data
	}

	result := make(map[string]json.RawMessage)
	parsed.ForEach(func(key, value gjson.Result) bool {
		k := key.String()
		if strings.HasPrefix(k, "_airbyte_") {
			return true // skip
		}
		result[k] = json.RawMessage(value.Raw)
		return true
	})

	out, _ := json.Marshal(result)
	return out
}

