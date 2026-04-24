package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"centralized-data-service/internal/repository"
	"centralized-data-service/pkgs/natsconn"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"
)

// EventBridge listens to PostgreSQL changes and publishes minimized NATS events for Moleculer services.
// Critical tables: LISTEN/NOTIFY (realtime <10ms)
// Non-critical tables: Polling changelog (1-5s interval)
type EventBridge struct {
	pool         *pgxpool.Pool
	nats         *natsconn.NatsClient
	registryRepo *repository.RegistryRepo
	logger       *zap.Logger
	stopCh       chan struct{}
}

// MoleculerEvent is the CloudEvents-compatible format for downstream services.
// Data payloads are minimized to metadata only; raw row images are not forwarded.
type MoleculerEvent struct {
	SpecVersion string    `json:"specversion"`
	Source      string    `json:"source"`
	Type        string    `json:"type"`
	Time        string    `json:"time"`
	Data        EventData `json:"data"`
}

type EventData struct {
	Op     string                 `json:"op"` // c=create, u=update, d=delete
	Table  string                 `json:"table"`
	Before map[string]interface{} `json:"before,omitempty"`
	After  map[string]interface{} `json:"after,omitempty"`
}

func NewEventBridge(pool *pgxpool.Pool, nats *natsconn.NatsClient, registryRepo *repository.RegistryRepo, logger *zap.Logger) *EventBridge {
	return &EventBridge{
		pool:         pool,
		nats:         nats,
		registryRepo: registryRepo,
		logger:       logger,
		stopCh:       make(chan struct{}),
	}
}

// StartTriggerListener listens for PostgreSQL NOTIFY events on critical tables
func (eb *EventBridge) StartTriggerListener(ctx context.Context, channel string) {
	go func() {
		conn, err := eb.pool.Acquire(ctx)
		if err != nil {
			eb.logger.Error("event bridge: failed to acquire connection", zap.Error(err))
			return
		}
		defer conn.Release()

		_, err = conn.Exec(ctx, fmt.Sprintf("LISTEN %s", channel))
		if err != nil {
			eb.logger.Error("event bridge: LISTEN failed", zap.String("channel", channel), zap.Error(err))
			return
		}

		eb.logger.Info("event bridge: listening", zap.String("channel", channel))

		for {
			select {
			case <-ctx.Done():
				return
			case <-eb.stopCh:
				return
			default:
				notification, err := conn.Conn().WaitForNotification(ctx)
				if err != nil {
					if ctx.Err() != nil {
						return
					}
					eb.logger.Warn("event bridge: notification error", zap.Error(err))
					continue
				}

				eb.publishEvent(notification.Channel, notification.Payload)
			}
		}
	}()
}

// StartPoller polls non-critical tables for changes at a fixed interval
func (eb *EventBridge) StartPoller(ctx context.Context, interval time.Duration) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		eb.logger.Info("event bridge: poller started", zap.Duration("interval", interval))

		for {
			select {
			case <-ctx.Done():
				return
			case <-eb.stopCh:
				return
			case <-ticker.C:
				eb.pollChanges(ctx)
			}
		}
	}()
}

func (eb *EventBridge) pollChanges(ctx context.Context) {
	entries, err := eb.registryRepo.GetAllActive(ctx)
	if err != nil {
		return
	}

	for _, entry := range entries {
		if entry.Priority == "critical" || entry.Priority == "high" {
			continue // handled by trigger listener
		}

		// Check _updated_at for recent changes
		var count int64
		eb.pool.QueryRow(ctx,
			fmt.Sprintf(`SELECT COUNT(*) FROM "%s" WHERE _updated_at > NOW() - INTERVAL '10 seconds'`, entry.TargetTable),
		).Scan(&count)

		if count > 0 {
			eb.publishEvent("cdc_change", fmt.Sprintf(`{"table":"%s","count":%d}`, entry.TargetTable, count))
		}
	}
}

func (eb *EventBridge) publishEvent(channel, payload string) {
	var data map[string]interface{}
	json.Unmarshal([]byte(payload), &data)

	table, _ := data["table"].(string)
	if table == "" {
		table = channel
	}

	minimized := minimizeBridgePayload(table, data)

	event := MoleculerEvent{
		SpecVersion: "1.0",
		Source:      fmt.Sprintf("/cdc/postgres/goopay/%s", table),
		Type:        "io.goopay.datachangeevent",
		Time:        time.Now().Format(time.RFC3339),
		Data: EventData{
			Op:    "u",
			Table: table,
			After: minimized,
		},
	}

	eventJSON, _ := json.Marshal(event)
	subject := fmt.Sprintf("moleculer.event.%s", table)

	if err := eb.nats.Conn.Publish(subject, eventJSON); err != nil {
		eb.logger.Error("event bridge: publish failed", zap.String("subject", subject), zap.Error(err))
	}
}

func (eb *EventBridge) Stop() {
	close(eb.stopCh)
}

func minimizeBridgePayload(table string, data map[string]interface{}) map[string]interface{} {
	out := map[string]interface{}{
		"table": table,
	}
	if len(data) == 0 {
		return out
	}

	if count, ok := data["count"]; ok {
		out["count"] = count
	}
	if op, ok := data["op"].(string); ok && op != "" {
		out["op"] = op
	}
	if source, ok := data["source"].(string); ok && source != "" {
		out["source"] = source
	}
	if id := firstBridgeID(data); id != "" {
		out["record_id"] = id
	}

	keys := minimizedChangedFields(data)
	if len(keys) > 0 {
		out["changed_fields"] = keys
	}

	return out
}

func firstBridgeID(data map[string]interface{}) string {
	if id := stringifyBridgeValue(data["record_id"]); id != "" {
		return id
	}
	if id := stringifyBridgeValue(data["id"]); id != "" {
		return id
	}
	if after, ok := data["after"].(map[string]interface{}); ok {
		if id := stringifyBridgeValue(after["_id"]); id != "" {
			return id
		}
		if id := stringifyBridgeValue(after["id"]); id != "" {
			return id
		}
	}
	return ""
}

func stringifyBridgeValue(value interface{}) string {
	switch v := value.(type) {
	case string:
		return v
	case float64:
		return fmt.Sprintf("%.0f", v)
	case int:
		return fmt.Sprintf("%d", v)
	case int64:
		return fmt.Sprintf("%d", v)
	case map[string]interface{}:
		if oid, ok := v["$oid"].(string); ok {
			return oid
		}
	}
	return ""
}

func minimizedChangedFields(data map[string]interface{}) []string {
	fieldSet := make(map[string]struct{})
	for key := range data {
		switch key {
		case "table", "count", "op", "source", "record_id", "id", "after", "before":
			continue
		default:
			fieldSet[key] = struct{}{}
		}
	}
	for _, side := range []string{"after", "before"} {
		nested, ok := data[side].(map[string]interface{})
		if !ok {
			continue
		}
		for key := range nested {
			fieldSet[key] = struct{}{}
		}
	}
	if len(fieldSet) == 0 {
		return nil
	}
	fields := make([]string, 0, len(fieldSet))
	for key := range fieldSet {
		fields = append(fields, key)
	}
	sort.Strings(fields)
	return fields
}
