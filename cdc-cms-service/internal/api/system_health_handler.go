// Package api — system_health_handler.go (v3)
//
// The handler is deliberately thin: it reads the latest Snapshot from Redis
// (written by `service.Collector` every 15s) and returns it. This keeps the
// API p99 well under 50ms even during a cascading outage elsewhere, because
// no external call is made on the hot path.
//
// Error shapes:
//   - Redis returns nil (no snapshot yet): HTTP 503 {"status": "initializing"}.
//   - Any other Redis error: HTTP 503 {"status": "initializing", "error": ...}.
//   - Snapshot JSON corrupt: HTTP 500.
//
// Backward compatibility: the JSON keys match the v2 handler one-for-one
// (timestamp, overall, infrastructure, cdc_pipeline, reconciliation, latency,
// failed_sync, alerts, recent_events). A new top-level field `cache_age_seconds`
// is added; existing FE code ignores unknown keys.
//
// Legacy responsibilities preserved here: the `RestartDebezium` action
// endpoint is unchanged — it still proxies POST to Kafka Connect. (Scope note
// per the task brief: we only touch system-health; Restart stays put because
// it lives in this file by history and the FE calls it via the existing
// route.)
package api

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"cdc-cms-service/internal/app/commands"
	"cdc-cms-service/internal/app/ports"
	"cdc-cms-service/internal/infra/messaging"
	"cdc-cms-service/internal/middleware"
	"cdc-cms-service/internal/service"
	"cdc-cms-service/pkgs/natsconn"
	"cdc-cms-service/pkgs/rediscache"

	"github.com/gofiber/fiber/v2"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

// SystemHealthHandler reads cached snapshots from Redis and serves them.
type SystemHealthHandler struct {
	redisCache      *rediscache.RedisCache
	natsClient      *natsconn.NatsClient
	bus             ports.CommandBus
	kafkaConnectURL string
	cacheKey        string
	debeziumName    string
	logger          *zap.Logger
}

// NewSystemHealthHandler constructs the handler.
func NewSystemHealthHandler(
	redisCache *rediscache.RedisCache,
	natsClient *natsconn.NatsClient,
	bus ports.CommandBus,
	kafkaConnectURL string,
	cacheKey string,
	debeziumName string,
	logger *zap.Logger,
) *SystemHealthHandler {
	if cacheKey == "" {
		cacheKey = "system_health:snapshot"
	}
	if debeziumName == "" {
		debeziumName = "goopay-mongodb-cdc"
	}
	return &SystemHealthHandler{
		redisCache:      redisCache,
		natsClient:      natsClient,
		bus:             bus,
		kafkaConnectURL: kafkaConnectURL,
		cacheKey:        cacheKey,
		debeziumName:    debeziumName,
		logger:          logger,
	}
}

// Health returns the cached snapshot + cache_age_seconds.
func (h *SystemHealthHandler) Health(c *fiber.Ctx) error {
	if h.redisCache == nil {
		return c.Status(503).JSON(fiber.Map{
			"status":  "initializing",
			"message": "redis not configured",
		})
	}
	ctx, cancel := context.WithTimeout(c.Context(), 500*time.Millisecond)
	defer cancel()

	raw, err := h.redisCache.Get(ctx, h.cacheKey)
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return c.Status(503).JSON(fiber.Map{
				"status":  "initializing",
				"message": "collector not ready yet",
			})
		}
		h.logger.Warn("redis GET snapshot failed", zap.Error(err))
		return c.Status(503).JSON(fiber.Map{
			"status":  "initializing",
			"message": "cache unavailable",
		})
	}

	var snap service.Snapshot
	if err := json.Unmarshal([]byte(raw), &snap); err != nil {
		h.logger.Error("snapshot unmarshal failed", zap.Error(err))
		return c.Status(500).JSON(fiber.Map{"error": "snapshot corrupt"})
	}

	// Compute age against cached timestamp (not wall-clock at write time).
	age := int(time.Since(snap.Timestamp).Seconds())
	if age < 0 {
		age = 0
	}
	snap.CacheAgeSeconds = age

	return c.JSON(snap)
}

// RestartDebezium dispatches a restart command via CommandBus. CMS không còn gọi
// Kafka Connect REST trực tiếp (Rule B: external mutate thuộc Worker).
func (h *SystemHealthHandler) RestartDebezium(c *fiber.Ctx) error {
	if h.bus == nil {
		return c.Status(503).JSON(fiber.Map{"error": "command bus not configured"})
	}

	user := middleware.GetUsername(c)
	ctx := messaging.WithMetadata(c.UserContext(), user, c.Get("X-Correlation-Id"), c.Get("Idempotency-Key"))
	cmd := commands.RestartDebeziumCommand{
		ConnectorName:   h.debeziumName,
		KafkaConnectURL: h.kafkaConnectURL,
	}
	res, derr := h.bus.Dispatch(ctx, cmd)
	if derr != nil {
		return c.Status(500).JSON(fiber.Map{"error": "dispatch failed: " + derr.Error()})
	}

	return c.Status(202).JSON(fiber.Map{
		"message":        "restart-debezium command accepted",
		"connector_name": h.debeziumName,
		"job_id":         res.JobID,
	})
}
