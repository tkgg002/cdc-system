package api

import (
	"encoding/json"
	"time"

	"cdc-cms-service/pkgs/natsconn"

	"github.com/gofiber/fiber/v2"
)

type IntrospectionHandler struct {
	natsClient *natsconn.NatsClient
}

func NewIntrospectionHandler(natsClient *natsconn.NatsClient) *IntrospectionHandler {
	return &IntrospectionHandler{
		natsClient: natsClient,
	}
}

// Scan godoc
// @Summary      Scan raw data for new fields
// @Description  Scans the _raw_data column of a target table to find fields that are not yet mapped
// @Tags         Introspection
// @Produce      json
// @Param        table   path string true "Target table name"
// @Success      200 {object} map[string]interface{}
// @Failure      404 {object} map[string]string
// @Failure      500 {object} map[string]string
// @Security     BearerAuth
// @Router       /api/introspection/scan/{table} [get]
func (h *IntrospectionHandler) Scan(c *fiber.Ctx) error {
	targetTable := c.Params("table")

	msg, err := h.natsClient.Conn.Request("cdc.cmd.scan-raw-data", []byte(targetTable), 10*time.Second)
	if err == nil {
		var res map[string]interface{}
		if err := json.Unmarshal(msg.Data, &res); err == nil {
			if status, _ := res["status"].(string); status == "ok" {
				return c.JSON(res)
			}
		}
	}

	payload, _ := json.Marshal(map[string]interface{}{
		"target_table": targetTable,
	})
	msg, err = h.natsClient.Conn.Request("cdc.cmd.introspect", payload, 10*time.Second)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": "failed to request introspection: " + err.Error()})
	}

	var res map[string]interface{}
	if err := json.Unmarshal(msg.Data, &res); err != nil {
		return c.Status(500).JSON(fiber.Map{"error": "invalid response from worker"})
	}

	if status, ok := res["status"].(string); ok && status == "error" {
		if errMsg, ok := res["error"].(string); ok && errMsg == "table not registered" {
			return c.Status(404).JSON(fiber.Map{"error": errMsg})
		}
		return c.Status(500).JSON(fiber.Map{"error": res["error"]})
	}

	return c.JSON(res)
}

// ScanRawData godoc
// @Summary      Scan _raw_data JSONB for unmapped fields
// @Tags         Introspection
// @Produce      json
// @Param        table   path string true "Target table name (e.g. cdc_merchants)"
// @Success      200 {object} map[string]interface{}
// @Failure      500 {object} map[string]string
// @Security     BearerAuth
// @Router       /api/introspection/scan-raw/{table} [get]
func (h *IntrospectionHandler) ScanRawData(c *fiber.Ctx) error {
	targetTable := c.Params("table")

	msg, err := h.natsClient.Conn.Request("cdc.cmd.scan-raw-data", []byte(targetTable), 10*time.Second)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": "failed to request scan: " + err.Error()})
	}

	var res map[string]interface{}
	if err := json.Unmarshal(msg.Data, &res); err != nil {
		return c.Status(500).JSON(fiber.Map{"error": "invalid response from worker"})
	}

	if status, ok := res["status"].(string); ok && status == "error" {
		return c.Status(500).JSON(fiber.Map{"error": res["error"]})
	}

	return c.JSON(res)
}
