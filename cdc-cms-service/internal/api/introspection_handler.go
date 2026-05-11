package api

import (
	"encoding/json"
	"fmt"
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

// ... các method Scan/ScanRawData giữ nguyên ...

func (h *IntrospectionHandler) DiscoverMongoDatabases(c *fiber.Ctx) error {
	host := c.Query("host", "localhost")
	port := c.Query("port", "17017")
	
	correlationID := fmt.Sprintf("mongo-discovery-%d", time.Now().UnixNano())
	replySubj := "cdc.evt.introspect.mongo.databases." + correlationID

	payload, _ := json.Marshal(map[string]string{
		"host": host,
		"port": port,
		"reply_to": replySubj,
	})

	// Lắng nghe kết quả trên subject cụ thể
	sub, err := h.natsClient.Conn.SubscribeSync(replySubj)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": "failed to subscribe for response"})
	}
	defer sub.Unsubscribe()

	// Publish lệnh
	if err := h.natsClient.Conn.Publish("cdc.cmd.introspect.mongo.databases", payload); err != nil {
		return c.Status(500).JSON(fiber.Map{"error": "failed to publish discovery command"})
	}

	// Chờ kết quả (timeout 10s)
	msg, err := sub.NextMsg(10 * time.Second)
	if err != nil {
		return c.Status(fiber.StatusServiceUnavailable).JSON(fiber.Map{
			"error": "Worker timeout or no response on " + replySubj,
		})
	}

	var resp struct {
		Databases []string `json:"databases"`
		Error     string   `json:"error"`
	}
	if err := json.Unmarshal(msg.Data, &resp); err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": "Failed to parse worker response",
		})
	}

	if resp.Error != "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": resp.Error,
		})
	}

	return c.JSON(resp)
}

func (h *IntrospectionHandler) DiscoverMongoCollections(c *fiber.Ctx) error {
	db := c.Params("db")
	host := c.Query("host", "localhost")
	port := c.Query("port", "17017")

	correlationID := fmt.Sprintf("mongo-discovery-cols-%d", time.Now().UnixNano())
	replySubj := "cdc.evt.introspect.mongo.collections." + correlationID

	payload, _ := json.Marshal(map[string]string{
		"host": host,
		"port": port,
		"db":   db,
		"reply_to": replySubj,
	})

	sub, err := h.natsClient.Conn.SubscribeSync(replySubj)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": "failed to subscribe for response"})
	}
	defer sub.Unsubscribe()

	if err := h.natsClient.Conn.Publish("cdc.cmd.introspect.mongo.collections", payload); err != nil {
		return c.Status(500).JSON(fiber.Map{"error": "failed to publish discovery command"})
	}

	msg, err := sub.NextMsg(10 * time.Second)
	if err != nil {
		return c.Status(fiber.StatusServiceUnavailable).JSON(fiber.Map{
			"error": "Worker timeout or no response",
		})
	}

	var resp struct {
		Collections []string `json:"collections"`
		Error       string   `json:"error"`
	}
	if err := json.Unmarshal(msg.Data, &resp); err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": "Failed to parse worker response",
		})
	}

	if resp.Error != "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": resp.Error,
		})
	}

	return c.JSON(resp)
}

// Giữ lại Scan và ScanRawData ở đây
func (h *IntrospectionHandler) Scan(c *fiber.Ctx) error {
	targetTable := c.Params("table")
	correlationID := fmt.Sprintf("scan-%s-%d", targetTable, time.Now().UnixNano())
	replySubj := "cdc.evt.scan.raw." + correlationID

	payload, _ := json.Marshal(map[string]string{
		"target_table": targetTable,
		"reply_to":     replySubj,
	})

	sub, err := h.natsClient.Conn.SubscribeSync(replySubj)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": "failed to subscribe for response"})
	}
	defer sub.Unsubscribe()

	if err := h.natsClient.Conn.Publish("cdc.cmd.scan-raw-data", payload); err != nil {
		return c.Status(500).JSON(fiber.Map{"error": "failed to publish scan command"})
	}

	msg, err := sub.NextMsg(10 * time.Second)
	if err == nil {
		var res map[string]interface{}
		if err := json.Unmarshal(msg.Data, &res); err == nil {
			if status, _ := res["status"].(string); status == "ok" {
				return c.JSON(res)
			}
		}
	}

	// Fallback to introspect if scan-raw fails or returns nothing
	correlationID = fmt.Sprintf("introspect-%s-%d", targetTable, time.Now().UnixNano())
	replySubj = "cdc.evt.introspect." + correlationID
	sub2, err := h.natsClient.Conn.SubscribeSync(replySubj)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": "failed to subscribe for introspect response"})
	}
	defer sub2.Unsubscribe()

	payload, _ = json.Marshal(map[string]interface{}{
		"target_table": targetTable,
		"reply_to":     replySubj,
	})
	if err := h.natsClient.Conn.Publish("cdc.cmd.introspect", payload); err != nil {
		return c.Status(500).JSON(fiber.Map{"error": "failed to publish introspect command: " + err.Error()})
	}

	msg, err = sub2.NextMsg(10 * time.Second)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": "failed to request introspection: " + err.Error()})
	}

	var res map[string]interface{}
	json.Unmarshal(msg.Data, &res)
	return c.JSON(res)
}

func (h *IntrospectionHandler) ScanRawData(c *fiber.Ctx) error {
	targetTable := c.Params("table")
	correlationID := fmt.Sprintf("scan-raw-%s-%d", targetTable, time.Now().UnixNano())
	replySubj := "cdc.evt.scan.raw." + correlationID

	payload, _ := json.Marshal(map[string]string{
		"target_table": targetTable,
		"reply_to":     replySubj,
	})

	sub, err := h.natsClient.Conn.SubscribeSync(replySubj)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": "failed to subscribe for response"})
	}
	defer sub.Unsubscribe()

	if err := h.natsClient.Conn.Publish("cdc.cmd.scan-raw-data", payload); err != nil {
		return c.Status(500).JSON(fiber.Map{"error": "failed to publish scan command"})
	}

	msg, err := sub.NextMsg(15 * time.Second)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": "failed to request scan: " + err.Error()})
	}

	var res map[string]interface{}
	json.Unmarshal(msg.Data, &res)
	return c.JSON(res)
}
