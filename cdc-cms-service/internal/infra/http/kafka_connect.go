// Package http — outbound HTTP clients.
//
// kafka_connect.go is the typed Kafka Connect REST client. It owns
// all transport plumbing (URL composition, JSON marshal, status-code
// handling). The CMS API handler (system_connectors_handler.go)
// delegates here for both reads and writes; reads are wrapped
// further by app/queries/list_connectors.go to give a CQRS-symmetric
// shape (handler → query → port → adapter).
//
// The wire shapes (ConnectorView, ConnectorStatusResp, ConnectorTask,
// ConnectorState) are kept here because they correspond 1:1 with the
// Kafka Connect REST schema; queries/ uses them via the port.
package http

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// ConnectorState mirrors the connector-level status block returned by
// Kafka Connect REST.
type ConnectorState struct {
	State    string `json:"state"`
	WorkerID string `json:"worker_id"`
}

// ConnectorTask mirrors one task element of Kafka Connect's
// /connectors/:name/status response.
type ConnectorTask struct {
	ID       int    `json:"id"`
	State    string `json:"state"`
	WorkerID string `json:"worker_id"`
	Trace    string `json:"trace,omitempty"`
}

// ConnectorStatusResp is the full /connectors/:name/status payload.
type ConnectorStatusResp struct {
	Type      string          `json:"type"`
	Connector ConnectorState  `json:"connector"`
	Tasks     []ConnectorTask `json:"tasks"`
}

// ConnectorView is the projected list-shape used by GET
// /api/v1/system/connectors. JSON tags pin the wire contract.
type ConnectorView struct {
	Name      string            `json:"name"`
	State     string            `json:"state"`
	Type      string            `json:"type"`
	Connector string            `json:"connector_class"`
	Tasks     []ConnectorTask   `json:"tasks"`
	Config    map[string]string `json:"config,omitempty"`
}

// KafkaConnectClient is a thin typed wrapper around the Kafka Connect
// REST API.
type KafkaConnectClient struct {
	baseURL    string
	httpClient *http.Client
}

// NewKafkaConnectClient constructs the client. Trailing slash on
// baseURL is normalized away to keep relative-path concatenation safe.
func NewKafkaConnectClient(baseURL string) *KafkaConnectClient {
	return &KafkaConnectClient{
		baseURL:    strings.TrimRight(baseURL, "/"),
		httpClient: &http.Client{Timeout: 60 * time.Second},
	}
}

// ListNames returns the connector names registered with Kafka Connect.
func (c *KafkaConnectClient) ListNames(ctx context.Context) ([]string, error) {
	var names []string
	if err := c.doJSON(ctx, http.MethodGet, "/connectors", nil, &names); err != nil {
		return nil, err
	}
	return names, nil
}

// GetStatus returns the connector + task-level status block.
func (c *KafkaConnectClient) GetStatus(ctx context.Context, name string) (ConnectorStatusResp, error) {
	var s ConnectorStatusResp
	err := c.doJSON(ctx, http.MethodGet,
		"/connectors/"+url.PathEscape(name)+"/status", nil, &s)
	return s, err
}

// GetConfig returns the (raw, unfiltered) connector config map.
func (c *KafkaConnectClient) GetConfig(ctx context.Context, name string) (map[string]string, error) {
	var cfg map[string]string
	err := c.doJSON(ctx, http.MethodGet,
		"/connectors/"+url.PathEscape(name)+"/config", nil, &cfg)
	return cfg, err
}

// ListPlugins returns installed connector plugins.
func (c *KafkaConnectClient) ListPlugins(ctx context.Context) ([]map[string]any, error) {
	var plugins []map[string]any
	if err := c.doJSON(ctx, http.MethodGet, "/connector-plugins", nil, &plugins); err != nil {
		return nil, err
	}
	return plugins, nil
}

// Restart triggers a full connector restart (connector + tasks).
func (c *KafkaConnectClient) Restart(ctx context.Context, name string) error {
	path := fmt.Sprintf("/connectors/%s/restart?includeTasks=true&onlyFailed=false", url.PathEscape(name))
	return c.doJSON(ctx, http.MethodPost, path, nil, nil)
}

// RestartTask restarts one task on a connector.
func (c *KafkaConnectClient) RestartTask(ctx context.Context, name, taskID string) error {
	path := fmt.Sprintf("/connectors/%s/tasks/%s/restart", url.PathEscape(name), url.PathEscape(taskID))
	return c.doJSON(ctx, http.MethodPost, path, nil, nil)
}

// Create POSTs a new connector. Returns the upstream response payload.
func (c *KafkaConnectClient) Create(ctx context.Context, name string, cfg map[string]string) (map[string]any, error) {
	payload := map[string]any{"name": name, "config": cfg}
	var resp map[string]any
	err := c.doJSON(ctx, http.MethodPost, "/connectors", payload, &resp)
	return resp, err
}

// UpdateConfig replaces the connector config using Kafka Connect's
// PUT /connectors/:name/config endpoint.
func (c *KafkaConnectClient) UpdateConfig(ctx context.Context, name string, cfg map[string]string) (map[string]any, error) {
	var resp map[string]any
	err := c.doJSON(ctx, http.MethodPut, "/connectors/"+url.PathEscape(name)+"/config", cfg, &resp)
	return resp, err
}

// Delete removes a connector.
func (c *KafkaConnectClient) Delete(ctx context.Context, name string) error {
	return c.doJSON(ctx, http.MethodDelete, "/connectors/"+url.PathEscape(name), nil, nil)
}

// Lifecycle dispatches pause/resume (op = "pause" or "resume").
func (c *KafkaConnectClient) Lifecycle(ctx context.Context, name, op string) error {
	path := fmt.Sprintf("/connectors/%s/%s", url.PathEscape(name), op)
	return c.doJSON(ctx, http.MethodPut, path, nil, nil)
}

func (c *KafkaConnectClient) doJSON(ctx context.Context, method, relPath string, body any, target any) error {
	u := c.baseURL + relPath
	log.Printf("[KafkaConnect] %s %s", method, u)
	start := time.Now()

	var reqBody io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return fmt.Errorf("marshal body: %w", err)
		}
		reqBody = strings.NewReader(string(b))
	}

	req, err := http.NewRequestWithContext(ctx, method, u, reqBody)
	if err != nil {
		return fmt.Errorf("new request: %w", err)
	}
	req.Header.Set("Accept", "application/json")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := c.httpClient.Do(req)
	log.Printf("[KafkaConnect] %s %s finished in %v, err=%v", method, u, time.Since(start), err)
	if err != nil {
		return fmt.Errorf("connect call: %w", err)
	}
	defer resp.Body.Close()

	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 400 {
		return fmt.Errorf("kafka connect HTTP %d: %s", resp.StatusCode, string(raw))
	}
	if target == nil || len(raw) == 0 {
		return nil
	}
	if err := json.Unmarshal(raw, target); err != nil {
		return fmt.Errorf("parse response: %w", err)
	}
	return nil
}

// FilterSafeConfig strips credentials / internal-only keys before
// returning config to the UI. Used by both List (per-connector
// config preview) and Create (fingerprint persistence).
func FilterSafeConfig(cfg map[string]string) map[string]string {
	if cfg == nil {
		return nil
	}
	out := make(map[string]string, len(cfg))
	for k, v := range cfg {
		lk := strings.ToLower(k)
		if strings.Contains(lk, "password") || strings.Contains(lk, "secret") ||
			strings.Contains(lk, "token") || strings.Contains(lk, "credentials") ||
			strings.Contains(lk, "ssl.key") {
			out[k] = "***"
			continue
		}
		out[k] = v
	}
	return out
}
