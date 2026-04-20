package airbyte

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"go.uber.org/zap"
)

type Client struct {
	baseURL     string
	apiKey      string
	workspaceID string
	httpClient  *http.Client
	logger      *zap.Logger
}

type Source struct {
	SourceID     string `json:"sourceId"`
	Name         string `json:"name"`
	SourceName   string `json:"sourceName"`
	WorkspaceID  string `json:"workspaceId"`
	Database     string `json:"database"`
	ConnectionID string `json:"connectionId,omitempty"`
}

type Connection struct {
	ConnectionID string      `json:"connectionId"`
	Name         string      `json:"name"`
	SourceID     string      `json:"sourceId"`
	DestID       string      `json:"destinationId"`
	SyncCatalog  SyncCatalog `json:"syncCatalog"`
	Status       string      `json:"status"`
}

type JobStatus struct {
	JobID     int64     `json:"jobId"`
	Status    string    `json:"status"` // E.g., "running", "succeeded", "failed"
	CreatedAt time.Time `json:"createdAt"`
	UpdatedAt time.Time `json:"updatedAt"`
}

type SyncCatalog struct {
	Streams []StreamConfig `json:"streams"`
}

type Stream struct {
	Name                    string      `json:"name"`
	Namespace               string      `json:"namespace,omitempty"`
	SyncMode                string      `json:"syncMode,omitempty"`
	SupportedSyncModes      []string    `json:"supportedSyncModes,omitempty"`
	JsonSchema              interface{} `json:"jsonSchema"`
	DefaultCursorField      []string    `json:"defaultCursorField,omitempty"`
	SourceDefinedPrimaryKey [][]string  `json:"sourceDefinedPrimaryKey,omitempty"`
}

func NewClient(baseURL, apiKey, workspaceID string, logger *zap.Logger) *Client {
	return &Client{
		baseURL:     baseURL,
		apiKey:      apiKey,
		workspaceID: workspaceID,
		httpClient:  &http.Client{Timeout: 30 * time.Second},
		logger:      logger,
	}
}

func (c *Client) DiscoverSchema(ctx context.Context, sourceID string) (*SyncCatalog, error) {
	url := fmt.Sprintf("%s/v1/sources/discover_schema", c.baseURL)
	payload := map[string]interface{}{
		"sourceId":      sourceID,
		"disable_cache": true,
	}
	payloadBytes, _ := json.Marshal(payload)

	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(payloadBytes))
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	c.setHeaders(req)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("execute request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("airbyte API error %d: %s", resp.StatusCode, string(body))
	}

	var result struct {
		Catalog SyncCatalog `json:"catalog"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("decode discover schema: %w", err)
	}

	c.logger.Info("airbyte schema discovery completed successfully", zap.String("source_id", sourceID), zap.Int("streams_count", len(result.Catalog.Streams)))
	return &result.Catalog, nil
}

func (c *Client) ListSources(ctx context.Context) ([]Source, error) {
	// 1. Get Workspace ID first (Common for Airbyte OSS)
	workspaceID, err := c.getDefaultWorkspaceID(ctx)
	if err != nil {
		return nil, fmt.Errorf("get workspace: %w", err)
	}

	url := fmt.Sprintf("%s/v1/sources/list", c.baseURL)
	payload := map[string]string{"workspaceId": workspaceID}
	payloadBytes, _ := json.Marshal(payload)

	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(payloadBytes))
	if err != nil {
		return nil, err
	}
	c.setHeaders(req)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("airbyte error %d: %s", resp.StatusCode, string(body))
	}

	var result struct {
		Sources []struct {
			SourceID                string `json:"sourceId"`
			Name                    string `json:"name"`
			SourceName              string `json:"sourceName"`
			WorkspaceID             string `json:"workspaceId"`
			ConnectionConfiguration struct {
				Database string `json:"database"`
			} `json:"connectionConfiguration"`
		} `json:"sources"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}

	sources := make([]Source, 0, len(result.Sources))
	for _, s := range result.Sources {
		sources = append(sources, Source{
			SourceID:    s.SourceID,
			Name:        s.Name,
			SourceName:  s.SourceName,
			WorkspaceID: s.WorkspaceID,
			Database:    s.ConnectionConfiguration.Database,
		})
	}

	return sources, nil
}

func (c *Client) getDefaultWorkspaceID(ctx context.Context) (string, error) {
	if c.workspaceID != "" {
		return c.workspaceID, nil
	}

	url := fmt.Sprintf("%s/v1/workspaces/list", c.baseURL)
	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewBuffer([]byte("{\"includeDeleted\": false}")))
	if err != nil {
		return "", err
	}
	c.setHeaders(req)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusOK {
		var result struct {
			Workspaces []struct {
				WorkspaceID string `json:"workspaceId"`
			} `json:"workspaces"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&result); err == nil && len(result.Workspaces) > 0 {
			return result.Workspaces[0].WorkspaceID, nil
		}
	}
	// Fallback logic
	if resp.StatusCode == http.StatusForbidden || resp.StatusCode == http.StatusUnauthorized {
		c.logger.Warn("Airbyte API access denied for workspace list", zap.Int("status", resp.StatusCode))
		return "", nil
	}

	c.logger.Warn("Failed to list workspaces, using default fallback ID", zap.Int("status", resp.StatusCode))
	return "", nil
}

func (c *Client) TriggerSync(ctx context.Context, connectionID string) (string, error) {
	url := fmt.Sprintf("%s/v1/connections/%s/sync", c.baseURL, connectionID)
	req, err := http.NewRequestWithContext(ctx, "POST", url, nil)
	if err != nil {
		return "", fmt.Errorf("create request: %w", err)
	}
	c.setHeaders(req)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("execute request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("airbyte API error %d: %s", resp.StatusCode, string(body))
	}

	var result struct {
		JobID  string `json:"jobId"`
		Status string `json:"status"`
	}
	json.NewDecoder(resp.Body).Decode(&result)
	return result.JobID, nil
}

type StreamConfig struct {
	Stream Stream `json:"stream"`
	Config Config `json:"config"`
}

type Config struct {
	SyncMode            string     `json:"syncMode,omitempty"`
	DestinationSyncMode string     `json:"destinationSyncMode,omitempty"`
	Selected            bool       `json:"selected"`
	PrimaryKey          [][]string `json:"primaryKey,omitempty"`
	CursorField         []string   `json:"cursorField,omitempty"`
	ReplicationMethod   string     `json:"replicationMethod,omitempty"`
	AliasName           string     `json:"aliasName,omitempty"`
}

type FieldConfig struct {
	FieldName string `json:"fieldName"`
	Selected  bool   `json:"selected"`
}

func (c *Client) GetConnection(ctx context.Context, connectionID string) (*Connection, error) {
	url := fmt.Sprintf("%s/v1/connections/get", c.baseURL)
	payload := map[string]string{"connectionId": connectionID}
	payloadBytes, _ := json.Marshal(payload)

	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(payloadBytes))
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	c.setHeaders(req)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("execute request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("airbyte API error %d: %s", resp.StatusCode, string(body))
	}

	var conn Connection
	if err := json.NewDecoder(resp.Body).Decode(&conn); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}
	return &conn, nil
}

func (c *Client) ListConnections(ctx context.Context, workspaceID string) ([]Connection, error) {
	url := fmt.Sprintf("%s/v1/connections/list", c.baseURL)
	payload := map[string]string{"workspaceId": workspaceID}
	payloadBytes, _ := json.Marshal(payload)

	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(payloadBytes))
	if err != nil {
		return nil, err
	}
	c.setHeaders(req)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("airbyte API error %d: %s", resp.StatusCode, string(body))
	}

	var result struct {
		Connections []Connection `json:"connections"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}
	return result.Connections, nil
}

func (c *Client) UpdateConnection(ctx context.Context, connectionID string, status string, streams []StreamConfig) error {
	url := fmt.Sprintf("%s/v1/connections/update", c.baseURL)
	payload := map[string]interface{}{
		"connectionId": connectionID,
		"status":       status,
		"syncCatalog":  map[string]interface{}{"streams": streams},
	}
	payloadBytes, _ := json.Marshal(payload)

	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(payloadBytes))
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	c.setHeaders(req)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("execute request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("airbyte API error %d: %s", resp.StatusCode, string(body))
	}
	return nil
}

func (c *Client) ListJobs(ctx context.Context, connectionID string) ([]JobStatus, error) {
	url := fmt.Sprintf("%s/v1/jobs/list", c.baseURL)
	payload := map[string]interface{}{
		"configType": "sync",
		"pagination": map[string]interface{}{"pageSize": 20},
	}
	if connectionID != "" {
		payload["configId"] = connectionID
	} else {
		// If global list, we need workspaceId usually
		wsID, _ := c.getDefaultWorkspaceID(ctx)
		payload["workspaceId"] = wsID
	}
	payloadBytes, _ := json.Marshal(payload)

	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(payloadBytes))
	if err != nil {
		return nil, err
	}
	c.setHeaders(req)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("airbyte API error %d: %s", resp.StatusCode, string(body))
	}

	var result struct {
		Jobs []struct {
			Job struct {
				ID        int64  `json:"id"`
				CreatedAt int64  `json:"createdAt"`
				UpdatedAt int64  `json:"updatedAt"`
				Status    string `json:"status"`
			} `json:"job"`
		} `json:"jobs"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}

	jobs := make([]JobStatus, 0, len(result.Jobs))
	for _, j := range result.Jobs {
		jobs = append(jobs, JobStatus{
			JobID:     j.Job.ID,
			Status:    j.Job.Status,
			CreatedAt: time.Unix(j.Job.CreatedAt, 0),
			UpdatedAt: time.Unix(j.Job.UpdatedAt, 0),
		})
	}
	return jobs, nil
}

// GetWorkspaceID returns the configured workspace ID
func (c *Client) GetWorkspaceID() string {
	return c.workspaceID
}

// GetBaseURL returns the Airbyte API base URL
func (c *Client) GetBaseURL() string {
	return c.baseURL
}

// Destination represents an Airbyte destination connector
type Destination struct {
	DestinationID   string `json:"destinationId"`
	Name            string `json:"name"`
	DestinationName string `json:"destinationName"`
	WorkspaceID     string `json:"workspaceId"`
}

// ListDestinations returns all configured Airbyte destinations
func (c *Client) ListDestinations(ctx context.Context) ([]Destination, error) {
	url := fmt.Sprintf("%s/v1/destinations/list", c.baseURL)
	payload, _ := json.Marshal(map[string]string{"workspaceId": c.workspaceID})

	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(payload))
	if err != nil {
		return nil, err
	}
	c.setHeaders(req)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("list destinations: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("airbyte error %d: %s", resp.StatusCode, string(body))
	}

	var result struct {
		Destinations []Destination `json:"destinations"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}
	return result.Destinations, nil
}

// ConnectionDetail is an enriched connection view
type ConnectionDetail struct {
	ConnectionID   string `json:"connectionId"`
	Name           string `json:"name"`
	SourceID       string `json:"sourceId"`
	DestinationID  string `json:"destinationId"`
	Status         string `json:"status"`
	StreamCount    int    `json:"streamCount"`
	EnabledStreams int    `json:"enabledStreams"`
}

// ListConnectionDetails returns connections with stream count summary
func (c *Client) ListConnectionDetails(ctx context.Context) ([]ConnectionDetail, error) {
	conns, err := c.ListConnections(ctx, c.workspaceID)
	if err != nil {
		return nil, err
	}

	details := make([]ConnectionDetail, 0, len(conns))
	for _, conn := range conns {
		enabled := 0
		for _, s := range conn.SyncCatalog.Streams {
			if s.Config.Selected {
				enabled++
			}
		}
		details = append(details, ConnectionDetail{
			ConnectionID:  conn.ConnectionID,
			Name:          conn.Name,
			SourceID:      conn.SourceID,
			DestinationID: conn.DestID,
			Status:        conn.Status,
			StreamCount:   len(conn.SyncCatalog.Streams),
			EnabledStreams: enabled,
		})
	}
	return details, nil
}

// StreamDetail is a stream with its config + registry comparison
type StreamDetail struct {
	Name         string   `json:"name"`
	Namespace    string   `json:"namespace"`
	SyncMode     string   `json:"syncMode"`
	Selected     bool     `json:"selected"`
	CursorField  []string `json:"cursorField,omitempty"`
	PrimaryKey   [][]string `json:"primaryKey,omitempty"`
	IsRegistered bool     `json:"isRegistered"`
}

// GetConnectionStreams returns streams of a connection with their config
func (c *Client) GetConnectionStreams(ctx context.Context, connectionID string) ([]StreamDetail, error) {
	conn, err := c.GetConnection(ctx, connectionID)
	if err != nil {
		return nil, err
	}

	streams := make([]StreamDetail, 0, len(conn.SyncCatalog.Streams))
	for _, sc := range conn.SyncCatalog.Streams {
		streams = append(streams, StreamDetail{
			Name:        sc.Stream.Name,
			Namespace:   sc.Stream.Namespace,
			SyncMode:    sc.Config.SyncMode,
			Selected:    sc.Config.Selected,
			CursorField: sc.Config.CursorField,
			PrimaryKey:  sc.Config.PrimaryKey,
		})
	}
	return streams, nil
}

func (c *Client) setHeaders(req *http.Request) {
	if c.apiKey != "" {
		if strings.Contains(c.apiKey, ":") {
			// Basic Auth: username:password
			req.Header.Set("Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte(c.apiKey)))
		} else {
			// Bearer Token
			req.Header.Set("Authorization", "Bearer "+c.apiKey)
		}
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
}
