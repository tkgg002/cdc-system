package airbyte

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"go.uber.org/zap"
)

type Client struct {
	baseURL      string
	clientID     string
	clientSecret string
	httpClient   *http.Client
	logger       *zap.Logger
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

type SyncCatalog struct {
	Streams []StreamConfig `json:"streams"`
}

type Stream struct {
	Name                    string                 `json:"name"`
	Namespace               string                 `json:"namespace"`
	SyncMode                string                 `json:"syncMode"`
	SupportedSyncModes      []string               `json:"supportedSyncModes"`
	JSONSchema              map[string]interface{} `json:"jsonSchema"`
	// SourceDefinedPrimaryKey lifted from CMS Airbyte client so the Worker
	// can infer PK fields during stream import (TASK 9 HandleImportStreams
	// + TASK 10 HandleBulkSyncFromAirbyte).
	SourceDefinedPrimaryKey [][]string `json:"sourceDefinedPrimaryKey,omitempty"`
	DefaultCursorField      []string   `json:"defaultCursorField,omitempty"`
}

// JobStatus mirrors the Airbyte job list response (needed by Worker sync
// handlers reporting job_id back to the caller).
type JobStatus struct {
	JobID     int64     `json:"jobId"`
	Status    string    `json:"status"`
	CreatedAt time.Time `json:"createdAt"`
	UpdatedAt time.Time `json:"updatedAt"`
}

func NewClient(baseURL, clientID, clientSecret string, logger *zap.Logger) *Client {
	return &Client{
		baseURL:      baseURL,
		clientID:     clientID,
		clientSecret: clientSecret,
		httpClient:   &http.Client{Timeout: 30 * time.Second},
		logger:       logger,
	}
}

func (c *Client) DiscoverSourceSchema(ctx context.Context, sourceID string) (*SyncCatalog, error) {
	url := fmt.Sprintf("%s/api/v1/sources/%s/discover_schema", c.baseURL, sourceID)
	req, err := http.NewRequestWithContext(ctx, "POST", url, nil)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	c.setHeaders(ctx, req)

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
		return nil, fmt.Errorf("decode response: %w", err)
	}

	c.logger.Info("airbyte schema discovery completed", zap.String("source_id", sourceID))
	return &result.Catalog, nil
}

func (c *Client) ListSources(ctx context.Context) ([]Source, error) {
	// 1. Get Workspace ID first (Common for Airbyte OSS)
	workspaceID, err := c.getDefaultWorkspaceID(ctx)
	if err != nil {
		return nil, fmt.Errorf("get workspace: %w", err)
	}

	url := fmt.Sprintf("%s/api/v1/sources/list", c.baseURL)
	payload := map[string]string{"workspaceId": workspaceID}
	payloadBytes, _ := json.Marshal(payload)

	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(payloadBytes))
	if err != nil {
		return nil, err
	}
	c.setHeaders(ctx, req)

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
	url := fmt.Sprintf("%s/api/v1/workspaces/list", c.baseURL)
	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewBuffer([]byte("{\"includeDeleted\": false}")))
	if err != nil {
		return "", err
	}
	c.setHeaders(ctx, req)

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

	// Fallback for Airbyte OSS Default Workspace
	c.logger.Warn("Failed to list workspaces, using default fallback ID", zap.Int("status", resp.StatusCode))
	return "ece70fcd-015f-419a-883c-e411e9fbd439", nil
}

func (c *Client) TriggerSync(ctx context.Context, connectionID string) (string, error) {
	url := fmt.Sprintf("%s/api/v1/connections/%s/sync", c.baseURL, connectionID)
	req, err := http.NewRequestWithContext(ctx, "POST", url, nil)
	if err != nil {
		return "", fmt.Errorf("create request: %w", err)
	}
	c.setHeaders(ctx, req)

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
	Stream   Stream        `json:"stream"`
	Config   Config        `json:"config"`
}

type Config struct {
	SyncMode            string     `json:"syncMode,omitempty"`
	DestinationSyncMode string     `json:"destinationSyncMode,omitempty"`
	Selected            bool       `json:"selected"`
	PrimaryKey          [][]string `json:"primaryKey,omitempty"`
	CursorField         []string   `json:"cursorField,omitempty"`
	AliasName           string     `json:"aliasName,omitempty"`
}

type FieldConfig struct {
	FieldName string `json:"fieldName"`
	Selected  bool   `json:"selected"`
}

func (c *Client) GetConnection(ctx context.Context, connectionID string) (*Connection, error) {
	url := fmt.Sprintf("%s/api/v1/connections/get", c.baseURL)
	payload := map[string]string{"connectionId": connectionID}
	payloadBytes, _ := json.Marshal(payload)

	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(payloadBytes))
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	c.setHeaders(ctx, req)

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
	url := fmt.Sprintf("%s/api/v1/connections/list", c.baseURL)
	payload := map[string]string{"workspaceId": workspaceID}
	payloadBytes, _ := json.Marshal(payload)

	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(payloadBytes))
	if err != nil {
		return nil, err
	}
	c.setHeaders(ctx, req)

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

func (c *Client) UpdateConnection(ctx context.Context, connectionID string, streams []StreamConfig) error {
	url := fmt.Sprintf("%s/api/v1/connections/update", c.baseURL)
	payload := map[string]interface{}{
		"connectionId": connectionID,
		"syncCatalog":  map[string]interface{}{"streams": streams},
	}
	payloadBytes, _ := json.Marshal(payload)

	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(payloadBytes))
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	c.setHeaders(ctx, req)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("execute request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("airbyte API error %d: %s", resp.StatusCode, string(body) )
	}
	return nil
}

func (c *Client) setHeaders(ctx context.Context, req *http.Request) {
	if c.clientID != "" && c.clientSecret != "" {
		token := c.getAccessToken(ctx)
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
}

func (c *Client) getAccessToken(ctx context.Context) string {
	url := fmt.Sprintf("%s/api/v1/applications/token", c.baseURL) // public API oauth token
	// Airbyte OSS oauth tokens usually accept client_id and client_secret in json
	payload := map[string]string{
		"grant_type":    "client_credentials",
		"client_id":     c.clientID,
		"client_secret": c.clientSecret,
	}
	payloadBytes, _ := json.Marshal(payload)
	
	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(payloadBytes))
	if err != nil {
		c.logger.Error("Failed to create token request", zap.Error(err))
		return ""
	}
	req.Header.Set("Content-Type", "application/json")
	
	resp, err := c.httpClient.Do(req)
	if err != nil {
		c.logger.Error("Failed to fetch access token", zap.Error(err))
		return ""
	}
	defer resp.Body.Close()
	
	if resp.StatusCode == http.StatusOK {
		var result struct {
			AccessToken string `json:"access_token"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&result); err == nil {
			return result.AccessToken
		}
	} else {
	    // If token endpoint fails, we might just be using a plain token or Airbyte basic auth setup masquerading
	    c.logger.Warn("Failed to fetch oauth token, falling back to basic auth / api key if possible", zap.Int("status", resp.StatusCode))
	}
	
	// Fallback to basic auth with clientID/clientSecret
	return base64.StdEncoding.EncodeToString([]byte(c.clientID + ":" + c.clientSecret))
}

// DiscoverSchema is a Worker-side alias for DiscoverSourceSchema (keeps
// parity with the CMS Airbyte client naming used in the boundary refactor
// payload handlers).
func (c *Client) DiscoverSchema(ctx context.Context, sourceID string) (*SyncCatalog, error) {
	return c.DiscoverSourceSchema(ctx, sourceID)
}

// GetWorkspaceID returns a best-effort workspace id (OSS fallback baked
// into getDefaultWorkspaceID).
func (c *Client) GetWorkspaceID() string {
	id, err := c.getDefaultWorkspaceID(context.Background())
	if err != nil {
		return ""
	}
	return id
}

// ListJobs returns the 20 most recent sync jobs for a connection. Used by
// the Worker sync handlers to echo job_id back on NATS results.
func (c *Client) ListJobs(ctx context.Context, connectionID string) ([]JobStatus, error) {
	url := fmt.Sprintf("%s/api/v1/jobs/list", c.baseURL)
	payload := map[string]interface{}{
		"configType": "sync",
		"pagination": map[string]interface{}{"pageSize": 20},
	}
	if connectionID != "" {
		payload["configId"] = connectionID
	} else {
		wsID, _ := c.getDefaultWorkspaceID(ctx)
		payload["workspaceId"] = wsID
	}
	payloadBytes, _ := json.Marshal(payload)

	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(payloadBytes))
	if err != nil {
		return nil, err
	}
	c.setHeaders(ctx, req)

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
