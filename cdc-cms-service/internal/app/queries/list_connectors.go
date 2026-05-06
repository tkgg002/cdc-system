// Package queries — read-side use cases (CQRS Q-side).
//
// Connector reads proxy Kafka Connect REST. The HTTP client lives in
// `internal/infra/http/kafka_connect.go`; this file only orchestrates
// (loops over names, fans out status+config calls, projects to the
// wire shape).
package queries

import (
	"context"

	infrahttp "cdc-cms-service/internal/infra/http"
)

// ConnectorReader is the read-side port for the Kafka Connect proxy.
// Single caller (the CMS connectors handler), so it lives next to the
// query handlers rather than in `ports/`.
type ConnectorReader interface {
	ListNames(ctx context.Context) ([]string, error)
	GetStatus(ctx context.Context, name string) (infrahttp.ConnectorStatusResp, error)
	GetConfig(ctx context.Context, name string) (map[string]string, error)
	ListPlugins(ctx context.Context) ([]map[string]any, error)
}

// ----- ListConnectors ----------------------------------------------

// ListConnectorsQuery is GET /api/v1/system/connectors. Unfiltered.
type ListConnectorsQuery struct{}

func (q ListConnectorsQuery) Type() string { return "connectors.list" }

// ListConnectorsResult contains the per-connector projected views.
type ListConnectorsResult struct {
	Data  []infrahttp.ConnectorView
	Count int
}

// ListConnectorsHandler orchestrates the N+1 fan-out: for each name
// it pulls /status + /config from the upstream. Per-connector
// failures are tolerated (legacy behaviour: empty fields, but the
// connector stays in the response).
type ListConnectorsHandler struct {
	reader ConnectorReader
}

func NewListConnectorsHandler(r ConnectorReader) *ListConnectorsHandler {
	return &ListConnectorsHandler{reader: r}
}

func (h *ListConnectorsHandler) Handle(ctx context.Context, _ ListConnectorsQuery) (ListConnectorsResult, error) {
	names, err := h.reader.ListNames(ctx)
	if err != nil {
		return ListConnectorsResult{}, err
	}
	out := make([]infrahttp.ConnectorView, 0, len(names))
	for _, name := range names {
		v := infrahttp.ConnectorView{Name: name}
		if status, err := h.reader.GetStatus(ctx, name); err == nil {
			v.State = status.Connector.State
			v.Type = status.Type
			v.Tasks = status.Tasks
		}
		if cfg, err := h.reader.GetConfig(ctx, name); err == nil {
			v.Connector = cfg["connector.class"]
			v.Config = infrahttp.FilterSafeConfig(cfg)
		}
		out = append(out, v)
	}
	return ListConnectorsResult{Data: out, Count: len(out)}, nil
}

// ----- GetConnector ------------------------------------------------

// GetConnectorQuery is GET /api/v1/system/connectors/:name.
type GetConnectorQuery struct {
	Name string
}

func (q GetConnectorQuery) Type() string { return "connectors.get" }

// GetConnectorResult is the projected single-connector view. The
// status block is mandatory; config errors are swallowed (legacy
// behaviour — `_ = ...` in the original handler) so the response
// can still surface the status payload.
type GetConnectorResult struct {
	Name   string
	Status infrahttp.ConnectorStatusResp
	Config map[string]string
}

// GetConnectorHandler resolves the query against the reader.
type GetConnectorHandler struct {
	reader ConnectorReader
}

func NewGetConnectorHandler(r ConnectorReader) *GetConnectorHandler {
	return &GetConnectorHandler{reader: r}
}

func (h *GetConnectorHandler) Handle(ctx context.Context, q GetConnectorQuery) (GetConnectorResult, error) {
	status, err := h.reader.GetStatus(ctx, q.Name)
	if err != nil {
		return GetConnectorResult{}, err
	}
	cfg, _ := h.reader.GetConfig(ctx, q.Name)
	return GetConnectorResult{
		Name:   q.Name,
		Status: status,
		Config: infrahttp.FilterSafeConfig(cfg),
	}, nil
}

// ----- ListConnectorPlugins ----------------------------------------

// ListConnectorPluginsQuery is GET /api/v1/system/connector-plugins.
type ListConnectorPluginsQuery struct{}

func (q ListConnectorPluginsQuery) Type() string { return "connectors.plugins" }

// ListConnectorPluginsResult mirrors the upstream payload exactly.
type ListConnectorPluginsResult struct {
	Data  []map[string]any
	Count int
}

// ListConnectorPluginsHandler resolves the query.
type ListConnectorPluginsHandler struct {
	reader ConnectorReader
}

func NewListConnectorPluginsHandler(r ConnectorReader) *ListConnectorPluginsHandler {
	return &ListConnectorPluginsHandler{reader: r}
}

func (h *ListConnectorPluginsHandler) Handle(ctx context.Context, _ ListConnectorPluginsQuery) (ListConnectorPluginsResult, error) {
	plugins, err := h.reader.ListPlugins(ctx)
	if err != nil {
		return ListConnectorPluginsResult{}, err
	}
	return ListConnectorPluginsResult{Data: plugins, Count: len(plugins)}, nil
}
