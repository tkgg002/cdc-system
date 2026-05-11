package service

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.uber.org/zap"
)

// DebeziumSignalConfig — see config.DebeziumConfig. Kept as a
// service-local struct so `internal/service` does not import the top
// level config package (circular dep from test harness).
type DebeziumSignalConfig struct {
	SignalCollection     string
	ConnectorStatusURL   string
	IncrementalChunkSize int
}

func (c *DebeziumSignalConfig) applyDefaults() {
	if c.SignalCollection == "" {
		c.SignalCollection = "debezium_signal"
	}
	if c.IncrementalChunkSize <= 0 {
		c.IncrementalChunkSize = 5000
	}
}

// DebeziumSignalClient inserts `execute-snapshot` commands into the
// Mongo signal collection (plan v3 §7). Debezium picks them up and
// emits incremental-snapshot chunks over Kafka like any other CDC
// event, so downstream ReconHeal does not need a custom fetch path
// on the happy path.
//
// Health check: the connector status endpoint (Kafka Connect REST)
// is probed on every call when configured. A connector in state
// RUNNING → Signal path is safe; anything else → caller must fall
// back to direct heal.
type DebeziumSignalClient struct {
	mongoClient *mongo.Client
	cfg         DebeziumSignalConfig
	httpClient  *http.Client
	logger      *zap.Logger
}

// NewDebeziumSignalClient constructs a Signal client. `mongoClient`
// must target the Mongo source (where Debezium's signal collection
// lives). `cfg.SignalCollection` defaults to `debezium_signal`.
func NewDebeziumSignalClient(mongoClient *mongo.Client, cfg DebeziumSignalConfig, logger *zap.Logger) *DebeziumSignalClient {
	cfg.applyDefaults()
	return &DebeziumSignalClient{
		mongoClient: mongoClient,
		cfg:         cfg,
		httpClient:  &http.Client{Timeout: 5 * time.Second},
		logger:      logger,
	}
}

// IsConfigured returns true when a mongo client is wired. Heal
// orchestration uses this to decide whether the Signal path is an
// option at all.
func (d *DebeziumSignalClient) IsConfigured() bool {
	return d != nil && d.mongoClient != nil
}

// TriggerIncrementalSnapshot inserts the execute-snapshot command
// into the configured signal collection on the source Mongo database.
//
// Args:
//   - database:   Mongo source database (maps to the Debezium source.database)
//   - collection: collection name (goopay.xxx — we build the qualified
//     identifier internally using the database prefix agreed with the
//     connector config: `<database>.<collection>`)
//   - filter:     Mongo filter expression applied by Debezium during the
//     incremental snapshot. Empty string → full collection snapshot.
//
// Returns the inserted signal ObjectID as string for audit linkage.
func (d *DebeziumSignalClient) TriggerIncrementalSnapshot(
	ctx context.Context,
	database, collection, filter string,
) (string, error) {
	if !d.IsConfigured() {
		return "", fmt.Errorf("debezium signal: mongo client not configured")
	}
	if database == "" || collection == "" {
		return "", fmt.Errorf("debezium signal: database and collection required")
	}

	qualified := database + "." + collection

	// Payload matches Debezium MongoDB incremental snapshot API.
	// See: https://debezium.io/documentation/reference/connectors/mongodb.html#mongodb-incremental-snapshots
	data := bson.M{
		"data-collections": []string{qualified},
		"type":             "incremental",
	}
	if strings.TrimSpace(filter) != "" {
		data["additional-conditions"] = []bson.M{
			{
				"data-collection": qualified,
				"filter":          filter,
			},
		}
	}

	doc := bson.M{
		"type": "execute-snapshot",
		"data": data,
	}

	coll := d.mongoClient.Database(database).Collection(d.cfg.SignalCollection)
	res, err := coll.InsertOne(ctx, doc)
	if err != nil {
		return "", fmt.Errorf("insert signal into %s.%s: %w", database, d.cfg.SignalCollection, err)
	}

	signalID := fmt.Sprintf("%v", res.InsertedID)
	d.logger.Info("debezium signal inserted",
		zap.String("database", database),
		zap.String("collection", collection),
		zap.String("filter", filter),
		zap.String("signal_id", signalID),
	)
	return signalID, nil
}

// BuildUpdatedAtRangeFilter produces the Mongo filter expression
// accepted by Debezium's `additional-conditions`. It targets the
// `updated_at` field (consistent with the rest of the v3 recon
// design) and produces a half-open interval [tLo, tHi).
//
// The generated string uses ISODate(...) literals because Debezium's
// expression evaluator parses Mongo shell syntax.
func BuildUpdatedAtRangeFilter(tLo, tHi time.Time) string {
	return fmt.Sprintf(
		"updated_at >= ISODate('%s') AND updated_at < ISODate('%s')",
		tLo.UTC().Format(time.RFC3339),
		tHi.UTC().Format(time.RFC3339),
	)
}

// IsConnectorHealthy probes the Kafka Connect REST status endpoint.
// Returns:
//   - healthy=true  when the connector status JSON reports "state":"RUNNING"
//     on both the connector and its primary task.
//   - healthy=false + nil err when the endpoint is reachable but the
//     connector is not running — heal orchestrator must fall back.
//   - healthy=false + err when the endpoint is unreachable. We treat
//     this as "assume unhealthy" at the caller side so the DIRECT heal
//     path still runs.
//
// When ConnectorStatusURL is empty we return (true, nil) — operators
// who have not wired the health URL get optimistic behavior; the
// InsertOne will fail loudly if the connector is actually dead.
func (d *DebeziumSignalClient) IsConnectorHealthy(ctx context.Context) (bool, error) {
	if d.cfg.ConnectorStatusURL == "" {
		return true, nil
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, d.cfg.ConnectorStatusURL, nil)
	if err != nil {
		return false, fmt.Errorf("build status request: %w", err)
	}

	resp, err := d.httpClient.Do(req)
	if err != nil {
		return false, fmt.Errorf("probe connector status: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return false, nil
	}

	// Schema:
	// {
	//   "name":"mongodb-connector",
	//   "connector":{"state":"RUNNING",...},
	//   "tasks":[{"state":"RUNNING",...}]
	// }
	var body struct {
		Connector struct {
			State string `json:"state"`
		} `json:"connector"`
		Tasks []struct {
			State string `json:"state"`
		} `json:"tasks"`
	}
	dec := json.NewDecoder(resp.Body)
	if err := dec.Decode(&body); err != nil {
		return false, fmt.Errorf("parse connector status: %w", err)
	}

	if !strings.EqualFold(body.Connector.State, "RUNNING") {
		return false, nil
	}
	for _, t := range body.Tasks {
		if !strings.EqualFold(t.State, "RUNNING") {
			return false, nil
		}
	}
	return true, nil
}
