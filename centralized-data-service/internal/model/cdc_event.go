package model

// CDCEvent represents a CloudEvents CDC message from Debezium/Airbyte
type CDCEvent struct {
	SpecVersion     string       `json:"specversion"`
	ID              string       `json:"id"`
	Source          interface{}  `json:"source"` // string (custom) or object (Debezium Kafka)
	Type            string       `json:"type"`
	DataContentType string       `json:"datacontenttype"`
	Time            string       `json:"time"`
	Data            CDCEventData `json:"data"`
}

type CDCEventData struct {
	Op     string                 `json:"op"`     // "c" create, "u" update, "d" delete
	Before map[string]interface{} `json:"before"`
	After  map[string]interface{} `json:"after"`
	// SourceTsMs carries the Debezium `payload.source.ts_ms` value
	// (milliseconds since epoch at which the event occurred in the
	// source DB). Populated by the Kafka consumer. Zero = unknown.
	SourceTsMs int64 `json:"source_ts_ms,omitempty"`
}

// UpsertRecord is the internal representation for batch buffer
type UpsertRecord struct {
	TableName       string
	PrimaryKeyField string
	PrimaryKeyValue string
	MappedData      map[string]interface{}
	RawData         string
	Source          string
	Hash            string
	// SourceTsMs is the Debezium source ts_ms (milliseconds since epoch).
	// Zero means "unknown" — the UPSERT SQL MUST then skip the OCC
	// guard on _source_ts to keep legacy / bridge inserts working.
	SourceTsMs int64
}
