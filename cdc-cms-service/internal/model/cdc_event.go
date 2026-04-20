package model

// CDCEvent represents a CloudEvents CDC message from Debezium/Airbyte
type CDCEvent struct {
	SpecVersion     string       `json:"specversion"`
	ID              string       `json:"id"`
	Source          string       `json:"source"`
	Type            string       `json:"type"`
	DataContentType string       `json:"datacontenttype"`
	Time            string       `json:"time"`
	Data            CDCEventData `json:"data"`
}

type CDCEventData struct {
	Op     string                 `json:"op"`     // "c" create, "u" update, "d" delete
	Before map[string]interface{} `json:"before"`
	After  map[string]interface{} `json:"after"`
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
}
