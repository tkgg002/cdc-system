// Package sinkworker implements the v1.25 Debezium sink pipeline:
//
//	MongoDB -> Debezium -> Kafka -> SinkWorker -> cdc_internal.<table>
//
// (Phase 1 parallel independence contract — see plan v7.2 §10). It does
// NOT share code with the legacy handler to avoid accidental coupling.
//
// This file owns the envelope parsing primitives: Avro/Schema-Registry
// decoding, Debezium union unwrapping, canonicalisation of the envelope
// back to deterministic JSON for _raw_data storage and hashing.
package sinkworker

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/linkedin/goavro/v2"
)

// avroDecoder decodes Confluent-framed Avro messages by resolving schema IDs
// against the Schema Registry. A lightweight cache avoids hitting the
// registry once per message. Safe for concurrent use.
type avroDecoder struct {
	schemaRegistryURL string
	httpClient        *http.Client
	mu                sync.RWMutex
	cache             map[int32]*goavro.Codec
}

func newAvroDecoder(schemaRegistryURL string) *avroDecoder {
	return &avroDecoder{
		schemaRegistryURL: schemaRegistryURL,
		httpClient:        &http.Client{Timeout: 10 * time.Second},
		cache:             make(map[int32]*goavro.Codec),
	}
}

// decodeMessage takes a Kafka value bytes slice and returns a Go-native map.
// If the value is Confluent-framed Avro (magic byte 0x00 + 4-byte schema id)
// it decodes via Schema Registry. Otherwise falls back to plain JSON.
// Empty slice => nil (caller treats as tombstone).
func (d *avroDecoder) decodeMessage(value []byte) (map[string]any, error) {
	if len(value) == 0 {
		return nil, nil
	}
	if len(value) > 5 && value[0] == 0 {
		schemaID := int32(binary.BigEndian.Uint32(value[1:5]))
		codec, err := d.getCodec(schemaID)
		if err != nil {
			return nil, fmt.Errorf("avro codec for schema %d: %w", schemaID, err)
		}
		native, _, err := codec.NativeFromBinary(value[5:])
		if err != nil {
			return nil, fmt.Errorf("avro decode (schema %d): %w", schemaID, err)
		}
		m, ok := native.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("avro root expected map, got %T", native)
		}
		return m, nil
	}
	// Plain JSON envelope
	var m map[string]any
	if err := json.Unmarshal(value, &m); err != nil {
		return nil, fmt.Errorf("envelope JSON parse: %w", err)
	}
	return m, nil
}

func (d *avroDecoder) getCodec(schemaID int32) (*goavro.Codec, error) {
	d.mu.RLock()
	c, ok := d.cache[schemaID]
	d.mu.RUnlock()
	if ok {
		return c, nil
	}

	url := fmt.Sprintf("%s/schemas/ids/%d", d.schemaRegistryURL, schemaID)
	resp, err := d.httpClient.Get(url)
	if err != nil {
		return nil, fmt.Errorf("fetch schema %d: %w", schemaID, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("schema registry returned %d for schema %d", resp.StatusCode, schemaID)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read schema %d: %w", schemaID, err)
	}
	var env struct {
		Schema string `json:"schema"`
	}
	if err := json.Unmarshal(body, &env); err != nil {
		return nil, fmt.Errorf("parse registry response schema %d: %w", schemaID, err)
	}
	codec, err := goavro.NewCodec(env.Schema)
	if err != nil {
		return nil, fmt.Errorf("goavro codec schema %d: %w", schemaID, err)
	}

	d.mu.Lock()
	d.cache[schemaID] = codec
	d.mu.Unlock()
	return codec, nil
}

// unwrapAvroUnion collapses goavro's `map[type]value` representation of a
// union into the underlying value. Non-union values pass through unchanged.
// This mirrors the legacy handler's helper (keep local so we never
func unwrapAvroUnion(v any) any {
	if v == nil {
		return nil
	}
	m, ok := v.(map[string]any)
	if !ok || len(m) != 1 {
		return v
	}
	for _, val := range m {
		return val
	}
	return v
}

// Debezium MongoDB connector sends `after` / `before` as a JSON string
// wrapped in a nullable Avro union. This function returns the parsed
// document map, or nil+nil when the field is absent / null.
func decodeAfter(raw any) (map[string]any, error) {
	raw = unwrapAvroUnion(raw)
	if raw == nil {
		return nil, nil
	}
	switch v := raw.(type) {
	case map[string]any:
		return v, nil
	case string:
		if v == "" {
			return nil, nil
		}
		var m map[string]any
		if err := json.Unmarshal([]byte(v), &m); err != nil {
			return nil, fmt.Errorf("after JSON parse: %w", err)
		}
		return m, nil
	case []byte:
		if len(v) == 0 {
			return nil, nil
		}
		var m map[string]any
		if err := json.Unmarshal(v, &m); err != nil {
			return nil, fmt.Errorf("after bytes parse: %w", err)
		}
		return m, nil
	}
	return nil, fmt.Errorf("after field unexpected type %T", raw)
}

// extractSourceTsMs picks up source.ts_ms (Debezium's authoritative
// event-time clock in milliseconds) and falls back to top-level ts_ms.
// Returns 0 if neither is present — the OCC guard will then allow any
// incoming update (safe because the compare is `NULL OR EXCLUDED > target`).
func extractSourceTsMs(envelope map[string]any) int64 {
	src, _ := envelope["source"].(map[string]any)
	if src != nil {
		if ts := numericMs(src["ts_ms"]); ts > 0 {
			return ts
		}
	}
	return numericMs(envelope["ts_ms"])
}

// isSnapshotEvent returns true when the Debezium envelope carries the
// `source.snapshot` marker set to "true", "last", or "incremental".
// Streaming (oplog) events either omit the field or set it to "false".
//
// Used to route snapshot-originated rows through a DO NOTHING upsert
// path so a re-snapshot can never overwrite a newer streaming update
// of the same document — plan Phase 2 §2 Option B.
func isSnapshotEvent(envelope map[string]any) bool {
	src, _ := envelope["source"].(map[string]any)
	if src == nil {
		return false
	}
	raw := unwrapAvroUnion(src["snapshot"])
	switch v := raw.(type) {
	case string:
		switch v {
		case "true", "last", "incremental":
			return true
		}
	case bool:
		return v
	}
	return false
}

func numericMs(v any) int64 {
	switch x := v.(type) {
	case int64:
		return x
	case int:
		return int64(x)
	case int32:
		return int64(x)
	case float64:
		return int64(x)
	case string:
		n, _ := strconv.ParseInt(x, 10, 64)
		return n
	}
	return 0
}

// extractSourceID derives _gpay_source_id from the Mongo document.
// Order of preference:
//  1. after._id.$oid  (Mongo Extended JSON)
//  2. after._id string / number scalar
//  3. Kafka message.Key (Debezium sends the PK here as a second source of truth)
//
// Returns "" if none found.
func extractSourceID(after map[string]any, msgKey []byte) string {
	if after != nil {
		if raw, ok := after["_id"]; ok {
			if s := coerceID(raw); s != "" {
				return s
			}
		}
	}
	if len(msgKey) == 0 {
		return ""
	}
	// Debezium serialises the key as a JSON object like {"id": "<oid>"} or
	// {"id": "{\"$oid\": \"...\"}"}. Try both.
	var keyObj map[string]any
	if err := json.Unmarshal(msgKey, &keyObj); err == nil {
		for _, v := range keyObj {
			if s := coerceID(v); s != "" {
				return s
			}
		}
	}
	return string(msgKey)
}

func coerceID(v any) string {
	switch x := v.(type) {
	case string:
		// May still be `{"$oid":"..."}` as a JSON string
		var wrap map[string]any
		if err := json.Unmarshal([]byte(x), &wrap); err == nil {
			if oid, _ := wrap["$oid"].(string); oid != "" {
				return oid
			}
		}
		return x
	case map[string]any:
		if oid, _ := x["$oid"].(string); oid != "" {
			return oid
		}
		// fallback: stringify first scalar field
		for _, val := range x {
			if s, ok := val.(string); ok && s != "" {
				return s
			}
		}
	case float64:
		return strconv.FormatInt(int64(x), 10)
	case int64:
		return strconv.FormatInt(x, 10)
	}
	return ""
}

// canonicalJSON produces a deterministic JSON byte slice for the envelope.
// Keys are sorted at every object level so the same logical payload always
// hashes to the same _hash — critical for Policy 2 (Payload Integrity).
func canonicalJSON(v any) ([]byte, error) {
	var buf bytes.Buffer
	if err := writeCanonical(&buf, v); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func writeCanonical(buf *bytes.Buffer, v any) error {
	switch x := v.(type) {
	case nil:
		buf.WriteString("null")
	case map[string]any:
		keys := make([]string, 0, len(x))
		for k := range x {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		buf.WriteByte('{')
		for i, k := range keys {
			if i > 0 {
				buf.WriteByte(',')
			}
			kb, _ := json.Marshal(k)
			buf.Write(kb)
			buf.WriteByte(':')
			if err := writeCanonical(buf, x[k]); err != nil {
				return err
			}
		}
		buf.WriteByte('}')
	case []any:
		buf.WriteByte('[')
		for i, elem := range x {
			if i > 0 {
				buf.WriteByte(',')
			}
			if err := writeCanonical(buf, elem); err != nil {
				return err
			}
		}
		buf.WriteByte(']')
	default:
		// Scalars: int, float, string, bool — rely on encoding/json.
		b, err := json.Marshal(x)
		if err != nil {
			return err
		}
		buf.Write(b)
	}
	return nil
}
