package kafka

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"

	"github.com/linkedin/goavro/v2"
	"go.uber.org/zap"
)

// AvroDecoder handles Confluent Schema Registry Avro deserialization
type AvroDecoder struct {
	registryURL string
	cache       sync.Map // schema ID → *goavro.Codec
	logger      *zap.Logger
}

func NewAvroDecoder(registryURL string, logger *zap.Logger) *AvroDecoder {
	return &AvroDecoder{registryURL: registryURL, logger: logger}
}

// Decode deserializes Confluent Avro wire format (magic byte + 4-byte schema ID + Avro binary)
func (d *AvroDecoder) Decode(data []byte) (map[string]interface{}, error) {
	if len(data) < 6 || data[0] != 0 {
		return nil, fmt.Errorf("not confluent avro format (len=%d, magic=%d)", len(data), data[0])
	}

	schemaID := int32(binary.BigEndian.Uint32(data[1:5]))
	avroData := data[5:]

	codec, err := d.getCodec(schemaID)
	if err != nil {
		return nil, fmt.Errorf("schema %d: %w", schemaID, err)
	}

	native, _, err := codec.NativeFromBinary(avroData)
	if err != nil {
		return nil, fmt.Errorf("avro decode (schema %d): %w", schemaID, err)
	}

	result, ok := native.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("avro decoded to %T, expected map", native)
	}
	return result, nil
}

func (d *AvroDecoder) getCodec(schemaID int32) (*goavro.Codec, error) {
	if cached, ok := d.cache.Load(schemaID); ok {
		return cached.(*goavro.Codec), nil
	}

	url := fmt.Sprintf("%s/schemas/ids/%d", d.registryURL, schemaID)
	resp, err := http.Get(url)
	if err != nil {
		return nil, fmt.Errorf("fetch: %w", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("registry %d: %s", resp.StatusCode, string(body))
	}

	var result struct {
		Schema string `json:"schema"`
	}
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, fmt.Errorf("parse: %w", err)
	}

	sanitized := SanitizeAvroSchema(result.Schema)
	codec, err := goavro.NewCodec(sanitized)
	if err != nil {
		return nil, fmt.Errorf("codec: %w", err)
	}

	d.cache.Store(schemaID, codec)
	d.logger.Info("avro schema cached", zap.Int32("schema_id", schemaID))
	return codec, nil
}

// SanitizeAvroSchema fixes invalid Avro names (e.g., dashes in namespace)
func SanitizeAvroSchema(schema string) string {
	var parsed interface{}
	if err := json.Unmarshal([]byte(schema), &parsed); err != nil {
		return strings.ReplaceAll(schema, "-", "_")
	}
	fixAvroNames(parsed)
	fixed, _ := json.Marshal(parsed)
	return string(fixed)
}

func fixAvroNames(v interface{}) {
	switch val := v.(type) {
	case map[string]interface{}:
		for k, v2 := range val {
			if k == "name" || k == "namespace" {
				if s, ok := v2.(string); ok {
					val[k] = strings.ReplaceAll(s, "-", "_")
				}
			}
			fixAvroNames(v2)
		}
	case []interface{}:
		for _, item := range val {
			fixAvroNames(item)
		}
	}
}

// UnwrapAvroUnion extracts value from goavro union decode.
// goavro decodes union ["null","string"] as nil or map[string]interface{}{"string": "value"}
func UnwrapAvroUnion(v interface{}) interface{} {
	if v == nil {
		return nil
	}
	if m, ok := v.(map[string]interface{}); ok && len(m) == 1 {
		for _, val := range m {
			return val
		}
	}
	return v
}
