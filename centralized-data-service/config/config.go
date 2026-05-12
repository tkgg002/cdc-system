package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"reflect"
	"strings"
	"time"

	"github.com/go-viper/mapstructure/v2"
	"github.com/spf13/viper"
)

const defaultJWTPlaceholder = "change-me-in-production"

type AppConfig struct {
	Server   ServerConfig   `mapstructure:"server"`
	DB       DBConfig       `mapstructure:"db"`
	SystemDB SingleDBTarget `mapstructure:"systemDb"`
	ShadowDB MultiDBTargets `mapstructure:"shadowDb"`
	MasterDB MultiDBTargets `mapstructure:"masterDb"`
	// Sources — connection_code → external source DSN. Used by Debezium
	// connector registration (Track D/E) and source_object_registry
	// resolution. Keys match `connection_registry.connection_code` in
	// cdc_system.
	Sources map[string]string `mapstructure:"sources"`
	// Phase 01 split E2E (T-C1) — control-plane physical DSN.
	//
	//   ControlPlane → gpay-postgres-cdc / cdc_dw
	//                  Worker reads cdc_system.mapping_rule_v2 +
	//                  writes shadow_<src>.* against this DSN.
	//
	// The destination DSN (gpay-postgres-dest / goopay_dest) is
	// derived from MasterDB.URLs[MasterDB.DefaultKey] — single source
	// of truth, no separate `destination:` block. See DestinationURL().
	ControlPlane SingleDBTarget `mapstructure:"controlPlane"`
	Nats         NatsConfig     `mapstructure:"nats"`
	Redis        RedisConfig    `mapstructure:"redis"`
	Worker       WorkerConfig   `mapstructure:"worker"`
	JWT          JWTConfig      `mapstructure:"jwt"`
	Kafka        KafkaConfig    `mapstructure:"kafka"`
	Otel         OtelConfig     `mapstructure:"otel"`
	MongoDB      MongoDBConfig  `mapstructure:"mongodb"`
	Debezium     DebeziumConfig `mapstructure:"debezium"`
}

type MongoDBConfig struct {
	URL string `mapstructure:"url"`
}

// DebeziumConfig — plan v3 §7 Heal via Signal.
// SignalCollection is the Mongo collection Debezium watches for
// incremental-snapshot commands. Defaults to `debezium_signal`.
// ConnectorStatusURL (optional) lets the heal orchestrator probe
// connector health before attempting the signal path.
// KafkaConnectURL — base URL for the Kafka Connect REST API. Used by
// the Worker boundary refactor handlers (HandleRestartDebezium,
// HandleSyncState) to proxy connector restart/pause/resume calls.
// ConnectorName defaults the handler to a known Debezium connector if the
// payload omits one.
type DebeziumConfig struct {
	SignalDatabase       string `mapstructure:"signalDatabase"`
	SignalCollection     string `mapstructure:"signalCollection"`
	ConnectorStatusURL   string `mapstructure:"connectorStatusUrl"`
	IncrementalChunkSize int    `mapstructure:"incrementalChunkSize"`
	KafkaConnectURL      string `mapstructure:"kafkaConnectUrl"`
	ConnectorName        string `mapstructure:"connectorName"`
}

type OtelConfig struct {
	Enabled     bool        `mapstructure:"enabled"`
	ServiceName string      `mapstructure:"serviceName"`
	Endpoint    string      `mapstructure:"endpoint"`
	SampleRatio float64     `mapstructure:"sampleRatio"`
	Logs        OtelLogsCfg `mapstructure:"logs"`
}

// OtelLogsCfg mirrors observability.LogsConfig but lives in the
// config layer so the worker binary does not import observability
// just to read yml.
type OtelLogsCfg struct {
	SampleBySeverity OtelLogSampleCfg   `mapstructure:"sampleBySeverity"`
	MemoryLimitMiB   int                `mapstructure:"memoryLimitMib"`
	Fallback         OtelLogFallbackCfg `mapstructure:"fallback"`
}

type OtelLogSampleCfg struct {
	Debug float64 `mapstructure:"debug"`
	Info  float64 `mapstructure:"info"`
	Warn  float64 `mapstructure:"warn"`
	Error float64 `mapstructure:"error"`
	Fatal float64 `mapstructure:"fatal"`
}

type OtelLogFallbackCfg struct {
	DegradedAfterErrors int           `mapstructure:"degradedAfterErrors"`
	RecoverAfter        time.Duration `mapstructure:"recoverAfter"`
}

// KafkaConfig — Phase multi_engine_unified.
// TopicPrefix accepts both YAML scalar (`topicPrefix: cdc.gpay`) for
// backward-compat AND list (`topicPrefix: [cdc.gpay, cdc.goopay]`).
// Alias `topicPrefixes` is also accepted (merged via Load()) so a
// fresh config can use the more idiomatic plural form without losing
// older `topicPrefix:` deployments.
type KafkaConfig struct {
	Brokers           []string `mapstructure:"brokers"`
	GroupID           string   `mapstructure:"groupId"`
	TopicPrefix       []string `mapstructure:"topicPrefix"`
	SchemaRegistryURL string   `mapstructure:"schemaRegistryUrl"`
	Enabled           bool     `mapstructure:"enabled"`
}

type ServerConfig struct {
	Name string `mapstructure:"name"`
	Port string `mapstructure:"port"`
	Mode string `mapstructure:"mode"` // "worker" or "cms"
}

type SingleDBTarget struct {
	URL string `mapstructure:"url"`
}

type MultiDBTargets struct {
	DefaultKey string            `mapstructure:"defaultKey"`
	URLs       map[string]string `mapstructure:"urls"`
}

type DBConfig struct {
	Host            string        `mapstructure:"host"`
	Port            int           `mapstructure:"port"`
	UserName        string        `mapstructure:"username"`
	Password        string        `mapstructure:"password"`
	Database        string        `mapstructure:"database"`
	SSLMode         string        `mapstructure:"sslMode"`
	URL             string        `mapstructure:"url"`
	MaxOpenConn     int           `mapstructure:"maxOpenConn"`
	MaxIdleConn     int           `mapstructure:"maxIdleConn"`
	ConnMaxLifetime time.Duration `mapstructure:"connMaxLifetime"`
	// ReadReplicaDSN — optional Postgres read-replica DSN used by
	// Recon agents and other pure-read paths. Leave empty to reuse
	// the primary connection with SET TRANSACTION READ ONLY guard.
	// Format: postgres://user:pass@host:port/db?sslmode=disable
	ReadReplicaDSN string `mapstructure:"readReplicaDsn"`
}

func (cfg DBConfig) DSN() string {
	if strings.TrimSpace(cfg.URL) != "" {
		return strings.TrimSpace(cfg.URL)
	}
	return fmt.Sprintf(
		"host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		cfg.Host, cfg.Port, cfg.UserName, cfg.Password, cfg.Database, cfg.SSLMode,
	)
}

func (cfg DBConfig) PgxDSN() string {
	if strings.TrimSpace(cfg.URL) != "" {
		return strings.TrimSpace(cfg.URL)
	}
	return fmt.Sprintf(
		"postgres://%s:%s@%s:%d/%s?sslmode=%s",
		cfg.UserName, cfg.Password, cfg.Host, cfg.Port, cfg.Database, cfg.SSLMode,
	)
}

type NatsConfig struct {
	URL           string        `mapstructure:"url"`
	Name          string        `mapstructure:"name"`
	User          string        `mapstructure:"user"`
	Pass          string        `mapstructure:"pass"`
	MaxReconnect  int           `mapstructure:"maxReconnect"`
	ReconnectWait time.Duration `mapstructure:"reconnectWait"`
}

type RedisConfig struct {
	URL      string `mapstructure:"url"`
	Password string `mapstructure:"password"`
	DB       int    `mapstructure:"db"`
}

type WorkerConfig struct {
	PoolSize          int           `mapstructure:"poolSize"`
	BatchSize         int           `mapstructure:"batchSize"`
	BatchTimeout      time.Duration `mapstructure:"batchTimeout"`
	FetchSize         int           `mapstructure:"fetchSize"`
	TransformInterval time.Duration `mapstructure:"transformInterval"`
	ScanInterval      time.Duration `mapstructure:"scanInterval"`
}

type JWTConfig struct {
	Secret     string        `mapstructure:"secret"`
	Expiration time.Duration `mapstructure:"expiration"`
}

func NewConfig() (*AppConfig, error) {
	cfg := &AppConfig{}

	path := os.Getenv("cfgPath")
	if path == "" {
		path = os.Getenv("CFG_PATH")
	}
	if path == "" {
		path = "./config/config-local.yml"
	}
	log.Printf("config path: %s", path)

	v := viper.New()
	if strings.Contains(path, "/") || strings.HasSuffix(path, ".yml") || strings.HasSuffix(path, ".yaml") || strings.HasSuffix(path, ".json") {
		v.SetConfigFile(path)
	} else {
		v.SetConfigName(path)
		v.SetConfigType("yml")
		v.AddConfigPath("./config")
		v.AddConfigPath("config")
		v.AddConfigPath(".")
	}
	v.AutomaticEnv()
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))

	if err := v.ReadInConfig(); err != nil {
		return nil, fmt.Errorf("read config %s: %w", path, err)
	}

	decodeHook := viper.DecodeHook(mapstructure.ComposeDecodeHookFunc(
		mapstructure.StringToTimeDurationHookFunc(),
		mapstructure.StringToSliceHookFunc(","),
		stringToStringSliceHookFunc(),
	))
	if err := v.Unmarshal(cfg, decodeHook); err != nil {
		return nil, fmt.Errorf("unmarshal config: %w", err)
	}

	mergeTopicPrefixAlias(v, cfg)
	applyEnvOverrides(cfg)

	// Validate user inputs BEFORE fallbacks fill in derivable fields,
	// so cfg.DB.PgxDSN() literal-non-empty garbage doesn't slip past.
	if err := validateConfig(cfg); err != nil {
		return nil, fmt.Errorf("validate config: %w", err)
	}

	applyDBFallbacks(cfg)

	return cfg, nil
}

// stringToStringSliceHookFunc lets mapstructure accept a YAML scalar
// where the target field is []string. Without this hook,
// `topicPrefix: cdc.gpay` (scalar) errors when the struct field is
// `[]string`. With it, the scalar is wrapped as a singleton.
func stringToStringSliceHookFunc() mapstructure.DecodeHookFunc {
	return func(f reflect.Type, t reflect.Type, data any) (any, error) {
		if f.Kind() != reflect.String {
			return data, nil
		}
		if t.Kind() != reflect.Slice || t.Elem().Kind() != reflect.String {
			return data, nil
		}
		s, _ := data.(string)
		s = strings.TrimSpace(s)
		if s == "" {
			return []string{}, nil
		}
		return []string{s}, nil
	}
}

// mergeTopicPrefixAlias unions kafka.topicPrefixes (alias) into
// kafka.topicPrefix so both YAML keys are accepted. Order preserved,
// duplicates dropped, blanks skipped.
func mergeTopicPrefixAlias(v *viper.Viper, cfg *AppConfig) {
	extras := v.GetStringSlice("kafka.topicPrefixes")
	if len(extras) == 0 && len(cfg.Kafka.TopicPrefix) == 0 {
		return
	}
	seen := make(map[string]struct{}, len(extras)+len(cfg.Kafka.TopicPrefix))
	merged := make([]string, 0, len(extras)+len(cfg.Kafka.TopicPrefix))
	for _, p := range append(append([]string{}, cfg.Kafka.TopicPrefix...), extras...) {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		if _, dup := seen[p]; dup {
			continue
		}
		seen[p] = struct{}{}
		merged = append(merged, p)
	}
	cfg.Kafka.TopicPrefix = merged
}

func applyEnvOverrides(cfg *AppConfig) {
	if v := os.Getenv("DB_SINK_URL"); v != "" {
		cfg.DB.URL = v
		cfg.DB.SSLMode = "disable"
	}
	if v := os.Getenv("DB_READ_REPLICA_DSN"); v != "" {
		cfg.DB.ReadReplicaDSN = v
	}
	if v := os.Getenv("CDC_SYSTEM_DB_URL"); v != "" {
		cfg.SystemDB.URL = strings.TrimSpace(v)
	}
	if v := os.Getenv("CDC_CONTROL_PLANE_URL"); v != "" {
		cfg.ControlPlane.URL = strings.TrimSpace(v)
	}
	if v := os.Getenv("CDC_DESTINATION_URL"); v != "" {
		// Consolidated: destination is masterDb.default. The legacy env
		// var keeps working but writes to MasterDB instead of a separate
		// Destination block.
		if cfg.MasterDB.URLs == nil {
			cfg.MasterDB.URLs = make(map[string]string)
		}
		cfg.MasterDB.URLs["default"] = strings.TrimSpace(v)
		if strings.TrimSpace(cfg.MasterDB.DefaultKey) == "" {
			cfg.MasterDB.DefaultKey = "default"
		}
	}
	if v := os.Getenv("CDC_SHADOW_DB_URL"); v != "" {
		if cfg.ShadowDB.URLs == nil {
			cfg.ShadowDB.URLs = make(map[string]string)
		}
		cfg.ShadowDB.URLs["default"] = strings.TrimSpace(v)
		if strings.TrimSpace(cfg.ShadowDB.DefaultKey) == "" {
			cfg.ShadowDB.DefaultKey = "default"
		}
	}
	if v := os.Getenv("CDC_MASTER_DB_URL"); v != "" {
		if cfg.MasterDB.URLs == nil {
			cfg.MasterDB.URLs = make(map[string]string)
		}
		cfg.MasterDB.URLs["default"] = strings.TrimSpace(v)
		if strings.TrimSpace(cfg.MasterDB.DefaultKey) == "" {
			cfg.MasterDB.DefaultKey = "default"
		}
	}
	if v := os.Getenv("CDC_SHADOW_DB_URLS"); v != "" {
		cfg.ShadowDB.URLs = parseNamedURLs(v)
		if strings.TrimSpace(cfg.ShadowDB.DefaultKey) == "" {
			cfg.ShadowDB.DefaultKey = pickDefaultKey(cfg.ShadowDB.URLs)
		}
	}
	if v := os.Getenv("CDC_MASTER_DB_URLS"); v != "" {
		cfg.MasterDB.URLs = parseNamedURLs(v)
		if strings.TrimSpace(cfg.MasterDB.DefaultKey) == "" {
			cfg.MasterDB.DefaultKey = pickDefaultKey(cfg.MasterDB.URLs)
		}
	}
	if v := os.Getenv("CDC_SHADOW_DB_DEFAULT_KEY"); v != "" {
		cfg.ShadowDB.DefaultKey = strings.TrimSpace(v)
	}
	if v := os.Getenv("CDC_MASTER_DB_DEFAULT_KEY"); v != "" {
		cfg.MasterDB.DefaultKey = strings.TrimSpace(v)
	}
	if v := os.Getenv("NATS_URL"); v != "" {
		cfg.Nats.URL = v
	}
	if v := os.Getenv("REDIS_URL"); v != "" {
		cfg.Redis.URL = v
	}
	if v := os.Getenv("JWT_SECRET"); v != "" {
		cfg.JWT.Secret = v
	}
	// OTEL_ENDPOINT lets host-running processes (e.g. /tmp/cdc-admin-api-f3v2)
	// override the docker-DNS endpoint (`http://otel-collector:4318`) hard-coded
	// in YAML. Host process must use the host-mapped port (`localhost:14318`).
	if v := os.Getenv("OTEL_ENDPOINT"); v != "" {
		cfg.Otel.Endpoint = v
	}
	if v := os.Getenv("KAFKA_CONNECT_URL"); v != "" {
		cfg.Debezium.KafkaConnectURL = v
	}
	if v := os.Getenv("DEBEZIUM_CONNECTOR_NAME"); v != "" {
		cfg.Debezium.ConnectorName = v
	}
	if v := os.Getenv("KAFKA_BROKERS"); v != "" {
		cfg.Kafka.Brokers = strings.Split(v, ",")
	}
	if v := os.Getenv("KAFKA_SCHEMA_REGISTRY_URL"); v != "" {
		cfg.Kafka.SchemaRegistryURL = v
	}
	// G4 — Docker env override for MongoDB URL. Must run BEFORE applyDBFallbacks
	// so that the fallback bridge (sources → cfg.MongoDB.URL) sees the correct value.
	if v := os.Getenv("MONGODB_URL"); v != "" {
		cfg.MongoDB.URL = strings.TrimSpace(v)
		if cfg.Sources == nil {
			cfg.Sources = make(map[string]string)
		}
		cfg.Sources["mongodb_primary"] = strings.TrimSpace(v)
	}
	// Phase B5 (2026-05-05) — explicit SOURCE_DSN_<KEY> env override for
	// each connection_code in cfg.Sources. Convention: lowercased key in
	// YAML map ↔ uppercased key in env (e.g. `postgres_primary` ↔
	// `SOURCE_DSN_POSTGRES_PRIMARY`). Empty env = keep YAML.
	if v := os.Getenv("SOURCE_DSN_POSTGRES_PRIMARY"); v != "" {
		if cfg.Sources == nil {
			cfg.Sources = make(map[string]string)
		}
		cfg.Sources["postgres_primary"] = strings.TrimSpace(v)
	}
	if v := os.Getenv("SOURCE_DSN_MONGODB_PRIMARY"); v != "" {
		if cfg.Sources == nil {
			cfg.Sources = make(map[string]string)
		}
		cfg.Sources["mongodb_primary"] = strings.TrimSpace(v)
		// Also hydrate legacy alias to keep applyDBFallbacks bridge intact.
		if strings.TrimSpace(cfg.MongoDB.URL) == "" {
			cfg.MongoDB.URL = strings.TrimSpace(v)
		}
	}
	// applyDBFallbacks is called by NewConfig AFTER validateConfig so the
	// validator sees pre-fallback user intent. Do not call it here.
}

// validateConfig refuses configurations that would boot but fail at runtime
// (or — worse — silently connect to garbage). Runs AFTER applyEnvOverrides
// but BEFORE applyDBFallbacks so the validator sees user-provided fields,
// not derived ones.
func validateConfig(cfg *AppConfig) error {
	if strings.TrimSpace(cfg.Server.Port) == "" {
		return errors.New("server.port required (set in YAML or via env)")
	}
	// DB primary: at least ONE source must be set.
	hasLegacy := strings.TrimSpace(cfg.DB.URL) != "" ||
		(strings.TrimSpace(cfg.DB.Host) != "" && strings.TrimSpace(cfg.DB.Database) != "")
	hasSplit := strings.TrimSpace(cfg.SystemDB.URL) != ""
	if !hasLegacy && !hasSplit {
		return errors.New("DB connection required (set db.host+db.database, db.url, systemDb.url, or env CDC_SYSTEM_DB_URL/DB_SINK_URL)")
	}
	// MasterDB target — single source of truth for destination DW per Phase 01 split E2E.
	masterKey := strings.TrimSpace(cfg.MasterDB.DefaultKey)
	if masterKey == "" {
		masterKey = "default"
	}
	masterURL := ""
	if cfg.MasterDB.URLs != nil {
		masterURL = strings.TrimSpace(cfg.MasterDB.URLs[masterKey])
	}
	if masterURL == "" {
		return fmt.Errorf("masterDB.urls[%s] required (set in YAML or env CDC_MASTER_DB_URL/CDC_DESTINATION_URL)", masterKey)
	}
	if strings.TrimSpace(cfg.JWT.Secret) == "" {
		return errors.New("jwt.secret required (set in YAML or env JWT_SECRET)")
	}
	if strings.EqualFold(cfg.Server.Mode, "production") && cfg.JWT.Secret == defaultJWTPlaceholder {
		return errors.New("jwt.secret must not use default placeholder in production mode")
	}
	return nil
}

func applyDBFallbacks(cfg *AppConfig) {
	legacy := cfg.DB.PgxDSN()

	if strings.TrimSpace(cfg.SystemDB.URL) == "" {
		cfg.SystemDB.URL = legacy
	}
	if cfg.ShadowDB.URLs == nil || len(cfg.ShadowDB.URLs) == 0 {
		cfg.ShadowDB.URLs = map[string]string{"default": legacy}
	}
	if strings.TrimSpace(cfg.ShadowDB.DefaultKey) == "" {
		cfg.ShadowDB.DefaultKey = pickDefaultKey(cfg.ShadowDB.URLs)
	}
	if cfg.MasterDB.URLs == nil || len(cfg.MasterDB.URLs) == 0 {
		cfg.MasterDB.URLs = map[string]string{"default": legacy}
	}
	if strings.TrimSpace(cfg.MasterDB.DefaultKey) == "" {
		cfg.MasterDB.DefaultKey = pickDefaultKey(cfg.MasterDB.URLs)
	}

	// Phase 01 split E2E (T-C1) — control-plane fallback only.
	// Destination is no longer a standalone field; DestinationURL()
	// derives from MasterDB.URLs[MasterDB.DefaultKey] at read time.
	if strings.TrimSpace(cfg.ControlPlane.URL) == "" {
		cfg.ControlPlane.URL = strings.TrimSpace(cfg.SystemDB.URL)
	}

	// Sources consolidation — bridge legacy `mongodb.url` to the new
	// `sources:` block. If yaml ships only `sources.mongodb_primary`,
	// hydrate cfg.MongoDB.URL so existing callers (worker_server) keep
	// working without an explicit migration.
	if strings.TrimSpace(cfg.MongoDB.URL) == "" && len(cfg.Sources) > 0 {
		if v, ok := cfg.Sources["mongodb_primary"]; ok {
			cfg.MongoDB.URL = strings.TrimSpace(v)
		}
	}
	// Inverse bridge: if legacy `mongodb.url` was set but `sources` is
	// empty, expose it under the canonical key so connection_registry
	// lookups still resolve.
	if cfg.Sources == nil {
		cfg.Sources = make(map[string]string)
	}
	if _, ok := cfg.Sources["mongodb_primary"]; !ok && strings.TrimSpace(cfg.MongoDB.URL) != "" {
		cfg.Sources["mongodb_primary"] = strings.TrimSpace(cfg.MongoDB.URL)
	}
}

func parseNamedURLs(raw string) map[string]string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return map[string]string{}
	}

	out := make(map[string]string)

	// Preferred format: JSON object {"default":"postgres://...","finance":"postgres://..."}
	if strings.HasPrefix(raw, "{") {
		if err := json.Unmarshal([]byte(raw), &out); err == nil {
			return cleanURLMap(out)
		}
	}

	// Fallback format: default=postgres://...;finance=postgres://...
	for _, pair := range strings.Split(raw, ";") {
		pair = strings.TrimSpace(pair)
		if pair == "" {
			continue
		}
		parts := strings.SplitN(pair, "=", 2)
		if len(parts) != 2 {
			continue
		}
		key := strings.TrimSpace(parts[0])
		val := strings.TrimSpace(parts[1])
		if key == "" || val == "" {
			continue
		}
		out[key] = val
	}

	return cleanURLMap(out)
}

func cleanURLMap(in map[string]string) map[string]string {
	out := make(map[string]string, len(in))
	for k, v := range in {
		key := strings.TrimSpace(k)
		val := strings.TrimSpace(v)
		if key == "" || val == "" {
			continue
		}
		out[key] = val
	}
	return out
}

func pickDefaultKey(items map[string]string) string {
	if len(items) == 0 {
		return "default"
	}
	if _, ok := items["default"]; ok {
		return "default"
	}
	for k := range items {
		return k
	}
	return "default"
}

func (cfg *AppConfig) SystemDBURL() string {
	return strings.TrimSpace(cfg.SystemDB.URL)
}

// ControlPlaneURL returns the DSN for the cdc_dw instance (control
// plane registry + shadow_<src>). Phase 01 split E2E.
func (cfg *AppConfig) ControlPlaneURL() string {
	return strings.TrimSpace(cfg.ControlPlane.URL)
}

// DestinationURL returns the DSN for the goopay_dest instance
// (master + dw_<binding>). Phase 01 split E2E. Single source of
// truth: derives from MasterDB.URLs[MasterDB.DefaultKey].
func (cfg *AppConfig) DestinationURL() string {
	key := strings.TrimSpace(cfg.MasterDB.DefaultKey)
	if key == "" {
		key = "default"
	}
	if cfg.MasterDB.URLs == nil {
		return ""
	}
	return strings.TrimSpace(cfg.MasterDB.URLs[key])
}

// SourceURL returns the DSN registered under `sources.<name>` in
// config-local.yml. Returns empty string if the name is unknown.
// Used by Debezium connector registration and source resolution.
func (cfg *AppConfig) SourceURL(name string) string {
	name = strings.TrimSpace(name)
	if name == "" || cfg.Sources == nil {
		return ""
	}
	return strings.TrimSpace(cfg.Sources[name])
}

func (cfg *AppConfig) ShadowDBURLs() map[string]string {
	return cloneURLMap(cfg.ShadowDB.URLs)
}

func (cfg *AppConfig) MasterDBURLs() map[string]string {
	return cloneURLMap(cfg.MasterDB.URLs)
}

func (cfg *AppConfig) ShadowDBDefaultKey() string {
	return strings.TrimSpace(cfg.ShadowDB.DefaultKey)
}

func (cfg *AppConfig) MasterDBDefaultKey() string {
	return strings.TrimSpace(cfg.MasterDB.DefaultKey)
}

func cloneURLMap(in map[string]string) map[string]string {
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}
