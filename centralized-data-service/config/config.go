package config

import (
	"os"
	"strings"
	"time"

	"github.com/spf13/viper"
)

type AppConfig struct {
	Server  ServerConfig  `mapstructure:"server"`
	DB      DBConfig      `mapstructure:"db"`
	Nats    NatsConfig    `mapstructure:"nats"`
	Redis   RedisConfig   `mapstructure:"redis"`
	Worker  WorkerConfig  `mapstructure:"worker"`
	Airbyte AirbyteConfig `mapstructure:"airbyte"`
	JWT     JWTConfig     `mapstructure:"jwt"`
	Kafka   KafkaConfig   `mapstructure:"kafka"`
	Otel    OtelConfig    `mapstructure:"otel"`
	MongoDB MongoDBConfig `mapstructure:"mongodb"`
	Debezium DebeziumConfig `mapstructure:"debezium"`
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
	SignalCollection     string `mapstructure:"signalCollection"`
	ConnectorStatusURL   string `mapstructure:"connectorStatusUrl"`
	IncrementalChunkSize int    `mapstructure:"incrementalChunkSize"`
	KafkaConnectURL      string `mapstructure:"kafkaConnectUrl"`
	ConnectorName        string `mapstructure:"connectorName"`
}

type OtelConfig struct {
	Enabled     bool       `mapstructure:"enabled"`
	ServiceName string     `mapstructure:"serviceName"`
	Endpoint    string     `mapstructure:"endpoint"`
	SampleRatio float64    `mapstructure:"sampleRatio"`
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

type KafkaConfig struct {
	Brokers           []string `mapstructure:"brokers"`
	GroupID           string   `mapstructure:"groupId"`
	TopicPrefix       string   `mapstructure:"topicPrefix"`
	SchemaRegistryURL string   `mapstructure:"schemaRegistryUrl"`
	Enabled           bool     `mapstructure:"enabled"`
}

type ServerConfig struct {
	Name string `mapstructure:"name"`
	Port string `mapstructure:"port"`
	Mode string `mapstructure:"mode"` // "worker" or "cms"
}

type DBConfig struct {
	Host            string        `mapstructure:"host"`
	Port            int           `mapstructure:"port"`
	UserName        string        `mapstructure:"username"`
	Password        string        `mapstructure:"password"`
	Database        string        `mapstructure:"database"`
	SSLMode         string        `mapstructure:"sslMode"`
	MaxOpenConn     int           `mapstructure:"maxOpenConn"`
	MaxIdleConn     int           `mapstructure:"maxIdleConn"`
	ConnMaxLifetime time.Duration `mapstructure:"connMaxLifetime"`
	// ReadReplicaDSN — optional Postgres read-replica DSN used by
	// Recon agents and other pure-read paths. Leave empty to reuse
	// the primary connection with SET TRANSACTION READ ONLY guard.
	// Format: postgres://user:pass@host:port/db?sslmode=disable
	ReadReplicaDSN string `mapstructure:"readReplicaDsn"`
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

type AirbyteConfig struct {
	APIURL       string `mapstructure:"apiUrl"`
	ClientID     string `mapstructure:"clientId"`
	ClientSecret string `mapstructure:"clientSecret"`
}

type JWTConfig struct {
	Secret     string        `mapstructure:"secret"`
	Expiration time.Duration `mapstructure:"expiration"`
}

func NewConfig() (*AppConfig, error) {
	cfg := &AppConfig{}

	path := os.Getenv("CFG_PATH")
	if path == "" {
		path = "./config/config-local"
	}

	v := viper.New()
	v.SetConfigName(path)
	v.SetConfigType("yml")
	v.AddConfigPath(".")
	v.AutomaticEnv()
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))

	if err := v.ReadInConfig(); err != nil {
		return nil, err
	}

	if err := v.Unmarshal(cfg); err != nil {
		return nil, err
	}

	applyEnvOverrides(cfg)

	return cfg, nil
}

func applyEnvOverrides(cfg *AppConfig) {
	if v := os.Getenv("DB_SINK_URL"); v != "" {
		// Parse DSN format: postgres://user:pass@host:port/db?sslmode=disable
		// Keep config struct for GORM, but allow DSN env override
		cfg.DB.SSLMode = "disable"
	}
	if v := os.Getenv("DB_READ_REPLICA_DSN"); v != "" {
		cfg.DB.ReadReplicaDSN = v
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
	if v := os.Getenv("AIRBYTE_API_URL"); v != "" {
		cfg.Airbyte.APIURL = v
	}
	if v := os.Getenv("AIRBYTE_CLIENT_ID"); v != "" {
		cfg.Airbyte.ClientID = v
	}
	if v := os.Getenv("AIRBYTE_CLIENT_SECRET"); v != "" {
		cfg.Airbyte.ClientSecret = v
	}
	if v := os.Getenv("KAFKA_CONNECT_URL"); v != "" {
		cfg.Debezium.KafkaConnectURL = v
	}
	if v := os.Getenv("DEBEZIUM_CONNECTOR_NAME"); v != "" {
		cfg.Debezium.ConnectorName = v
	}
}
