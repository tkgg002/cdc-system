package config

import (
	"os"
	"strings"
	"time"

	"github.com/spf13/viper"
)

type AppConfig struct {
	Server ServerConfig `mapstructure:"server"`
	DB     DBConfig     `mapstructure:"db"`
	Nats   NatsConfig   `mapstructure:"nats"`
	Redis  RedisConfig  `mapstructure:"redis"`
	JWT    JWTConfig    `mapstructure:"jwt"`
	System SystemConfig `mapstructure:"system"`
	Otel   OtelConfig   `mapstructure:"otel"`
}

type OtelConfig struct {
	Enabled     bool    `mapstructure:"enabled"`
	ServiceName string  `mapstructure:"serviceName"`
	Endpoint    string  `mapstructure:"endpoint"`
	SampleRatio float64 `mapstructure:"sampleRatio"`
}

type SystemConfig struct {
	WorkerURL       string `mapstructure:"workerUrl"`
	KafkaConnectURL string `mapstructure:"kafkaConnectUrl"`
	NatsMonitorURL  string `mapstructure:"natsMonitorUrl"`
	// Prometheus HTTP API endpoint for histogram_quantile queries.
	// Empty => the collector falls back to scraping WorkerURL/metrics.
	PrometheusURL string `mapstructure:"prometheusUrl"`
	// kafka-exporter Prometheus text-format endpoint. Used to populate
	// `cdc_pipeline.consumer_lag.total_lag` in the system health snapshot,
	// which in turn drives the HighConsumerLag alert rule.
	// Empty => consumer_lag section is reported as "unknown".
	KafkaExporterURL string `mapstructure:"kafkaExporterUrl"`
	// Debezium MongoDB connector name (registered with Kafka Connect).
	DebeziumConnector string `mapstructure:"debeziumConnector"`
	// Optional override for the Redis cache key used by system health.
	HealthCacheKey string `mapstructure:"healthCacheKey"`
}

type ServerConfig struct {
	Name string `mapstructure:"name"`
	Port string `mapstructure:"port"`
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

	// Env overrides
	if v := os.Getenv("NATS_URL"); v != "" {
		cfg.Nats.URL = v
	}
	if v := os.Getenv("REDIS_URL"); v != "" {
		cfg.Redis.URL = v
	}
	if v := os.Getenv("JWT_SECRET"); v != "" {
		cfg.JWT.Secret = v
	}
	return cfg, nil
}
