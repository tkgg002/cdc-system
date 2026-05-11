package config

import (
	"errors"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/spf13/viper"
)

const defaultJWTPlaceholder = "change-me-in-production"

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
	WorkerURL         string `mapstructure:"workerUrl"`
	KafkaConnectURL   string `mapstructure:"kafkaConnectUrl"`
	NatsMonitorURL    string `mapstructure:"natsMonitorUrl"`
	PrometheusURL     string `mapstructure:"prometheusUrl"`
	KafkaExporterURL  string `mapstructure:"kafkaExporterUrl"`
	DebeziumConnector string `mapstructure:"debeziumConnector"`
	HealthCacheKey    string `mapstructure:"healthCacheKey"`
}

type ServerConfig struct {
	Name string `mapstructure:"name"`
	Port string `mapstructure:"port"`
	Mode string `mapstructure:"mode"`
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

	v.SetEnvPrefix("CMS")
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()

	envBinds := map[string][]string{
		"server.name":              {"CMS_SERVER_NAME"},
		"server.port":              {"CMS_SERVER_PORT"},
		"server.mode":              {"CMS_SERVER_MODE"},
		"db.host":                  {"CMS_DB_HOST"},
		"db.port":                  {"CMS_DB_PORT"},
		"db.username":              {"CMS_DB_USERNAME"},
		"db.password":              {"CMS_DB_PASSWORD"},
		"db.database":              {"CMS_DB_DATABASE"},
		"db.sslMode":               {"CMS_DB_SSL_MODE"},
		"db.maxOpenConn":           {"CMS_DB_MAX_OPEN_CONN"},
		"db.maxIdleConn":           {"CMS_DB_MAX_IDLE_CONN"},
		"db.connMaxLifetime":       {"CMS_DB_CONN_MAX_LIFETIME"},
		"nats.url":                 {"CMS_NATS_URL", "NATS_URL"},
		"nats.user":                {"CMS_NATS_USER"},
		"nats.pass":                {"CMS_NATS_PASS"},
		"redis.url":                {"CMS_REDIS_URL", "REDIS_URL"},
		"redis.password":           {"CMS_REDIS_PASSWORD"},
		"jwt.secret":               {"CMS_JWT_SECRET", "JWT_SECRET"},
		"jwt.expiration":           {"CMS_JWT_EXPIRATION"},
		"otel.enabled":             {"CMS_OTEL_ENABLED"},
		"otel.serviceName":         {"CMS_OTEL_SERVICE_NAME"},
		"otel.endpoint":            {"CMS_OTEL_ENDPOINT", "OTEL_EXPORTER_OTLP_ENDPOINT"},
		"otel.sampleRatio":         {"CMS_OTEL_SAMPLE_RATIO"},
		"system.workerUrl":         {"CMS_SYSTEM_WORKER_URL"},
		"system.kafkaConnectUrl":   {"CMS_SYSTEM_KAFKA_CONNECT_URL"},
		"system.natsMonitorUrl":    {"CMS_SYSTEM_NATS_MONITOR_URL"},
		"system.prometheusUrl":     {"CMS_SYSTEM_PROMETHEUS_URL"},
		"system.kafkaExporterUrl":  {"CMS_SYSTEM_KAFKA_EXPORTER_URL"},
		"system.debeziumConnector": {"CMS_SYSTEM_DEBEZIUM_CONNECTOR"},
		"system.healthCacheKey":    {"CMS_SYSTEM_HEALTH_CACHE_KEY"},
	}
	for key, envs := range envBinds {
		args := append([]string{key}, envs...)
		_ = v.BindEnv(args...)
	}

	if err := v.ReadInConfig(); err != nil {
		var notFound viper.ConfigFileNotFoundError
		if errors.As(err, &notFound) {
			log.Printf("config file %s not found, relying on env-only", path)
		} else {
			return nil, fmt.Errorf("read config %s: %w", path, err)
		}
	}

	if err := v.Unmarshal(cfg); err != nil {
		return nil, fmt.Errorf("unmarshal config: %w", err)
	}

	if err := validateConfig(cfg); err != nil {
		return nil, fmt.Errorf("validate config: %w", err)
	}

	return cfg, nil
}

func validateConfig(cfg *AppConfig) error {
	if cfg.Server.Port == "" {
		return errors.New("server.port required (set in YAML or CMS_SERVER_PORT)")
	}
	if cfg.DB.Host == "" {
		return errors.New("db.host required (set in YAML or CMS_DB_HOST)")
	}
	if cfg.DB.Database == "" {
		return errors.New("db.database required (set in YAML or CMS_DB_DATABASE)")
	}
	if cfg.DB.UserName == "" {
		return errors.New("db.username required (set in YAML or CMS_DB_USERNAME)")
	}
	if cfg.JWT.Secret == "" {
		return errors.New("jwt.secret required (set in YAML or CMS_JWT_SECRET)")
	}
	if strings.EqualFold(cfg.Server.Mode, "production") && cfg.JWT.Secret == defaultJWTPlaceholder {
		return errors.New("jwt.secret must not use default placeholder in production mode")
	}
	return nil
}
