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

type AppConfig struct {
	Server ServerConfig `mapstructure:"server"`
	DB     DBConfig     `mapstructure:"db"`
	JWT    JWTConfig    `mapstructure:"jwt"`
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

type JWTConfig struct {
	Secret            string        `mapstructure:"secret"`
	AccessExpiration  time.Duration `mapstructure:"accessExpiration"`
	RefreshExpiration time.Duration `mapstructure:"refreshExpiration"`
}

const defaultJWTPlaceholder = "change-me-in-production"

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
		v.AddConfigPath("./config")
		v.AddConfigPath("config")
		v.AddConfigPath(".")
	}

	v.SetEnvPrefix("AUTH")
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()

	envBinds := map[string][]string{
		"server.name":           {"AUTH_SERVER_NAME"},
		"server.port":           {"AUTH_SERVER_PORT"},
		"server.mode":           {"AUTH_SERVER_MODE"},
		"db.host":               {"AUTH_DB_HOST"},
		"db.port":               {"AUTH_DB_PORT"},
		"db.username":           {"AUTH_DB_USERNAME"},
		"db.password":           {"AUTH_DB_PASSWORD"},
		"db.database":           {"AUTH_DB_DATABASE"},
		"db.sslMode":            {"AUTH_DB_SSL_MODE"},
		"db.maxOpenConn":        {"AUTH_DB_MAX_OPEN_CONN"},
		"db.maxIdleConn":        {"AUTH_DB_MAX_IDLE_CONN"},
		"db.connMaxLifetime":    {"AUTH_DB_CONN_MAX_LIFETIME"},
		"jwt.secret":            {"AUTH_JWT_SECRET", "JWT_SECRET"},
		"jwt.accessExpiration":  {"AUTH_JWT_ACCESS_EXPIRATION"},
		"jwt.refreshExpiration": {"AUTH_JWT_REFRESH_EXPIRATION"},
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
		return errors.New("server.port required (set in YAML or AUTH_SERVER_PORT)")
	}
	if cfg.DB.Host == "" {
		return errors.New("db.host required (set in YAML or AUTH_DB_HOST)")
	}
	if cfg.DB.Database == "" {
		return errors.New("db.database required (set in YAML or AUTH_DB_DATABASE)")
	}
	if cfg.DB.UserName == "" {
		return errors.New("db.username required (set in YAML or AUTH_DB_USERNAME)")
	}
	if cfg.JWT.Secret == "" {
		return errors.New("jwt.secret required (set in YAML or AUTH_JWT_SECRET)")
	}
	if strings.EqualFold(cfg.Server.Mode, "production") && cfg.JWT.Secret == defaultJWTPlaceholder {
		return errors.New("jwt.secret must not use default placeholder in production mode")
	}
	return nil
}
