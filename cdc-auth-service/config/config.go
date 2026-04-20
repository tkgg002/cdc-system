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
	JWT    JWTConfig    `mapstructure:"jwt"`
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

type JWTConfig struct {
	Secret           string        `mapstructure:"secret"`
	AccessExpiration  time.Duration `mapstructure:"accessExpiration"`
	RefreshExpiration time.Duration `mapstructure:"refreshExpiration"`
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

	if v := os.Getenv("JWT_SECRET"); v != "" {
		cfg.JWT.Secret = v
	}

	return cfg, nil
}
