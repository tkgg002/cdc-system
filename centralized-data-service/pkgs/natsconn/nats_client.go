package natsconn

import (
	"fmt"

	"centralized-data-service/config"

	"github.com/nats-io/nats.go"
	"go.uber.org/zap"
)

type NatsClient struct {
	Conn *nats.Conn
	JS   nats.JetStreamContext
}

func NewNatsClient(cfg *config.AppConfig, logger *zap.Logger) (*NatsClient, error) {
	opts := []nats.Option{
		nats.Name(cfg.Nats.Name),
		nats.MaxReconnects(cfg.Nats.MaxReconnect),
		nats.ReconnectWait(cfg.Nats.ReconnectWait),
		nats.DisconnectErrHandler(func(_ *nats.Conn, err error) {
			logger.Warn("NATS disconnected", zap.Error(err))
		}),
		nats.ReconnectHandler(func(nc *nats.Conn) {
			logger.Info("NATS reconnected", zap.String("url", nc.ConnectedUrl()))
		}),
	}

	// Add auth credentials if configured
	if cfg.Nats.User != "" && cfg.Nats.Pass != "" {
		opts = append(opts, nats.UserInfo(cfg.Nats.User, cfg.Nats.Pass))
	}

	nc, err := nats.Connect(cfg.Nats.URL, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to connect NATS: %w", err)
	}

	js, err := nc.JetStream(nats.PublishAsyncMaxPending(256))
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("failed to create JetStream context: %w", err)
	}

	logger.Info("NATS JetStream connected", zap.String("url", cfg.Nats.URL))

	return &NatsClient{Conn: nc, JS: js}, nil
}

func (c *NatsClient) Close() {
	if c.Conn != nil {
		c.Conn.Close()
	}
}

// EnsureStreams creates NATS streams if they don't exist
func (c *NatsClient) EnsureStreams() error {
	streams := []struct {
		name     string
		subjects []string
	}{
		{"CDC_EVENTS", []string{"cdc.goopay.>"}},
		{"SCHEMA_DRIFT", []string{"schema.drift.detected"}},
		{"SCHEMA_CONFIG", []string{"schema.config.reload"}},
	}

	for _, s := range streams {
		_, err := c.JS.StreamInfo(s.name)
		if err == nats.ErrStreamNotFound {
			_, err = c.JS.AddStream(&nats.StreamConfig{
				Name:      s.name,
				Subjects:  s.subjects,
				Retention: nats.LimitsPolicy,
				MaxAge:    7 * 24 * 60 * 60 * 1e9, // 7 days in nanoseconds
				Storage:   nats.FileStorage,
				Replicas:  1,
			})
			if err != nil {
				return fmt.Errorf("failed to create stream %s: %w", s.name, err)
			}
		} else if err != nil {
			return fmt.Errorf("failed to get stream info %s: %w", s.name, err)
		}
	}

	return nil
}
