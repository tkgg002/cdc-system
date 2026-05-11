package rediscache

import (
	"context"
	"fmt"
	"time"

	"centralized-data-service/config"

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

type RedisCache struct {
	client *redis.Client
}

func NewRedisCache(cfg *config.AppConfig, logger *zap.Logger) (*RedisCache, error) {
	opts, err := redis.ParseURL(cfg.Redis.URL)
	if err != nil {
		// Fallback: build options manually
		opts = &redis.Options{
			Addr:     cfg.Redis.URL,
			Password: cfg.Redis.Password,
			DB:       cfg.Redis.DB,
		}
	}

	client := redis.NewClient(opts)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("failed to connect Redis: %w", err)
	}

	logger.Info("Redis connected", zap.String("addr", opts.Addr))

	return &RedisCache{client: client}, nil
}

func (c *RedisCache) Get(ctx context.Context, key string) (string, error) {
	return c.client.Get(ctx, key).Result()
}

func (c *RedisCache) Set(ctx context.Context, key, value string, ttl time.Duration) error {
	return c.client.Set(ctx, key, value, ttl).Err()
}

func (c *RedisCache) Delete(ctx context.Context, key string) error {
	return c.client.Del(ctx, key).Err()
}

func (c *RedisCache) DeletePattern(ctx context.Context, pattern string) error {
	iter := c.client.Scan(ctx, 0, pattern, 100).Iterator()
	for iter.Next(ctx) {
		c.client.Del(ctx, iter.Val())
	}
	return iter.Err()
}

func (c *RedisCache) Close() error {
	return c.client.Close()
}

// RawClient returns the underlying redis.Client. Intended for
// advanced primitives (SetNX/Eval/Script) used by recon leader
// election — avoids exposing the field while still allowing
// power users to call go-redis APIs directly.
func (c *RedisCache) RawClient() *redis.Client {
	return c.client
}
