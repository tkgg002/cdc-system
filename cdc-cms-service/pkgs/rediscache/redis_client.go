package rediscache

import (
	"context"
	"fmt"
	"time"

	"cdc-cms-service/config"

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

func (c *RedisCache) Ping(ctx context.Context) error {
	return c.client.Ping(ctx).Err()
}

// Client exposes the underlying go-redis client for middlewares that
// need primitives not wrapped by RedisCache (e.g. SetNX for idempotency
// locks, Incr+Expire for rate limiting). Keep this boundary narrow:
// prefer wrapper methods when possible.
func (c *RedisCache) Client() *redis.Client {
	return c.client
}

// SetNX wraps go-redis SetNX — convenience for idempotency/rate limiter.
func (c *RedisCache) SetNX(ctx context.Context, key, value string, ttl time.Duration) (bool, error) {
	return c.client.SetNX(ctx, key, value, ttl).Result()
}

// Incr wraps go-redis Incr — returns the post-increment counter.
func (c *RedisCache) Incr(ctx context.Context, key string) (int64, error) {
	return c.client.Incr(ctx, key).Result()
}

// Expire sets a TTL on an existing key (used by rate limiter after the
// first Incr to bound the counter window).
func (c *RedisCache) Expire(ctx context.Context, key string, ttl time.Duration) error {
	return c.client.Expire(ctx, key, ttl).Err()
}

// TTL returns the remaining TTL for a key (−2 if missing, −1 if no TTL).
func (c *RedisCache) TTL(ctx context.Context, key string) (time.Duration, error) {
	return c.client.TTL(ctx, key).Result()
}
