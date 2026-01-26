package adapter

import (
	"context"

	"github.com/go-redis/redis_rate/v10"
	"github.com/redis/go-redis/v9"
)

// RedisClient defines the interface for Redis operations to enable mocking
//
//go:generate mockgen -source=redis.go -destination=../mocks/redis.go -package=mocks -mock_names=RedisClient=MockRedisClient
type RedisClient interface {
	// Ping checks if Redis is reachable
	Ping(ctx context.Context) *redis.StatusCmd

	// NewRateLimiter creates a new rate limiter using this Redis client
	NewRateLimiter() RedisRateLimiter

	// Close closes the Redis connection
	Close() error
}

// RealRedisClient wraps the actual Redis client
type RealRedisClient struct {
	client *redis.Client
}

// NewRedisClient creates a new Redis client
func NewRedisClient(addr, password string, db int) RedisClient {
	return &RealRedisClient{
		client: redis.NewClient(&redis.Options{
			Addr:     addr,
			Password: password,
			DB:       db,
		}),
	}
}

// Ping checks if Redis is reachable
func (r *RealRedisClient) Ping(ctx context.Context) *redis.StatusCmd {
	return r.client.Ping(ctx)
}

// NewRateLimiter creates a new rate limiter using this Redis client
func (r *RealRedisClient) NewRateLimiter() RedisRateLimiter {
	return NewRateLimiter(redis_rate.NewLimiter(r.client))
}

// Close closes the Redis connection
func (r *RealRedisClient) Close() error {
	return r.client.Close()
}

// RedisRateLimiter defines the interface for distributed rate limiting operations
//
//go:generate mockgen -source=redis.go -destination=../mocks/redis.go -package=mocks -mock_names=RedisRateLimiter=MockRedisRateLimiter
type RedisRateLimiter interface {
	// Allow checks if a request is allowed based on the rate limit
	// Returns the result containing allowed status and retry information
	Allow(ctx context.Context, key string, limit redis_rate.Limit) (*redis_rate.Result, error)
}

// RealRateLimiter wraps the redis_rate.Limiter
type RealRateLimiter struct {
	limiter *redis_rate.Limiter
}

// NewRateLimiter creates a new rate limiter from a redis_rate.Limiter
func NewRateLimiter(limiter *redis_rate.Limiter) RedisRateLimiter {
	return &RealRateLimiter{
		limiter: limiter,
	}
}

// Allow checks if a request is allowed based on the rate limit
func (r *RealRateLimiter) Allow(ctx context.Context, key string, limit redis_rate.Limit) (*redis_rate.Result, error) {
	return r.limiter.Allow(ctx, key, limit)
}
