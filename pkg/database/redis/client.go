package redis

import (
	"context"
	"github.com/redis/go-redis/v9"
)

func NewRedisClient(ctx context.Context, redisHost, password string, redisDB int) (*redis.Client, error) {
	return NewRedisClientWithOptions(ctx, &redis.Options{
		Addr:     redisHost,
		Password: password,
		DB:       redisDB,
	})
}

func NewRedisClientWithOptions(ctx context.Context, opts *redis.Options) (*redis.Client, error) {
	redisClient := redis.NewClient(opts)

	if err := redisClient.Ping(ctx).Err(); err != nil {
		return nil, err
	}

	return redisClient, nil
}
