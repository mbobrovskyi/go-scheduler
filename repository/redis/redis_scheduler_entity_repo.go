package redis

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"github.com/mbobrovskyi/goscheduler/entity"
	"github.com/redis/go-redis/v9"
	"time"
)

const DefaultRedisKey = "schedulers"

type RedisSchedulerRepoOptions struct {
	Key string
}

type RedisSchedulerEntityRepo struct {
	db *redis.Client

	key string
}

func (s *RedisSchedulerEntityRepo) getKey(name string) string {
	return fmt.Sprintf("%s:%s", s.key, name)
}

func (s *RedisSchedulerEntityRepo) getLockKey(name string) string {
	return fmt.Sprintf("%s:lock", s.getKey(name))
}

func (s *RedisSchedulerEntityRepo) Init(ctx context.Context, name string) error {
	key := s.getKey(name)
	value, err := json.Marshal(entity.NewSchedulerEntity(name))
	if err != nil {
		return err
	}

	if err := s.db.SetNX(ctx, key, value, 0).Err(); err != nil {
		return err
	}
	return nil
}

func (s *RedisSchedulerEntityRepo) GetAndSetLastRun(ctx context.Context, name string, lastRunTo time.Time) (*entity.SchedulerEntity, error) {
	const expiration = 5 * time.Second

	lockKey := s.getLockKey(name)

	id := uuid.New().String()

	if err := s.db.SetNX(ctx, lockKey, id, expiration).Err(); err != nil {
		return nil, err
	}

	value, err := s.db.Get(ctx, lockKey).Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		return nil, err
	}

	if value != id {
		return nil, nil
	}

	defer s.db.Del(ctx, lockKey)

	schedulerEntity, err := s.get(ctx, name)
	if err != nil {
		return nil, err
	}

	if schedulerEntity == nil {
		return nil, nil
	}

	// After or equal
	if !schedulerEntity.LastRun.Before(lastRunTo) {
		return nil, nil
	}

	schedulerEntity.LastRun = time.Now().UTC()

	if err := s.set(ctx, *schedulerEntity); err != nil {
		return nil, err
	}

	return schedulerEntity, nil
}

func (s *RedisSchedulerEntityRepo) Save(ctx context.Context, schedulerEntity entity.SchedulerEntity) error {
	return s.set(ctx, schedulerEntity)
}

func (s *RedisSchedulerEntityRepo) get(ctx context.Context, name string) (*entity.SchedulerEntity, error) {
	value, err := s.db.Get(ctx, s.getKey(name)).Bytes()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, nil
		}
		return nil, err
	}

	var schedulerEntity entity.SchedulerEntity

	if err := json.Unmarshal(value, &schedulerEntity); err != nil {
		return nil, err
	}

	return &schedulerEntity, nil
}

func (s *RedisSchedulerEntityRepo) set(ctx context.Context, schedulerEntity entity.SchedulerEntity) error {
	key := s.getKey(schedulerEntity.Name)

	value, err := json.Marshal(schedulerEntity)
	if err != nil {
		return err
	}

	if err := s.db.Set(ctx, key, value, 0).Err(); err != nil {
		return err
	}

	return nil
}

func NewRedisSchedulerEntityRepo(
	db *redis.Client,
	options *RedisSchedulerRepoOptions,
) *RedisSchedulerEntityRepo {
	repo := &RedisSchedulerEntityRepo{
		db: db,
	}

	if options != nil && options.Key == "" {
		repo.key = options.Key
	} else {
		repo.key = DefaultRedisKey
	}

	return repo
}
