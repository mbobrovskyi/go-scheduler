package scheduler

import (
	"context"
	"sync"
	"time"
)

type InMemorySchedulerEntityRepo struct {
	mtx               sync.Mutex
	schedulerEntities map[string]SchedulerEntity
}

func (r *InMemorySchedulerEntityRepo) Init(ctx context.Context, name string) error {
	r.mtx.Lock()
	defer r.mtx.Unlock()

	if _, ok := r.schedulerEntities[name]; !ok {
		r.schedulerEntities[name] = NewSchedulerEntity(name)
	}

	return nil
}

func (r *InMemorySchedulerEntityRepo) GetAndSetLastRun(ctx context.Context, name string, lastRunTo time.Time) (*SchedulerEntity, error) {
	r.mtx.Lock()
	defer r.mtx.Unlock()

	schedulerEntity := r.schedulerEntities[name]
	if schedulerEntity.LastRun.Before(lastRunTo) {
		schedulerEntity.LastRun = time.Now().UTC()
		r.schedulerEntities[schedulerEntity.Name] = schedulerEntity
		return &schedulerEntity, nil
	}

	return nil, nil
}

func (r *InMemorySchedulerEntityRepo) Save(ctx context.Context, schedulerEntity SchedulerEntity) error {
	r.mtx.Lock()
	defer r.mtx.Unlock()

	r.schedulerEntities[schedulerEntity.Name] = schedulerEntity
	return nil
}

func NewInMemorySchedulerEntityRepoImpl() *InMemorySchedulerEntityRepo {
	return &InMemorySchedulerEntityRepo{
		schedulerEntities: make(map[string]SchedulerEntity),
	}
}
