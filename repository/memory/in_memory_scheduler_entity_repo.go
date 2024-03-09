package memory

import (
	"context"
	"github.com/mbobrovskyi/goscheduler/entity"
	"sync"
	"time"
)

type InMemorySchedulerEntityRepo struct {
	mtx               sync.Mutex
	schedulerEntities map[string]entity.SchedulerEntity
}

func (r *InMemorySchedulerEntityRepo) Init(ctx context.Context, name string) error {
	r.mtx.Lock()
	defer r.mtx.Unlock()

	if _, ok := r.schedulerEntities[name]; !ok {
		r.schedulerEntities[name] = entity.NewSchedulerEntity(name)
	}

	return nil
}

func (r *InMemorySchedulerEntityRepo) GetAndSetLastRun(ctx context.Context, name string, lastRunTo time.Time) (*entity.SchedulerEntity, error) {
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

func (r *InMemorySchedulerEntityRepo) Save(ctx context.Context, schedulerEntity entity.SchedulerEntity) error {
	r.mtx.Lock()
	defer r.mtx.Unlock()

	r.schedulerEntities[schedulerEntity.Name] = schedulerEntity
	return nil
}

func NewInMemorySchedulerEntityRepo() *InMemorySchedulerEntityRepo {
	return &InMemorySchedulerEntityRepo{
		schedulerEntities: make(map[string]entity.SchedulerEntity),
	}
}
