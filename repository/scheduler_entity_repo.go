package repository

import (
	"context"
	"github.com/mbobrovskyi/goscheduler/entity"
	"time"
)

type SchedulerEntityRepo interface {
	Init(ctx context.Context, name string) error
	GetAndSetLastRun(ctx context.Context, name string, lastRunTo time.Time) (*entity.SchedulerEntity, error)
	Save(ctx context.Context, scheduler entity.SchedulerEntity) error
}
