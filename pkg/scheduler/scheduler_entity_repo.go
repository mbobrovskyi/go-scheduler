package scheduler

import (
	"context"
	"time"
)

type SchedulerEntityRepo interface {
	Init(ctx context.Context, name string) error
	GetAndSetLastRun(ctx context.Context, name string, lastRunTo time.Time) (*SchedulerEntity, error)
	Save(ctx context.Context, scheduler SchedulerEntity) error
}
