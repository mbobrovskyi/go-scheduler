package scheduler

import (
	"context"
	"time"
)

type Context interface {
	context.Context

	Name() string
	Interval() time.Duration
	SchedulerEntity() SchedulerEntity
	Counter() int64
	SaveSchedulerEntity(schedulerEntity SchedulerEntity) error
	ExecutionTime() time.Time
}

type SchedulerContext struct {
	context.Context

	schedulerEntityRepo SchedulerEntityRepo
	schedulerName       string
	schedulerInterval   time.Duration
	schedulerEntity     SchedulerEntity
	counter             int64

	executionTime time.Time
}

func (s *SchedulerContext) Name() string {
	return s.schedulerName
}

func (s *SchedulerContext) Interval() time.Duration {
	return s.schedulerInterval
}

func (s *SchedulerContext) SchedulerEntity() SchedulerEntity {
	return s.schedulerEntity
}

func (s *SchedulerContext) Counter() int64 {
	return s.counter
}

func (s *SchedulerContext) ExecutionTime() time.Time {
	return s.executionTime
}

func (s *SchedulerContext) SaveSchedulerEntity(schedulerEntity SchedulerEntity) error {
	err := s.schedulerEntityRepo.Save(s.Context, schedulerEntity)
	if err != nil {
		return err
	}

	return nil
}

func NewContext(
	ctx context.Context,
	schedulerEntityRepo SchedulerEntityRepo,
	name string,
	interval time.Duration,
	schedulerEntity SchedulerEntity,
	counter int64,
	executionTime time.Time,
) Context {
	return &SchedulerContext{
		Context:             ctx,
		schedulerEntityRepo: schedulerEntityRepo,
		schedulerName:       name,
		schedulerInterval:   interval,
		schedulerEntity:     schedulerEntity,
		counter:             counter,
		executionTime:       executionTime,
	}
}
