package goscheduler

import (
	"context"
	"github.com/mbobrovskyi/goscheduler/entity"
	"github.com/mbobrovskyi/goscheduler/repository"
	"time"
)

type Context interface {
	context.Context

	Name() string
	Interval() time.Duration
	SchedulerEntity() entity.SchedulerEntity
	Counter() int
	ExecutionTime() time.Time
}

type SchedulerContext struct {
	context.Context

	schedulerEntityRepo repository.SchedulerEntityRepo
	schedulerName       string
	schedulerInterval   time.Duration
	schedulerEntity     entity.SchedulerEntity
	counter             int

	executionTime time.Time
}

func (s *SchedulerContext) Name() string {
	return s.schedulerName
}

func (s *SchedulerContext) Interval() time.Duration {
	return s.schedulerInterval
}

func (s *SchedulerContext) SchedulerEntity() entity.SchedulerEntity {
	return s.schedulerEntity
}

func (s *SchedulerContext) Counter() int {
	return s.counter
}

func (s *SchedulerContext) ExecutionTime() time.Time {
	return s.executionTime
}

func NewContext(
	ctx context.Context,
	schedulerEntityRepo repository.SchedulerEntityRepo,
	name string,
	interval time.Duration,
	schedulerEntity entity.SchedulerEntity,
	counter int,
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
