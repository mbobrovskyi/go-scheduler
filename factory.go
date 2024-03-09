package goscheduler

import (
	"github.com/mbobrovskyi/goscheduler/repository"
	"github.com/mbobrovskyi/goscheduler/repository/memory"
)

type SchedulerOptions struct {
	Logger              Logger                         // Default no logs
	SchedulerEntityRepo repository.SchedulerEntityRepo // Default InMemorySchedulerEntityRepo
}

func NewScheduler() Scheduler {
	return NewSchedulerWithOptions(SchedulerOptions{})
}

func NewSchedulerWithOptions(options SchedulerOptions) Scheduler {
	scheduler := &schedulerImpl{
		logger:              newNopLogger(),
		schedulerEntityRepo: memory.NewInMemorySchedulerEntityRepo(),
		schedules:           make(map[string]Schedule),
	}

	if options.Logger != nil {
		scheduler.logger = options.Logger
	}

	if options.SchedulerEntityRepo != nil {
		scheduler.schedulerEntityRepo = options.SchedulerEntityRepo
	}

	return scheduler
}
