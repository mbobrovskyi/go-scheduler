package scheduler

import (
	"time"
)

type ExecuteFunction func(ctx Context) (bool, error)

type Schedule struct {
	Name     string
	Interval time.Duration
	Function ExecuteFunction
	Counter  int64
	LastRun  time.Time
}

func (s Schedule) retryInterval() time.Duration {
	if s.Interval < time.Minute {
		return time.Second
	}
	return time.Minute
}
