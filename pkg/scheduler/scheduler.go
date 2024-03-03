package scheduler

import (
	"context"
	"errors"
	"fmt"
	"github.com/mbobrovskyi/go-scheduler/pkg/logger"
	"github.com/mbobrovskyi/go-scheduler/pkg/sleep"
	"time"

	"golang.org/x/exp/maps"
	"golang.org/x/sync/errgroup"
)

var (
	SchedulerAlreadyStartedError = errors.New("scheduler already started")
	ScheduleAlreadyExistError    = errors.New("schedule already exist")
)

type Scheduler interface {
	Add(name string, interval time.Duration, f ExecuteFunction) error
	Schedules() []Schedule
	Start(ctx context.Context) error
}

type schedulerImpl struct {
	log                 logger.Logger
	schedulerEntityRepo SchedulerEntityRepo
	schedules           map[string]Schedule
	isStarted           bool
}

func (s *schedulerImpl) Add(name string, interval time.Duration, f ExecuteFunction) error {
	if s.isStarted {
		return SchedulerAlreadyStartedError
	}

	if _, ok := s.schedules[name]; ok {
		return ScheduleAlreadyExistError
	}

	s.schedules[name] = Schedule{
		Name:     name,
		Interval: interval,
		Function: f,
	}

	return nil
}

func (s *schedulerImpl) Schedules() []Schedule {
	return maps.Values(s.schedules)
}

func (s *schedulerImpl) Start(ctx context.Context) error {
	if s.isStarted {
		return SchedulerAlreadyStartedError
	}

	s.isStarted = true
	defer func() {
		s.isStarted = false
	}()

	errGroup, ctx := errgroup.WithContext(ctx)

	for _, schedule := range s.schedules {
		func(schedule Schedule) {
			errGroup.Go(func() error {
				if err := s.init(ctx, schedule.Name); err != nil {
					return err
				}

				if err := s.startSchedule(ctx, schedule); err != nil {
					return err
				}

				return nil
			})
		}(schedule)
	}

	if err := errGroup.Wait(); err != nil && !errors.Is(err, context.Canceled) {
		return err
	}

	return nil
}

func (s *schedulerImpl) init(ctx context.Context, name string) error {
	if err := s.schedulerEntityRepo.Init(ctx, name); err != nil {
		return fmt.Errorf("failed to init schedule '%s': %w", name, err)
	}

	return nil
}

func (s *schedulerImpl) startSchedule(ctx context.Context, schedule Schedule) error {
	s.log.Debugf("Schedule '%s': started", schedule.Name)

	retryInterval := schedule.retryInterval()

	for {
		select {
		case <-ctx.Done():
			s.log.Debugf("Schedule '%s': stopped", schedule.Name)
			return ctx.Err()
		case now := <-time.After(time.Until(schedule.LastRun.Add(schedule.Interval))):
			s.log.Debugf("Schedule '%s': running", schedule.Name)

			schedulerEntity, err := s.schedulerEntityRepo.GetAndSetLastRun(ctx, schedule.Name, now.Add(-schedule.Interval))
			if err != nil {
				if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
					s.log.Errorf("Schedule '%s': error on get schedule and set last run: %s", schedule.Name, err.Error())
				}
				continue
			}

			if schedulerEntity == nil {
				sleep.WithContext(ctx, retryInterval)
				continue
			}

			schedule.Counter++
			schedule.LastRun = schedulerEntity.LastRun
			executionTime := now

			if !schedulerEntity.LastSuccess.IsZero() {
				executionTime = schedulerEntity.LastSuccess.Add(schedule.Interval)
			}

			scheduleCtx := NewContext(ctx, s.schedulerEntityRepo,
				schedule.Name, schedule.Interval,
				*schedulerEntity, schedule.Counter, executionTime)

			success, err := schedule.Function(scheduleCtx)
			if err != nil {
				if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
					s.log.Errorf("Schedule '%s': error on execute function: %s", schedule.Name, err.Error())
				}
				errMessage := err.Error()
				schedulerEntity.LastError = &errMessage
			} else {
				schedulerEntity.LastError = nil
			}

			schedulerEntity.LastFinishedAt = time.Now().UTC()

			if success {
				schedulerEntity.LastSuccess = schedulerEntity.LastRun
			}

			if err = s.schedulerEntityRepo.Save(ctx, *schedulerEntity); err != nil {
				if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
					s.log.Errorf("Schedule '%s': error on save schedule: %s", schedule.Name, err.Error())
				}
				continue
			}
		}
	}
}

func NewScheduler(log logger.Logger, schedulerEntityRepo SchedulerEntityRepo) Scheduler {
	return &schedulerImpl{
		log:                 log,
		schedulerEntityRepo: schedulerEntityRepo,
		schedules:           make(map[string]Schedule),
	}
}
