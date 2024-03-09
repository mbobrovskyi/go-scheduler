package goscheduler_test

import (
	"context"
	"fmt"
	"github.com/docker/docker/api/types/container"
	"github.com/mbobrovskyi/golibs/database/mongodb"
	"github.com/mbobrovskyi/golibs/database/postgres"
	"github.com/mbobrovskyi/golibs/database/redis"
	"github.com/mbobrovskyi/golibs/logger"
	"github.com/mbobrovskyi/goscheduler"
	"github.com/mbobrovskyi/goscheduler/repository"
	"github.com/mbobrovskyi/goscheduler/repository/memory"
	"github.com/mbobrovskyi/goscheduler/repository/mongo"
	postgresrepository "github.com/mbobrovskyi/goscheduler/repository/postgres"
	redisrepository "github.com/mbobrovskyi/goscheduler/repository/redis"
	"github.com/stretchr/testify/assert"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestSchedulerInMemory(t *testing.T) {
	testScheduler(t, memory.NewInMemorySchedulerEntityRepo())
}

func TestSchedulerMultipleInMemory(t *testing.T) {
	testSchedulerMultiple(t, memory.NewInMemorySchedulerEntityRepo())
}

func TestSchedulerMongodb(t *testing.T) {
	testScheduler(t, mongodbSchedulerEntityRepo(t))
}

func TestSchedulerMongodbMultiple(t *testing.T) {
	testSchedulerMultiple(t, mongodbSchedulerEntityRepo(t))
}

func TestSchedulerRedis(t *testing.T) {
	testScheduler(t, redisSchedulerEntityRepo(t))
}

func TestSchedulerRedisMultiple(t *testing.T) {
	testSchedulerMultiple(t, redisSchedulerEntityRepo(t))
}

func TestSchedulerPostgres(t *testing.T) {
	testScheduler(t, postgresSchedulerEntityRepo(t))
}

func TestSchedulerPostgresMultiple(t *testing.T) {
	testSchedulerMultiple(t, postgresSchedulerEntityRepo(t))
}

func mongodbSchedulerEntityRepo(t *testing.T) repository.SchedulerEntityRepo {
	ctx := context.Background()

	req := testcontainers.ContainerRequest{
		Image:        "mongo:latest",
		ExposedPorts: []string{"27017/tcp"},
		HostConfigModifier: func(hostConfig *container.HostConfig) {
			hostConfig.AutoRemove = true
		},
		WaitingFor: wait.ForAll(
			wait.ForLog("Waiting for connections"),
			wait.ForListeningPort("27017/tcp"),
		),
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		if err := container.Terminate(ctx); err != nil {
			t.Fatalf("failed to terminate container: %s", err)
		}
	})

	endpoint, err := container.Endpoint(ctx, "mongodb")
	if err != nil {
		t.Fatal(fmt.Errorf("failed to get endpoint: %w", err))
	}

	dbClient, err := mongodb.NewMongoDbClient(context.Background(), endpoint)
	if err != nil {
		t.Fatal(err)
	}

	db := dbClient.Database("test")

	options := &mongo.MongoDBSchedulerEntityRepoOptions{
		CollectionName: "test-scheduler",
	}
	schedulerRepo := mongo.NewMongoDBSchedulerEntityRepo(db, options)

	return schedulerRepo
}

func redisSchedulerEntityRepo(t *testing.T) repository.SchedulerEntityRepo {
	ctx := context.Background()

	req := testcontainers.ContainerRequest{
		Image:        "redis:latest",
		ExposedPorts: []string{"6379/tcp"},
		HostConfigModifier: func(hostConfig *container.HostConfig) {
			hostConfig.AutoRemove = true
		},
		WaitingFor: wait.ForLog("Ready to accept connections"),
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		if err := container.Terminate(ctx); err != nil {
			t.Fatalf("failed to terminate container: %s", err)
		}
	})

	ip, err := container.Host(ctx)
	if err != nil {
		t.Fatal(err)
	}

	mappedPort, err := container.MappedPort(ctx, "6379")
	if err != nil {
		t.Fatal(err)
	}

	db, err := redis.NewRedisClient(context.Background(), fmt.Sprintf("%s:%s", ip, mappedPort.Port()), "", 0)
	if err != nil {
		t.Fatal(err)
	}

	options := &redisrepository.RedisSchedulerRepoOptions{
		Key: "test-scheduler",
	}
	schedulerRepo := redisrepository.NewRedisSchedulerEntityRepo(db, options)

	return schedulerRepo
}

func postgresSchedulerEntityRepo(t *testing.T) repository.SchedulerEntityRepo {
	containerPort := "5432/tcp"
	imageName := "postgres:latest"
	ctx := context.Background()
	req := testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image: imageName,
			Env: map[string]string{
				"POSTGRES_PASSWORD": "postgres",
				"POSTGRES_USER":     "postgres",
				"POSTGRES_DB":       "test",
			},
			ExposedPorts: []string{containerPort},
			WaitingFor:   wait.ForLog("database system is ready to accept connections"),
		},
		Started: true,
	}

	postgresContainer, err := testcontainers.GenericContainer(ctx, req)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		if err := postgresContainer.Terminate(ctx); err != nil {
			t.Fatalf("failed to terminate container: %s", err)
		}
	})

	ip, err := postgresContainer.Host(ctx)
	if err != nil {
		t.Fatal(err)
	}

	mappedPort, err := postgresContainer.MappedPort(ctx, "5432")
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(2 * time.Second)

	connectionString := fmt.Sprintf("postgresql://postgres:postgres@%s:%s/test?sslmode=disable", ip, mappedPort.Port())
	db, err := postgres.NewPostgresClient(
		ctx,
		connectionString,
	)
	if err != nil {
		t.Fatal(err)
	}

	options := &postgresrepository.PostgresSchedulerEntityRepoOptions{
		TableName: "test_scheduler",
	}
	schedulerRepo := postgresrepository.NewPostgresSchedulerEntity(db, options)

	return schedulerRepo
}

func testScheduler(t *testing.T, schedulerEntityRepo repository.SchedulerEntityRepo) {
	s := goscheduler.NewSchedulerWithOptions(goscheduler.SchedulerOptions{
		Logger:              logger.NewTestLogger(t),
		SchedulerEntityRepo: schedulerEntityRepo,
	})

	var (
		count         int64 = 5
		executedCount atomic.Int64
	)

	if err := s.Add(fmt.Sprintf("schedule"), time.Second, func(ctx goscheduler.Context) (bool, error) {
		t.Logf("Counter: %d. Last Run: %s. Last Finished At: %s. Last Success: %s.",
			ctx.Counter(),
			ctx.SchedulerEntity().LastRun.Format(time.RFC3339),
			ctx.SchedulerEntity().LastFinishedAt.Format(time.RFC3339),
			ctx.SchedulerEntity().LastSuccess.Format(time.RFC3339))
		executedCount.Add(1)
		if ctx.Counter()%2 == 0 {
			return true, nil
		} else {
			return false, nil
		}
	}); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(count)*time.Second)
	err := s.Start(ctx)
	if err != nil && err != context.DeadlineExceeded {
		t.Fatal(err)
	}

	cancel()

	assert.Equal(t, count, executedCount.Load(), "they should be equal")
}

func testSchedulerMultiple(t *testing.T, schedulerEntityRepo repository.SchedulerEntityRepo) {
	const (
		intervalSec = 2
		interval    = intervalSec * time.Second
		duration    = 10
		schedulers  = 3
		schedules   = 3
	)

	ctx, cancel := context.WithTimeout(context.Background(), duration*time.Second)

	executedCount := atomic.Int64{}

	wg := sync.WaitGroup{}

	for i := 0; i < schedulers; i++ {
		wg.Add(1)

		go func(i int) {
			defer wg.Done()

			s := goscheduler.NewSchedulerWithOptions(goscheduler.SchedulerOptions{
				Logger:              logger.NewTestLogger(t),
				SchedulerEntityRepo: schedulerEntityRepo,
			})

			for j := 0; j < schedules; j++ {
				func(i, j int) {
					if err := s.Add(fmt.Sprintf("schedule %d", j+1), interval, func(ctx goscheduler.Context) (bool, error) {
						t.Logf("Counter: %d. Last Run: %s. Last Success: %s. Last Finished At: %s. Scheduler: %d. Schedule %d.",
							ctx.Counter(),
							ctx.SchedulerEntity().LastRun.Format(time.RFC3339),
							ctx.SchedulerEntity().LastFinishedAt.Format(time.RFC3339),
							ctx.SchedulerEntity().LastSuccess.Format(time.RFC3339), i+1, j+1)
						executedCount.Add(1)
						if ctx.Counter()%2 == 0 {
							return true, nil
						} else {
							return false, nil
						}
					}); err != nil {
						t.Fatal(err)
					}
				}(i, j)
			}

			err := s.Start(ctx)
			if err != nil && err != context.DeadlineExceeded {
				t.Fatal(err)
			}
		}(i)
	}

	wg.Wait()

	cancel()

	assert.Equal(t, int64(duration/intervalSec*schedules), executedCount.Load(), "they should be equal")
}
