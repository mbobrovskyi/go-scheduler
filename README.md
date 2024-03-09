# Go Scheduler (beta)

Golang scheduler that allow to save last run state in database and run only on one replica.


## Repositories 

Supported repositories [Add a new repository?](scheduler_entity_repo.go)

1. [In memory](in_memory_scheduler_entity_repo.go).
2. [Redis](redis_scheduler_entity_repo.go).
3. MongoDB.
4. Postgres.


## How to use

```go
package main

import (
	"context"
	"fmt"
	"github.com/mbobrovskyi/goscheduler"
	"time"
)

func main() {
	var scheduler = goscheduler.NewScheduler()
	err := scheduler.Add("My test scheduler", time.Second, func(ctx goscheduler.Context) (bool, error) {
		fmt.Println("Scheduler name:", ctx.SchedulerEntity().Name)
		return true, nil
	})
	if err != nil {
		panic(err)
	}

	err = scheduler.Start(context.Background())
	if err != nil {
		panic(err)
	}
}
```