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
