package entity

import (
	"time"
)

type SchedulerEntity struct {
	Name           string    `bson:"name" json:"name"`
	LastRun        time.Time `bson:"lastRun" json:"lastRun"`
	LastFinishedAt time.Time `bson:"lastFinishedAt" json:"lastFinishedAt"`
	LastSuccess    time.Time `bson:"lastSuccess" json:"lastSuccess"`
	LastError      *string   `bson:"lastError" json:"lastError"`
}

func NewSchedulerEntity(name string) SchedulerEntity {
	return SchedulerEntity{Name: name}
}
