package sleep_test

import (
	"context"
	"github.com/mbobrovskyi/go-scheduler/pkg/sleep"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestSleepWithContext(t *testing.T) {
	timeoutSeconds := 5
	timeout := time.Duration(timeoutSeconds) * time.Second

	start := time.Now()

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	sleep.WithContext(ctx, 2*timeout)

	assert.Equal(t, timeoutSeconds, int(time.Now().Sub(start)/time.Second))
}
