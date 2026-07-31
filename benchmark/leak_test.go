package main

import (
	"context"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestBenchmark_NoGoroutineLeak(t *testing.T) {

	time.Sleep(100 * time.Millisecond)
	baseline := runtime.NumGoroutine()

	bc := &BenchmarkClient{
		mode:         "redis",
		clientIDs:    []string{"client-1"},
		redisLimiter: NewRedisLimiter("localhost:9999", 10, time.Second),
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	bc.RunBenchmark(ctx, 10, 500*time.Millisecond, 1)

	time.Sleep(200 * time.Millisecond)

	final := runtime.NumGoroutine()

	assert.LessOrEqual(t, final, baseline+2, "Benchmark leaked goroutines")
}

type dummyLimiter struct{}

func (d *dummyLimiter) Allow(ctx context.Context, clientID string, cost int64) (bool, int64, error) {
	return true, 0, nil
}
func (d *dummyLimiter) Close() error { return nil }
