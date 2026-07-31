package util

import (
	"context"
	"log/slog"
	"runtime/debug"
	"time"
)

func SafeGo(name string, fn func()) {
	defer func() {
		if r := recover(); r != nil {
			slog.Error("panic in goroutine", "name", name, "panic", r, "stack", string(debug.Stack()))
		}
	}()
	fn()
}

func SafeGoLoop(name string, ctx context.Context, fn func()) {
	var restarts uint64
	for ctx.Err() == nil {
		panicked := runLoopIteration(name, fn, &restarts)
		if ctx.Err() != nil {
			return
		}
		if !panicked {
			slog.Warn("goroutine loop returned before cancellation", "name", name)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func runLoopIteration(name string, fn func(), restarts *uint64) (panicked bool) {
	defer func() {
		if r := recover(); r != nil {
			*restarts = *restarts + 1
			panicked = true
			slog.Error("panic in goroutine loop", "name", name, "panic", r, "restart", *restarts, "stack", string(debug.Stack()))
		}
	}()
	fn()
	return false
}
