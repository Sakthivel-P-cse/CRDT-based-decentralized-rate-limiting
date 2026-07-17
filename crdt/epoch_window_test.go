package crdt

import (
	"testing"
	"time"
)

func TestEpochWindow_WindowingEdges(t *testing.T) {
	w := NewEpochWindow(1*time.Second, 5*time.Second)

	now := time.Now()

	// Increment in current epoch.
	w.AllowAndIncrement("nodeA", 1000, 10, now)
	if val := w.Value(now); val != 10 {
		t.Fatalf("expected 10, got %d", val)
	}

	// Increment 4 seconds ago (still inside the 5-second window).
	w.AllowAndIncrement("nodeB", 1000, 5, now.Add(-4*time.Second))
	if val := w.Value(now); val != 15 {
		t.Fatalf("expected 15, got %d", val)
	}

	// Advance time by 2 seconds; the -4s bucket is now 6s old → outside window.
	// Trigger eviction by writing a zero-cost increment.
	now = now.Add(2 * time.Second)
	w.AllowAndIncrement("nodeA", 1000, 0, now)

	if val := w.Value(now); val != 10 {
		t.Fatalf("after eviction expected 10, got %d", val)
	}
}