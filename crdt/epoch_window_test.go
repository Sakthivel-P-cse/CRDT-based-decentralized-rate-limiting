package crdt

import (
	"testing"
	"time"
)

func TestEpochWindow_WindowingEdges(t *testing.T) {
	w := NewEpochWindow(1*time.Second, 5*time.Second)

	now := time.Now()

	w.AllowAndIncrement("nodeA", 1000, 10, now)
	if val := w.Value(now); val != 10 {
		t.Fatalf("expected 10, got %d", val)
	}

	w.AllowAndIncrement("nodeB", 1000, 5, now.Add(-4*time.Second))
	if val := w.Value(now); val != 15 {
		t.Fatalf("expected 15, got %d", val)
	}

	now = now.Add(2 * time.Second)
	w.AllowAndIncrement("nodeA", 1000, 0, now)

	if val := w.Value(now); val != 10 {
		t.Fatalf("after eviction expected 10, got %d\", val)\n}", val)
	}
}

func TestEpochWindow_ProportionalOldestBucket(t *testing.T) {
	epochSize := 100 * time.Millisecond
	windowSize := 250 * time.Millisecond
	w := NewEpochWindow(epochSize, windowSize)

	rawNow := time.Now()
	epochNs := int64(epochSize)
	nowNs := ((rawNow.UnixNano() + epochNs - 1) / epochNs) * epochNs
	now := time.Unix(0, nowNs)

	ok, _ := w.AllowAndIncrement("node", 10000, 100, now)
	if !ok {
		t.Fatal("expected allow for first increment")
	}

	oldTime := now.Add(-280 * time.Millisecond)
	ok, _ = w.AllowAndIncrement("node", 10000, 100, oldTime)
	if !ok {
		t.Fatal("expected allow for second increment")
	}

	windowStartNs := now.Add(-windowSize).UnixNano()
	oldEpochKey := oldTime.UnixNano() / epochNs
	epochEndNs := (oldEpochKey + 1) * epochNs
	overlapNs := epochEndNs - windowStartNs
	expectedFraction := float64(overlapNs) / float64(epochNs)
	expectedOldCount := int64(float64(100) * expectedFraction)

	expectedTotal := int64(100) + expectedOldCount

	got := w.Total(now)
	if got < expectedTotal-2 || got > expectedTotal+2 {
		t.Errorf("proportional sliding window: got Total=%d, want ~%d (fraction=%.3f, overlap=%dms/%dms)",
			got, expectedTotal, expectedFraction, overlapNs/int64(time.Millisecond), epochNs/int64(time.Millisecond))
	} else {
		t.Logf("proportional sliding window: Total=%d, expected≈%d (overlap %.1f%%)", got, expectedTotal, expectedFraction*100)
	}

	naiveTotal := int64(200)
	if got >= naiveTotal {
		t.Errorf("proportional Total (%d) should be less than naive full-count Total (%d)", got, naiveTotal)
	}
}
