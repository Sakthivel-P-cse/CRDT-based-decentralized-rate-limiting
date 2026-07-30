package crdt

import (
	"sync"
	"time"

	pb "github.com/sakthivel-p-cse/crdt-rate-limiter/proto"
)

type EpochWindow struct {
	mu         sync.RWMutex
	epochSize  time.Duration
	windowSize time.Duration
	buckets    map[int64]*GCounter
}

func NewEpochWindow(epochSize, windowSize time.Duration) *EpochWindow {
	return &EpochWindow{
		epochSize:  epochSize,
		windowSize: windowSize,
		buckets:    make(map[int64]*GCounter),
	}
}

func (w *EpochWindow) epochKey(t time.Time) int64 {
	return t.UnixNano() / int64(w.epochSize)
}

func (w *EpochWindow) weightedTotal(now time.Time) int64 {
	windowStart := now.Add(-w.windowSize)
	minKey := w.epochKey(windowStart)

	var total int64
	for key, counter := range w.buckets {
		if key < minKey {
			continue
		}
		rawCount := counterSum(counter)
		if key == minKey {

			epochEndNs := (minKey + 1) * int64(w.epochSize)
			windowStartNs := windowStart.UnixNano()
			overlapNs := epochEndNs - windowStartNs
			if overlapNs <= 0 {

				continue
			}
			epochSizeNs := int64(w.epochSize)
			if overlapNs >= epochSizeNs {

				total += rawCount
			} else {

				total += rawCount * overlapNs / epochSizeNs
			}
		} else {

			total += rawCount
		}
	}
	return total
}

func counterSum(c *GCounter) int64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	var sum int64
	for _, v := range c.counts {
		sum += v
	}
	return sum
}

func (w *EpochWindow) AllowAndIncrement(nodeID string, limit, cost int64, now time.Time) (bool, int64) {
	w.mu.Lock()
	defer w.mu.Unlock()

	total := w.weightedTotal(now)

	if total+cost <= limit {
		key := w.epochKey(now)
		if w.buckets[key] == nil {
			w.buckets[key] = NewGCounter()
		}
		w.buckets[key].Increment(nodeID, cost)
		w.evict(now)
		return true, limit - (total + cost)
	}

	w.evict(now)
	return false, limit - total
}

func (w *EpochWindow) Total(now time.Time) int64 {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.weightedTotal(now)
}

func (w *EpochWindow) ContributedInPreviousEpoch(nodeID string, now time.Time) bool {
	w.mu.RLock()
	defer w.mu.RUnlock()
	counter := w.buckets[w.epochKey(now)-1]
	if counter == nil {
		return false
	}
	return counter.Counts()[nodeID] > 0
}

func (w *EpochWindow) Value(now time.Time) int64 {
	return w.Total(now)
}

func (w *EpochWindow) Merge(other *EpochWindow) {
	other.mu.RLock()
	snapshot := make(map[int64]map[string]int64, len(other.buckets))
	for key, counter := range other.buckets {
		snapshot[key] = counter.Counts()
	}
	other.mu.RUnlock()

	w.mu.Lock()
	defer w.mu.Unlock()
	for key, nodeCounts := range snapshot {
		if w.buckets[key] == nil {
			w.buckets[key] = NewGCounter()
		}
		for nodeID, count := range nodeCounts {
			w.buckets[key].Set(nodeID, count)
		}
	}
}

func (w *EpochWindow) ToProto(since time.Time) *pb.ClientState {
	w.mu.RLock()
	defer w.mu.RUnlock()

	var sinceKey int64
	if !since.IsZero() {
		sinceKey = w.epochKey(since)
	}

	state := &pb.ClientState{
		Epochs: make(map[int64]*pb.EpochState),
	}
	for key, counter := range w.buckets {
		if key < sinceKey {
			continue
		}
		counts := counter.Counts()
		if len(counts) > 0 {
			state.Epochs[key] = &pb.EpochState{NodeCounts: counts}
		}
	}
	if len(state.Epochs) == 0 {
		return nil
	}
	return state
}

func (w *EpochWindow) MergeFromProto(cs *pb.ClientState) {
	if cs == nil {
		return
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	for key, epochState := range cs.Epochs {
		if epochState == nil {
			continue
		}
		if w.buckets[key] == nil {
			w.buckets[key] = NewGCounter()
		}
		for nodeID, count := range epochState.NodeCounts {
			if count > 0 {
				w.buckets[key].Set(nodeID, count)
			}
		}
	}
}

func (w *EpochWindow) evict(now time.Time) {
	minKey := w.epochKey(now.Add(-w.windowSize))
	for key := range w.buckets {
		if key < minKey {
			delete(w.buckets, key)
		}
	}
}
