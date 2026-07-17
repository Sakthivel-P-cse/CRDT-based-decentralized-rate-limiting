package crdt

import (
	"sync"
	"time"

	pb "github.com/sakthivel-p-cse/crdt-rate-limiter/proto"
)

// EpochWindow is a sliding-window rate-limit counter backed by CRDT G-Counters.
//
// Time is divided into fixed-size epochs (epochSize). The window spans a rolling
// duration of windowSize. Requests within that window are counted across all
// epochs. Old epochs are lazily evicted on every write.
//
// All methods are safe for concurrent use.
type EpochWindow struct {
	mu         sync.RWMutex
	epochSize  time.Duration
	windowSize time.Duration
	buckets    map[int64]*GCounter // epoch key → per-node counter
}

// NewEpochWindow creates a new EpochWindow.
// epochSize is the granularity of each time bucket; windowSize is the total
// rolling interval considered for rate limiting.
func NewEpochWindow(epochSize, windowSize time.Duration) *EpochWindow {
	return &EpochWindow{
		epochSize:  epochSize,
		windowSize: windowSize,
		buckets:    make(map[int64]*GCounter),
	}
}

// epochKey converts a wall-clock time to a monotonically increasing bucket key.
func (w *EpochWindow) epochKey(t time.Time) int64 {
	return t.UnixNano() / int64(w.epochSize)
}

// AllowAndIncrement atomically checks whether adding cost to the window would
// exceed limit, and if not, records the increment for nodeID.
//
// Returns (allowed, remaining) where remaining is the budget left after this
// decision. If denied, remaining reflects the budget before the attempted increment.
func (w *EpochWindow) AllowAndIncrement(nodeID string, limit, cost int64, now time.Time) (bool, int64) {
	w.mu.Lock()
	defer w.mu.Unlock()

	minKey := w.epochKey(now.Add(-w.windowSize))
	var total int64
	for key, counter := range w.buckets {
		if key >= minKey {
			// Read the raw counts inline to avoid a nested lock acquisition.
			counter.mu.RLock()
			for _, v := range counter.counts {
				total += v
			}
			counter.mu.RUnlock()
		}
	}

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

// Value returns the total count across all epochs that fall within the window.
func (w *EpochWindow) Value(now time.Time) int64 {
	w.mu.RLock()
	defer w.mu.RUnlock()

	minKey := w.epochKey(now.Add(-w.windowSize))
	var total int64
	for key, counter := range w.buckets {
		if key >= minKey {
			total += counter.Value()
		}
	}
	return total
}

// Merge merges the state of other into w using per-epoch, per-node maximums.
// This is the standard CRDT merge operation and is idempotent, commutative,
// and associative.
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

// ToProto serialises the window to its protobuf representation.
// Only epochs with epoch key >= epochKey(since) are included, enabling
// delta gossip: pass time.Time{} (zero value) to serialise all epochs.
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

// MergeFromProto deserialises a protobuf ClientState and merges it into w.
// Nil or empty inputs are silently ignored.
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

// evict removes epochs that have fallen outside the window. Must be called
// with w.mu held for writing.
func (w *EpochWindow) evict(now time.Time) {
	minKey := w.epochKey(now.Add(-w.windowSize))
	for key := range w.buckets {
		if key < minKey {
			delete(w.buckets, key)
		}
	}
}