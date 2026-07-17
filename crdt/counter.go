package crdt

import "sync"

// GCounter is a Grow-Only Counter CRDT.
// Each node in the cluster maintains its own monotonically increasing slot.
// The total value is the sum of all slots. Merging takes the per-node maximum,
// which guarantees commutativity, associativity, and idempotency.
type GCounter struct {
	mu     sync.RWMutex
	counts map[string]int64
}

// NewGCounter creates a new, empty GCounter.
func NewGCounter() *GCounter {
	return &GCounter{
		counts: make(map[string]int64),
	}
}

// Increment adds val to nodeID's slot. Non-positive values are silently ignored
// to preserve the grow-only invariant.
func (c *GCounter) Increment(nodeID string, val int64) {
	if val <= 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.counts[nodeID] += val
}

// Merge applies the per-node maximum of other into c.
// This operation is safe to call concurrently.
func (c *GCounter) Merge(other *GCounter) {
	other.mu.RLock()
	otherCounts := make(map[string]int64, len(other.counts))
	for k, v := range other.counts {
		otherCounts[k] = v
	}
	other.mu.RUnlock()

	c.mu.Lock()
	defer c.mu.Unlock()
	for nodeID, count := range otherCounts {
		if count > c.counts[nodeID] {
			c.counts[nodeID] = count
		}
	}
}

// Value returns the sum of all node slots.
func (c *GCounter) Value() int64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	var total int64
	for _, count := range c.counts {
		total += count
	}
	return total
}

// Counts returns a snapshot of all per-node slot values.
// Used for gossip serialisation.
func (c *GCounter) Counts() map[string]int64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make(map[string]int64, len(c.counts))
	for k, v := range c.counts {
		out[k] = v
	}
	return out
}

// Get returns the current count for the given node slot.
func (c *GCounter) Get(nodeID string) int64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.counts[nodeID]
}

// Set raises the count for nodeID to val if val is greater than the current
// value. This is the merge primitive used when deserialising gossip payloads.
func (c *GCounter) Set(nodeID string, val int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if val > c.counts[nodeID] {
		c.counts[nodeID] = val
	}
}