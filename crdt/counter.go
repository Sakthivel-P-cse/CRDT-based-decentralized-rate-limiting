package crdt

import "sync"

type GCounter struct {
	mu     sync.RWMutex
	counts map[string]int64
}

func NewGCounter() *GCounter {
	return &GCounter{
		counts: make(map[string]int64),
	}
}

func (c *GCounter) Increment(nodeID string, val int64) {
	if val <= 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.counts[nodeID] += val
}

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

func (c *GCounter) Value() int64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	var total int64
	for _, count := range c.counts {
		total += count
	}
	return total
}

func (c *GCounter) Counts() map[string]int64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make(map[string]int64, len(c.counts))
	for k, v := range c.counts {
		out[k] = v
	}
	return out
}

func (c *GCounter) Get(nodeID string) int64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.counts[nodeID]
}

func (c *GCounter) Set(nodeID string, val int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if val > c.counts[nodeID] {
		c.counts[nodeID] = val
	}
}
