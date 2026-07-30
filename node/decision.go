package node

import (
	"sync"
	"time"
)

type Priority int

const (
	PriorityCritical Priority = iota
	PriorityStandard
	PriorityBestEffort
)

type TierShedder struct{}

func (t *TierShedder) ShouldShed(priority Priority, utilizationRatio float64) bool {
	switch {
	case utilizationRatio < 0.70:
		return false
	case utilizationRatio < 0.90:
		return priority == PriorityBestEffort
	case utilizationRatio < 1.00:
		return priority != PriorityCritical
	default:
		return true
	}
}

func RejectProbability(currentCount, softThreshold, hardLimit int64) float64 {
	if currentCount <= softThreshold {
		return 0
	}
	if currentCount >= hardLimit {
		return 1
	}
	return float64(currentCount-softThreshold) / float64(hardLimit-softThreshold)
}

type AdaptiveBudget struct {
	mu                     sync.RWMutex
	current                int64
	min                    int64
	max                    int64
	additiveIncrease       int64
	multiplicativeDecrease float64
}

func NewAdaptiveBudget(originalStaticLimit int64) *AdaptiveBudget {
	minimum := int64(float64(originalStaticLimit) * 0.1)
	if minimum < 1 {
		minimum = 1
	}
	return &AdaptiveBudget{
		current:                originalStaticLimit,
		min:                    minimum,
		max:                    originalStaticLimit,
		multiplicativeDecrease: 0.5,
	}
}

func (b *AdaptiveBudget) Current() int64 {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.current
}

func (b *AdaptiveBudget) OnEpochComplete(contributedToOvershoot bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if contributedToOvershoot {
		b.current = max(b.min, int64(float64(b.current)*b.multiplicativeDecrease))
		return
	}
	b.additiveIncrease = int64(float64(b.current) * 0.05)
	if b.additiveIncrease < 1 && b.current < b.max {
		b.additiveIncrease = 1
	}
	b.current = min(b.max, b.current+b.additiveIncrease)
}

const retryAfterSafetyMargin = 50 * time.Millisecond

func EstimatedRetryAfter(currentGossipInterval time.Duration, convergenceRounds int) time.Duration {
	return currentGossipInterval*time.Duration(convergenceRounds) + retryAfterSafetyMargin
}
