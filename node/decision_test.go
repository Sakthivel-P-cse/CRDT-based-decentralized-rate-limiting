package node

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTierShedderShouldShed(t *testing.T) {
	shedder := &TierShedder{}
	tests := []struct {
		name        string
		utilization float64
		priority    Priority
		want        bool
	}{
		{"below critical", 0.69, PriorityCritical, false},
		{"below standard", 0.69, PriorityStandard, false},
		{"below best effort", 0.69, PriorityBestEffort, false},
		{"degraded critical", 0.70, PriorityCritical, false},
		{"degraded standard", 0.70, PriorityStandard, false},
		{"degraded best effort", 0.70, PriorityBestEffort, true},
		{"severe critical", 0.90, PriorityCritical, false},
		{"severe standard", 0.90, PriorityStandard, true},
		{"severe best effort", 0.90, PriorityBestEffort, true},
		{"exhausted critical", 1.00, PriorityCritical, true},
		{"exhausted standard", 1.00, PriorityStandard, true},
		{"exhausted best effort", 1.00, PriorityBestEffort, true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.want, shedder.ShouldShed(test.priority, test.utilization))
		})
	}
}

func TestRejectProbability(t *testing.T) {
	tests := []struct {
		current int64
		want    float64
	}{
		{70, 0},
		{80, 1.0 / 3.0},
		{85, 0.5},
		{100, 1},
	}

	for _, test := range tests {
		assert.InDelta(t, test.want, RejectProbability(test.current, 70, 100), 1e-12)
	}
}

func TestAdaptiveBudgetAIMD(t *testing.T) {
	budget := NewAdaptiveBudget(1000)

	budget.OnEpochComplete(true)
	assert.Equal(t, int64(500), budget.Current())
	budget.OnEpochComplete(false)
	assert.Equal(t, int64(525), budget.Current())
	budget.OnEpochComplete(false)
	assert.Equal(t, int64(551), budget.Current())
	assert.GreaterOrEqual(t, budget.Current(), int64(100))
	assert.LessOrEqual(t, budget.Current(), int64(1000))
}
