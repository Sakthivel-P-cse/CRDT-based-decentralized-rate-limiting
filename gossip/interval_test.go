package gossip

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestGossipInterval(t *testing.T) {
	base := time.Second
	tests := []struct {
		proximity float64
		want      time.Duration
	}{
		{0, time.Second},
		{0.5, 500 * time.Millisecond},
		{0.8, 200 * time.Millisecond},
		{1.0, 200 * time.Millisecond},
	}

	var previous time.Duration
	for i, test := range tests {
		got := GossipInterval(base, test.proximity)
		assert.Equal(t, test.want, got)
		if i > 0 {
			assert.LessOrEqual(t, got, previous)
		}
		assert.GreaterOrEqual(t, got, 200*time.Millisecond)
		previous = got
	}
}
