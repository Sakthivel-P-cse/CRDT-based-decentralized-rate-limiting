package node

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestMetrics_NoPanicOnBindFail(t *testing.T) {

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	assert.NoError(t, err)
	defer listener.Close()

	cfg := Config{
		NodeID:         "test-metrics-node",
		ListenAddr:     ":0",
		MetricsAddr:    listener.Addr().String(),
		RateLimit:      100,
		WindowSize:     60 * time.Second,
		EpochSize:      1 * time.Second,
		GossipInterval: 1 * time.Second,
		SuspectTimeout: 5 * time.Second,
		PingTimeout:    2 * time.Second,
		Fanout:         3,
	}

	srv := NewRateLimiterServer(cfg)

	assert.NotPanics(t, func() {
		err := srv.Start(context.Background())
		assert.Error(t, err)
	})
}
