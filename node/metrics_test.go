package node

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/sakthivel-p-cse/crdt-rate-limiter/crdt"
	pb "github.com/sakthivel-p-cse/crdt-rate-limiter/proto"
	"github.com/stretchr/testify/assert"
)

func TestOvershootMetric_AllClients(t *testing.T) {
	cfg := Config{
		NodeID:     "node-metric-test",
		RateLimit:  10,
		WindowSize: 60 * time.Second,
		EpochSize:  1 * time.Second,
	}
	srv := NewRateLimiterServer(cfg)

	client1 := "client-over-1"
	client2 := "client-over-2"
	client3 := "client-under-3"

	w1 := crdt.NewEpochWindow(cfg.EpochSize, cfg.WindowSize)
	w1.AllowAndIncrement("node-metric-test", 1000, 15, time.Now())

	w2 := crdt.NewEpochWindow(cfg.EpochSize, cfg.WindowSize)
	w2.AllowAndIncrement("node-metric-test", 1000, 20, time.Now())

	w3 := crdt.NewEpochWindow(cfg.EpochSize, cfg.WindowSize)
	w3.AllowAndIncrement("node-metric-test", 1000, 5, time.Now())

	srv.windows[client1] = w1
	srv.windows[client2] = w2
	srv.windows[client3] = w3

	srv.recordOvershootMetric()

	g1, err := estimatedOvershoot.GetMetricWithLabelValues(cfg.NodeID, client1)
	assert.NoError(t, err)
	val1 := testutil.ToFloat64(g1)
	assert.Equal(t, 0.5, val1)

	g2, err := estimatedOvershoot.GetMetricWithLabelValues(cfg.NodeID, client2)
	assert.NoError(t, err)
	val2 := testutil.ToFloat64(g2)
	assert.Equal(t, 1.0, val2)

	g3, err := estimatedOvershoot.GetMetricWithLabelValues(cfg.NodeID, client3)
	assert.NoError(t, err)
	val3 := testutil.ToFloat64(g3)
	assert.Equal(t, 0.0, val3)

	localClient := "local-client"
	SetOvershoot(cfg.NodeID, localClient, 1)
	_, err = srv.Allow(context.Background(), &pb.AllowRequest{ClientId: localClient, Cost: 1})
	assert.NoError(t, err)
	localGauge, err := estimatedOvershoot.GetMetricWithLabelValues(cfg.NodeID, localClient)
	assert.NoError(t, err)
	assert.Equal(t, 0.0, testutil.ToFloat64(localGauge))
}
