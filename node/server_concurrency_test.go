package node

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/sakthivel-p-cse/crdt-rate-limiter/crdt"
	pb "github.com/sakthivel-p-cse/crdt-rate-limiter/proto"
	"github.com/stretchr/testify/assert"
)

func TestGossipState_NoGlobalLockContention(t *testing.T) {
	cfg := Config{
		NodeID:         "test-node",
		ListenAddr:     ":0",
		EpochSize:      1 * time.Second,
		WindowSize:     60 * time.Second,
		GossipInterval: 500 * time.Millisecond,
		SuspectTimeout: 2 * time.Second,
		PingTimeout:    1 * time.Second,
		Fanout:         3,
		RateLimit:      1000,
	}
	srv := NewRateLimiterServer(cfg)

	unrelatedClient := "fast-client"
	srv.getWindow(unrelatedClient)

	req := &pb.GossipRequest{
		SourceNodeId: "peer-1",
		State:        make(map[string]*pb.ClientState),
	}

	numClients := 10000
	for i := 0; i < numClients; i++ {
		clientID := fmt.Sprintf("heavy-client-%d", i)
		cs := &pb.ClientState{
			Epochs: make(map[int64]*pb.EpochState),
		}

		for j := int64(0); j < 10; j++ {
			cs.Epochs[j] = &pb.EpochState{
				NodeCounts: map[string]int64{"peer-1": 100},
			}
		}
		req.State[clientID] = cs

		srv.windows[clientID] = crdt.NewEpochWindow(cfg.EpochSize, cfg.WindowSize)
	}

	var wg sync.WaitGroup
	startGossip := make(chan struct{})

	allowCount := 200
	latencies := make([]time.Duration, allowCount)

	wg.Add(1)
	go func() {
		defer wg.Done()
		<-startGossip
		for i := 0; i < allowCount; i++ {
			start := time.Now()
			_, _ = srv.Allow(context.Background(), &pb.AllowRequest{
				ClientId: unrelatedClient,
				Cost:     1,
			})
			latencies[i] = time.Since(start)

			time.Sleep(500 * time.Microsecond)
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		close(startGossip)
		_, _ = srv.GossipState(context.Background(), req)
	}()

	wg.Wait()

	var maxLatency time.Duration
	var sumLatency time.Duration
	for _, l := range latencies {
		if l > maxLatency {
			maxLatency = l
		}
		sumLatency += l
	}
	avgLatency := sumLatency / time.Duration(allowCount)

	t.Logf("Allow() latency during merge: max=%v, avg=%v", maxLatency, avgLatency)

	assert.Less(t, maxLatency.Milliseconds(), int64(15), "Allow() was blocked by GossipState global lock contention")
}
