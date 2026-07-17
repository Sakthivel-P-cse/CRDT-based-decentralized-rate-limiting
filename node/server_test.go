package node_test

import (
	"context"
	"testing"
	"time"

	"github.com/sakthivel-p-cse/crdt-rate-limiter/node"
	pb "github.com/sakthivel-p-cse/crdt-rate-limiter/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func baseConfig() node.Config {
	return node.Config{
		NodeID:         "test-node",
		ListenAddr:     "127.0.0.1:0",
		EpochSize:      1 * time.Second,
		WindowSize:     60 * time.Second,
		RateLimit:      10,
		GossipInterval: 1 * time.Second,
		SuspectTimeout: 2 * time.Second,
		PingTimeout:    500 * time.Millisecond,
		Fanout:         3,
	}
}

func TestRateLimiterServer_Allow(t *testing.T) {
	srv := node.NewRateLimiterServer(baseConfig())
	ctx := context.Background()

	tests := []struct {
		name      string
		clientID  string
		cost      int64
		wantAllow bool
		wantRem   int64
	}{
		{"under limit", "c1", 5, true, 5},
		{"exact limit", "c1", 5, true, 0},
		{"over limit", "c1", 1, false, 0},
		{"separate client isolated", "c2", 8, true, 2},
		{"zero cost probe after exhaustion returns allow=true (no budget consumed)", "c1", 0, true, 0},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			resp, err := srv.Allow(ctx, &pb.AllowRequest{ClientId: tc.clientID, Cost: tc.cost})
			require.NoError(t, err)
			assert.Equal(t, tc.wantAllow, resp.Allowed, "allowed mismatch")
			assert.Equal(t, tc.wantRem, resp.Remaining, "remaining mismatch")
		})
	}
}

func TestRateLimiterServer_Ping_AddsToMembership(t *testing.T) {
	srv := node.NewRateLimiterServer(baseConfig())
	ctx := context.Background()

	resp, err := srv.Ping(ctx, &pb.PingRequest{NodeId: "peer-1", ListenAddress: "127.0.0.1:9001"})
	require.NoError(t, err)
	assert.True(t, resp.Ack)
}

func TestRateLimiterServer_GossipState_MergesIncoming(t *testing.T) {
	srv := node.NewRateLimiterServer(baseConfig())
	ctx := context.Background()

	// Push a gossip payload with 7 units consumed by peer-1 for client-A.
	epochKey := time.Now().UnixNano() / int64(time.Second)
	gossipResp, err := srv.GossipState(ctx, &pb.GossipRequest{
		SourceNodeId: "peer-1",
		State: map[string]*pb.ClientState{
			"client-A": {
				Epochs: map[int64]*pb.EpochState{
					epochKey: {NodeCounts: map[string]int64{"peer-1": 7}},
				},
			},
		},
	})
	require.NoError(t, err)
	assert.True(t, gossipResp.Success)

	// Now only 3 units remain; Allow(4) should be denied.
	r, err := srv.Allow(ctx, &pb.AllowRequest{ClientId: "client-A", Cost: 4})
	require.NoError(t, err)
	assert.False(t, r.Allowed, "should deny: peer already consumed 7 of 10")
}

func TestRateLimiterServer_Lifecycle(t *testing.T) {
	cfg := baseConfig()
	cfg.Peers = []string{"127.0.0.1:9001"} // unreachable — join should be best-effort
	srv := node.NewRateLimiterServer(cfg)

	ctx := context.Background()
	require.NoError(t, srv.Start(ctx))

	srv.Stop() // must not deadlock
}
