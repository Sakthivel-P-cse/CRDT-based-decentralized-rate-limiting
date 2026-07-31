package node_test

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/sakthivel-p-cse/crdt-rate-limiter/node"
	pb "github.com/sakthivel-p-cse/crdt-rate-limiter/proto"
	"github.com/sakthivel-p-cse/crdt-rate-limiter/util"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
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

type blockingPingServer struct {
	pb.UnimplementedRateLimiterServer
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (s *blockingPingServer) Ping(ctx context.Context, _ *pb.PingRequest) (*pb.PingResponse, error) {
	s.once.Do(func() { close(s.entered) })
	select {
	case <-s.release:
		return &pb.PingResponse{Ack: true}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
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

func TestRateLimiterServer_RejectsNonPositiveCost(t *testing.T) {
	srv := node.NewRateLimiterServer(baseConfig())
	before := len(srv.Windows())

	for _, cost := range []int64{-1, 0} {
		resp, err := srv.Allow(context.Background(), &pb.AllowRequest{ClientId: "invalid-cost", Cost: cost})
		require.Error(t, err)
		assert.Nil(t, resp)
	}

	assert.Len(t, srv.Windows(), before)
	resp, err := srv.Allow(context.Background(), &pb.AllowRequest{ClientId: "invalid-cost", Cost: 1})
	require.NoError(t, err)
	assert.Equal(t, int64(9), resp.Remaining)
}

func TestRateLimiterServer_Ping_AddsToMembership(t *testing.T) {
	srv := node.NewRateLimiterServer(baseConfig())
	ctx := context.Background()

	resp, err := srv.Ping(ctx, &pb.PingRequest{NodeId: "peer-1", ListenAddress: "127.0.0.1:9001"})
	require.NoError(t, err)
	assert.True(t, resp.Ack)

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	targetConfig := baseConfig()
	targetConfig.NodeID = "target"
	targetConfig.ListenAddr = lis.Addr().String()
	target := node.NewRateLimiterServer(targetConfig)
	targetGRPC := grpc.NewServer()
	pb.RegisterRateLimiterServer(targetGRPC, target)
	go util.SafeGo("target-test-server", func() { _ = targetGRPC.Serve(lis) })
	defer lis.Close()
	_, err = srv.Ping(ctx, &pb.PingRequest{NodeId: "target", ListenAddress: lis.Addr().String()})
	require.NoError(t, err)

	resp, err = srv.Ping(ctx, &pb.PingRequest{
		NodeId:        "origin",
		ListenAddress: "origin:1",
		TargetNodeId:  "target",
	})
	require.NoError(t, err)
	assert.True(t, resp.Ack)

	targetGRPC.Stop()
	resp, err = srv.Ping(ctx, &pb.PingRequest{
		NodeId:        "origin",
		ListenAddress: "origin:1",
		TargetNodeId:  "target",
	})
	require.NoError(t, err)
	assert.False(t, resp.Ack)
}

func TestRateLimiterServer_GossipState_MergesIncoming(t *testing.T) {
	srv := node.NewRateLimiterServer(baseConfig())
	ctx := context.Background()

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

	r, err := srv.Allow(ctx, &pb.AllowRequest{ClientId: "client-A", Cost: 4})
	require.NoError(t, err)
	assert.False(t, r.Allowed, "should deny: peer already consumed 7 of 10")
}

func TestRateLimiterServer_Lifecycle(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	peer := grpc.NewServer()
	probe := &blockingPingServer{entered: make(chan struct{}), release: make(chan struct{})}
	pb.RegisterRateLimiterServer(peer, probe)
	go util.SafeGo("lifecycle-test-peer", func() { _ = peer.Serve(lis) })
	t.Cleanup(func() {
		peer.Stop()
		_ = lis.Close()
	})

	cfg := baseConfig()
	cfg.Peers = []string{lis.Addr().String()}
	srv := node.NewRateLimiterServer(cfg)

	startDone := make(chan error, 1)
	go func() {
		startDone <- srv.Start(context.Background())
	}()
	select {
	case <-probe.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("start did not reach cluster join")
	}
	stopDone := make(chan struct{})
	go func() {
		srv.Stop()
		close(stopDone)
	}()
	select {
	case <-stopDone:
		t.Fatal("stop returned before start completed")
	case <-time.After(50 * time.Millisecond):
	}
	close(probe.release)
	require.NoError(t, <-startDone)
	select {
	case <-stopDone:
	case <-time.After(2 * time.Second):
		t.Fatal("concurrent stop did not complete")
	}
	require.Error(t, srv.Start(context.Background()))

	stoppedBeforeStart := node.NewRateLimiterServer(baseConfig())
	stoppedBeforeStart.Stop()
	require.Error(t, stoppedBeforeStart.Start(context.Background()))
}
