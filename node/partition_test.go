package node_test

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"
	"fmt"

	"github.com/sakthivel-p-cse/crdt-rate-limiter/node"
	pb "github.com/sakthivel-p-cse/crdt-rate-limiter/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"
)

const bufSize = 1 << 20

// partitionedDialer routes gRPC connections through in-memory bufconn listeners
// and can toggle a network partition on/off.
type partitionedDialer struct {
	mu          sync.RWMutex
	partitioned bool
	listeners   map[string]*bufconn.Listener
}

func (d *partitionedDialer) setPartition(on bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.partitioned = on
}

func (d *partitionedDialer) dial(ctx context.Context, addr string) (net.Conn, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	if d.partitioned {
		return nil, net.ErrClosed
	}
	lis, ok := d.listeners[addr]
	if !ok {
		return nil, &net.OpError{Op: "dial", Err: &net.AddrError{Err: "no listener for address", Addr: addr}}
	}
	return lis.DialContext(ctx)
}

func TestNetworkPartition(t *testing.T) {
	addrs := []string{"node1:1", "node2:1", "node3:1"}

	dialer := &partitionedDialer{
		listeners: make(map[string]*bufconn.Listener, len(addrs)),
	}
	for _, addr := range addrs {
		dialer.listeners[addr] = bufconn.Listen(bufSize)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	servers := make([]*node.RateLimiterServer, len(addrs))
	grpcServers := make([]*grpc.Server, len(addrs))

	for i, addr := range addrs {
		peers := make([]string, 0, len(addrs)-1)
		for _, p := range addrs {
			if p != addr {
				peers = append(peers, p)
			}
		}

		cfg := node.Config{
			NodeID:         addr,
			ListenAddr:     addr,
			Peers:          peers,
			EpochSize:      100 * time.Millisecond,
			WindowSize:     500 * time.Millisecond,
			GossipInterval: 50 * time.Millisecond,
			SuspectTimeout: 1500 * time.Millisecond,
			PingTimeout:    500 * time.Millisecond,
			Fanout:         2,
			RateLimit:      1000,
		}

		srv := node.NewRateLimiterServer(cfg, grpc.WithContextDialer(dialer.dial))
		grpcSrv := grpc.NewServer()
		pb.RegisterRateLimiterServer(grpcSrv, srv)
		srv.SetGRPCServer(grpcSrv)

		servers[i] = srv
		grpcServers[i] = grpcSrv

		lis := dialer.listeners[addr]
		go func() { _ = grpcSrv.Serve(lis) }()
	}

	for _, srv := range servers {
		require.NoError(t, srv.Start(ctx))
	}

	// Allow SWIM join + initial gossip to settle (extra time under race detector).
	time.Sleep(800 * time.Millisecond)

	// Phase 1 — Node 1 consumes 500 out of 1000.
	resp, err := servers[0].Allow(context.Background(), &pb.AllowRequest{ClientId: "client-test", Cost: 500})
	require.NoError(t, err)
	assert.True(t, resp.Allowed)

	// Wait until Node 2 observes the update via gossip.
	assert.Eventually(t, func() bool {
		fmt.Println("Checking node 2...")
		r, e := servers[1].Allow(context.Background(), &pb.AllowRequest{ClientId: "client-test", Cost: 0})
		return e == nil && (1000-r.Remaining) == 500
	}, 5*time.Second, 50*time.Millisecond, "Node 2 did not receive gossip from Node 1")

	// Phase 2 — Partition. Both sides think they have 500 remaining.
	dialer.setPartition(true)

	resp, err = servers[0].Allow(context.Background(), &pb.AllowRequest{ClientId: "client-test", Cost: 500})
	require.NoError(t, err)
	assert.True(t, resp.Allowed)

	resp, err = servers[1].Allow(context.Background(), &pb.AllowRequest{ClientId: "client-test", Cost: 500})
	require.NoError(t, err)
	assert.True(t, resp.Allowed)
	// Total globally consumed: 1500 > 1000 → expected bounded overshoot.

	// Phase 3 — Heal and converge.
	dialer.setPartition(false)

	assert.Eventually(t, func() bool {
		r, e := servers[2].Allow(context.Background(), &pb.AllowRequest{ClientId: "client-test", Cost: 0})
		return e == nil && !r.Allowed
	}, 5*time.Second, 50*time.Millisecond, "Node 3 did not converge to deny state after partition heal")

	for _, s := range servers {
		s.Stop()
	}
}