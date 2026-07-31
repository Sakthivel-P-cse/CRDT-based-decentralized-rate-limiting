package node_test

import (
	"context"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sakthivel-p-cse/crdt-rate-limiter/node"
	pb "github.com/sakthivel-p-cse/crdt-rate-limiter/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

const bufSize = 1 << 20

type partitionedDialer struct {
	mu               sync.RWMutex
	partitioned      bool
	listeners        map[string]*bufconn.Listener
	interceptedCalls int64
}

func (d *partitionedDialer) setPartition(on bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.partitioned = on
}

func (d *partitionedDialer) isPartitioned() bool {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.partitioned
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

func (d *partitionedDialer) interceptor(
	ctx context.Context,
	method string,
	req, reply interface{},
	cc *grpc.ClientConn,
	invoker grpc.UnaryInvoker,
	opts ...grpc.CallOption,
) error {
	if d.isPartitioned() {
		atomic.AddInt64(&d.interceptedCalls, 1)
		return status.Errorf(codes.Unavailable, "network partition active")
	}
	return invoker(ctx, method, req, reply, cc, opts...)
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
			EpochSize:      200 * time.Millisecond,
			WindowSize:     30 * time.Second,
			GossipInterval: 50 * time.Millisecond,
			SuspectTimeout: 1500 * time.Millisecond,
			PingTimeout:    500 * time.Millisecond,
			Fanout:         2,
			RateLimit:      1000,
		}

		srv := node.NewRateLimiterServer(cfg,
			grpc.WithContextDialer(dialer.dial),
			grpc.WithUnaryInterceptor(dialer.interceptor),
		)
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

	time.Sleep(800 * time.Millisecond)

	resp, err := servers[0].Allow(context.Background(), &pb.AllowRequest{ClientId: "client-test", Cost: 500})
	require.NoError(t, err)
	assert.True(t, resp.Allowed)

	assert.Eventually(t, func() bool {
		fmt.Println("Checking node 2...")
		window := servers[1].Windows()["client-test"]
		return window != nil && window.Total(time.Now()) == 500
	}, 5*time.Second, 50*time.Millisecond, "Node 2 did not receive gossip from Node 1")

	pingCtx, pingCancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer pingCancel()
	conn, err := grpc.DialContext(pingCtx, "node2:1",
		grpc.WithContextDialer(dialer.dial),
		grpc.WithUnaryInterceptor(dialer.interceptor),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	require.NoError(t, err)
	defer conn.Close()
	client := pb.NewRateLimiterClient(conn)
	_, err = client.Ping(pingCtx, &pb.PingRequest{NodeId: "node1:1"})
	require.NoError(t, err)
	beforeIntercepts := atomic.LoadInt64(&dialer.interceptedCalls)

	dialer.setPartition(true)

	_, pingErr := client.Ping(context.Background(), &pb.PingRequest{NodeId: "node1:1"})
	require.Error(t, pingErr, "direct ping on the pooled connection must fail during partition")
	statusValue, ok := status.FromError(pingErr)
	require.True(t, ok)
	assert.Equal(t, codes.Unavailable, statusValue.Code())
	assert.Contains(t, statusValue.Message(), "network partition active")
	assert.Greater(t, atomic.LoadInt64(&dialer.interceptedCalls), beforeIntercepts)

	resp, err = servers[0].Allow(context.Background(), &pb.AllowRequest{ClientId: "client-test", Cost: 500})
	require.NoError(t, err)
	assert.True(t, resp.Allowed)

	resp, err = servers[1].Allow(context.Background(), &pb.AllowRequest{ClientId: "client-test", Cost: 500})
	require.NoError(t, err)
	assert.True(t, resp.Allowed)

	time.Sleep(150 * time.Millisecond)
	window := servers[1].Windows()["client-test"]
	require.NotNil(t, window)
	assert.Equal(t, int64(1000), window.Total(time.Now()))

	dialer.setPartition(false)

	assert.Eventually(t, func() bool {
		window := servers[2].Windows()["client-test"]
		return window != nil && window.Total(time.Now()) >= 1500
	}, 5*time.Second, 100*time.Millisecond, "Node 3 did not converge to deny state after partition heal")
	denied, err := servers[2].Allow(context.Background(), &pb.AllowRequest{ClientId: "client-test", Cost: 1})
	require.NoError(t, err)
	assert.False(t, denied.Allowed)

	for _, s := range servers {
		s.Stop()
	}
}
