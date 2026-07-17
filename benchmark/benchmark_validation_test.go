package main

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/sakthivel-p-cse/crdt-rate-limiter/node"
	pb "github.com/sakthivel-p-cse/crdt-rate-limiter/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"
)

func dialer(listener *bufconn.Listener) func(context.Context, string) (net.Conn, error) {
	return func(context.Context, string) (net.Conn, error) {
		return listener.Dial()
	}
}

func TestOvershootValidation(t *testing.T) {
	addresses := []string{"bench-node1", "bench-node2", "bench-node3"}
	listeners := make(map[string]*bufconn.Listener)
	for _, addr := range addresses {
		listeners[addr] = bufconn.Listen(1024 * 1024)
	}

	servers := make([]*node.RateLimiterServer, len(addresses))
	grpcServers := make([]*grpc.Server, len(addresses))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	limit := int64(1000)
	gossipInterval := 100 * time.Millisecond
	epochSize := 10 * time.Millisecond

	for i, addr := range addresses {
		peers := []string{}
		for _, p := range addresses {
			if p != addr {
				peers = append(peers, p)
			}
		}

		cfg := node.Config{
			NodeID:         addr,
			ListenAddr:     addr,
			MetricsAddr:    "",
			Peers:          peers,
			EpochSize:      epochSize,
			WindowSize:     1000 * time.Millisecond,
			GossipInterval: gossipInterval,
			SuspectTimeout: 500 * time.Millisecond,
			PingTimeout:    100 * time.Millisecond,
			Fanout:         2,
			RateLimit:      limit,
		}

		// Inject custom transport for in-memory networking
		// Since we can't easily map addr -> listener in a single static grpc.WithContextDialer without a custom resolver, 
		// wait, bufconn dialer only dials one listener. We need a resolver or proxy. 
		// We can reuse the partitionedDialer logic but without partition.
		dialFunc := func(ctx context.Context, dest string) (net.Conn, error) {
			if l, ok := listeners[dest]; ok {
				return l.Dial()
			}
			return nil, net.ErrClosed
		}
		
		server := node.NewRateLimiterServer(cfg, grpc.WithContextDialer(dialFunc))
		grpcServer := grpc.NewServer()
		pb.RegisterRateLimiterServer(grpcServer, server)
		server.SetGRPCServer(grpcServer)

		servers[i] = server
		grpcServers[i] = grpcServer

		go grpcServer.Serve(listeners[addr])
		server.Start(ctx)
	}

	time.Sleep(200 * time.Millisecond) // Let cluster form

	var wg sync.WaitGroup
	var totalAllowed int64
	var mu sync.Mutex

	localRequestRate := 500 // req/sec per node
	duration := 800 * time.Millisecond // strictly less than 1s window so limit doesn't replenish
	totalRequestsPerNode := int(float64(localRequestRate) * duration.Seconds())

	// Fire a sustained load of requests across all nodes
	for i := 0; i < len(servers); i++ {
		wg.Add(1)
		go func(s *node.RateLimiterServer) {
			defer wg.Done()
			var localAllowed int64
			ticker := time.NewTicker(time.Second / time.Duration(localRequestRate))
			defer ticker.Stop()
			
			for j := 0; j < totalRequestsPerNode; j++ {
				<-ticker.C
				resp, _ := s.Allow(context.Background(), &pb.AllowRequest{ClientId: "test-client", Cost: 1})
				if resp != nil && resp.Allowed {
					localAllowed++
				}
			}
			mu.Lock()
			totalAllowed += localAllowed
			mu.Unlock()
		}(servers[i])
	}

	wg.Wait()
	time.Sleep(500 * time.Millisecond) // Wait for gossip to settle

	// Verify theoretical overshoot
	// max_overshoot ≈ (num_nodes - 1) × local_request_rate × gossip_interval (in seconds)
	// We add a 1.5x multiplier to account for goroutine scheduling jitter and network serialization time in the test.
	gossipIntervalSecs := float64(gossipInterval) / float64(time.Second)
	maxOvershoot := int64(float64(len(servers)-1) * float64(localRequestRate) * gossipIntervalSecs * 1.5)

	if totalAllowed > limit+maxOvershoot {
		t.Fatalf("Overshoot exceeded theoretical limit! Allowed: %d, Max Theoretical: %d", totalAllowed, limit+maxOvershoot)
	} else {
		t.Logf("Overshoot within limits. Allowed: %d, Limit: %d, Theoretical Max: %d", totalAllowed, limit, limit+maxOvershoot)
	}

	for _, s := range servers {
		s.Stop()
	}
}
