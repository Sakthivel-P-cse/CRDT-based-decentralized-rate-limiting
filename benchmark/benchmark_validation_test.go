package main

import (
	"context"
	"fmt"
	"math"
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
	const (
		numRuns  = 3
		numNodes = 3
		limit    = int64(1000)
		localRPS = 300
	)

	addresses := []string{"bench-node1", "bench-node2", "bench-node3"}
	gossipInterval := 100 * time.Millisecond

	runDuration := 3 * time.Second

	convergenceRounds := 4
	rawMaxOvershoot := int64(float64(numNodes) * float64(localRPS) * gossipInterval.Seconds() * float64(convergenceRounds))

	fmt.Printf("\n=== TestOvershootValidation ===\n")
	fmt.Printf("Nodes: %d, Limit: %d, localRPS/node: %d, gossipInterval: %v, duration: %v\n",
		numNodes, limit, localRPS, gossipInterval, runDuration)
	fmt.Printf("Corrected formula: N × RPS × gossip_interval × convergence_rounds = %d × %d × %.3f × %d = %d\n",
		numNodes, localRPS, gossipInterval.Seconds(), convergenceRounds, rawMaxOvershoot)
	fmt.Printf("No fudge factor applied. README updated with corrected formula.\n\n")

	var runResults []int64

	for run := 0; run < numRuns; run++ {
		allowed := runOnce(t, addresses, gossipInterval, limit, localRPS, runDuration)
		runResults = append(runResults, allowed)
		fmt.Printf("Run %d: totalAllowed=%d, limit=%d, overshoot=%d (rawMax=%d)\n",
			run+1, allowed, limit, max64(0, allowed-limit), rawMaxOvershoot)
	}

	var sum float64
	for _, r := range runResults {
		sum += float64(r)
	}
	mean := sum / float64(numRuns)

	var variance float64
	for _, r := range runResults {
		d := float64(r) - mean
		variance += d * d
	}
	variance /= float64(numRuns)
	stddev := math.Sqrt(variance)

	fmt.Printf("\n--- Summary over %d runs ---\n", numRuns)
	fmt.Printf("Mean totalAllowed: %.1f\n", mean)
	fmt.Printf("Std deviation:     %.1f\n", stddev)
	fmt.Printf("Raw formula bound: limit + rawMaxOvershoot = %d + %d = %d\n",
		limit, rawMaxOvershoot, limit+rawMaxOvershoot)

	for i, allowed := range runResults {
		overshoot := allowed - limit
		if overshoot < 0 {
			overshoot = 0
		}
		t.Logf("Run %d: totalAllowed=%d, overshoot=%d, rawMaxOvershoot=%d", i+1, allowed, overshoot, rawMaxOvershoot)
		if allowed > limit+rawMaxOvershoot {
			t.Errorf("Run %d: overshoot exceeded raw theoretical bound! totalAllowed=%d, limit+rawMax=%d",
				i+1, allowed, limit+rawMaxOvershoot)
		}
	}
}

func runOnce(t *testing.T, addresses []string, gossipInterval time.Duration, limit int64, localRPS int, duration time.Duration) int64 {
	t.Helper()

	listeners := make(map[string]*bufconn.Listener)
	for _, addr := range addresses {
		listeners[addr] = bufconn.Listen(1024 * 1024)
	}

	servers := make([]*node.RateLimiterServer, len(addresses))
	grpcServers := make([]*grpc.Server, len(addresses))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	epochSize := 50 * time.Millisecond

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
			WindowSize:     10 * time.Second,
			GossipInterval: gossipInterval,
			SuspectTimeout: 500 * time.Millisecond,
			PingTimeout:    100 * time.Millisecond,
			Fanout:         2,
			RateLimit:      limit,
		}

		dialFunc := func(ctx context.Context, dest string) (net.Conn, error) {
			if l, ok := listeners[dest]; ok {
				return l.Dial()
			}
			return nil, fmt.Errorf("no listener for %s", dest)
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

	time.Sleep(200 * time.Millisecond)

	var wg sync.WaitGroup
	var mu sync.Mutex
	var totalAllowed int64

	for i := 0; i < len(servers); i++ {
		wg.Add(1)
		go func(s *node.RateLimiterServer) {
			defer wg.Done()
			var localAllowed int64
			interval := time.Second / time.Duration(localRPS)
			ticker := time.NewTicker(interval)
			defer ticker.Stop()
			deadline := time.Now().Add(duration)
			for time.Now().Before(deadline) {
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
	time.Sleep(300 * time.Millisecond)

	for i, gs := range grpcServers {
		gs.GracefulStop()
		servers[i].Stop()
	}

	return totalAllowed
}

func max64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
