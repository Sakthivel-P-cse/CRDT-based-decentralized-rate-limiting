package tests

import (
	"context"
	"net"
	"strings"
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

const chaosBufferSize = 1 << 20

type priorityStats struct {
	accepted map[pb.Priority]int
	total    map[pb.Priority]int
}

type remoteLoad struct {
	epoch int64
	count int64
}

type chaosNetwork struct {
	mu          sync.RWMutex
	listeners   map[string]*bufconn.Listener
	blocked     map[string]map[string]bool
	intercepted atomic.Int64
}

type chaosCluster struct {
	t           *testing.T
	network     *chaosNetwork
	ctx         context.Context
	cancel      context.CancelFunc
	configs     map[string]node.Config
	servers     map[string]*node.RateLimiterServer
	grpcServers map[string]*grpc.Server
}

func (n *chaosNetwork) dial(ctx context.Context, address string) (net.Conn, error) {
	n.mu.RLock()
	listener := n.listeners[address]
	n.mu.RUnlock()
	if listener == nil {
		return nil, &net.OpError{Op: "dial", Err: &net.AddrError{Err: "unknown address", Addr: address}}
	}
	return listener.DialContext(ctx)
}

func (n *chaosNetwork) interceptor(
	ctx context.Context,
	method string,
	request, reply interface{},
	connection *grpc.ClientConn,
	invoker grpc.UnaryInvoker,
	options ...grpc.CallOption,
) error {
	source := ""
	switch typed := request.(type) {
	case *pb.GossipRequest:
		source = typed.SourceNodeId
	case *pb.PingRequest:
		source = typed.NodeId
	}
	target := n.targetAddress(connection.Target())
	n.mu.RLock()
	blocked := n.blocked[source][target]
	n.mu.RUnlock()
	if blocked {
		n.intercepted.Add(1)
		return status.Error(codes.Unavailable, "chaos network partition")
	}
	return invoker(ctx, method, request, reply, connection, options...)
}

func (n *chaosNetwork) targetAddress(target string) string {
	n.mu.RLock()
	defer n.mu.RUnlock()
	for address := range n.listeners {
		if strings.Contains(target, address) {
			return address
		}
	}
	return target
}

func (n *chaosNetwork) setBlocked(source, target string, blocked bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.blocked[source] == nil {
		n.blocked[source] = make(map[string]bool)
	}
	n.blocked[source][target] = blocked
}

func newChaosCluster(t *testing.T, configs ...node.Config) *chaosCluster {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	cluster := &chaosCluster{
		t:           t,
		network:     &chaosNetwork{listeners: make(map[string]*bufconn.Listener), blocked: make(map[string]map[string]bool)},
		ctx:         ctx,
		cancel:      cancel,
		configs:     make(map[string]node.Config),
		servers:     make(map[string]*node.RateLimiterServer),
		grpcServers: make(map[string]*grpc.Server),
	}
	addresses := make([]string, len(configs))
	for i, config := range configs {
		addresses[i] = config.ListenAddr
		cluster.network.listeners[config.ListenAddr] = bufconn.Listen(chaosBufferSize)
	}
	for _, config := range configs {
		config.Peers = config.Peers[:0]
		for _, address := range addresses {
			if address != config.ListenAddr {
				config.Peers = append(config.Peers, address)
			}
		}
		cluster.configs[config.NodeID] = config
		cluster.addServer(config)
	}
	for _, config := range configs {
		require.NoError(t, cluster.servers[config.NodeID].Start(ctx))
	}
	t.Cleanup(cluster.close)
	return cluster
}

func (c *chaosCluster) addServer(config node.Config) *node.RateLimiterServer {
	c.t.Helper()
	server := node.NewRateLimiterServer(
		config,
		grpc.WithContextDialer(c.network.dial),
		grpc.WithUnaryInterceptor(c.network.interceptor),
	)
	grpcServer := grpc.NewServer()
	pb.RegisterRateLimiterServer(grpcServer, server)
	server.SetGRPCServer(grpcServer)
	listener := c.network.listeners[config.ListenAddr]
	go func() {
		_ = grpcServer.Serve(listener)
	}()
	c.servers[config.NodeID] = server
	c.grpcServers[config.NodeID] = grpcServer
	return server
}

func (c *chaosCluster) stop(nodeID string) {
	c.t.Helper()
	c.servers[nodeID].Stop()
	config := c.configs[nodeID]
	c.network.mu.Lock()
	_ = c.network.listeners[config.ListenAddr].Close()
	c.network.mu.Unlock()
}

func (c *chaosCluster) restore(nodeID string) *node.RateLimiterServer {
	c.t.Helper()
	config := c.configs[nodeID]
	c.network.mu.Lock()
	c.network.listeners[config.ListenAddr] = bufconn.Listen(chaosBufferSize)
	c.network.mu.Unlock()
	server := c.addServer(config)
	require.NoError(c.t, server.Start(c.ctx))
	return server
}

func (c *chaosCluster) gossip(sourceID, targetID string) {
	c.t.Helper()
	target := c.configs[targetID].ListenAddr
	connection, err := grpc.DialContext(
		c.ctx,
		target,
		grpc.WithContextDialer(c.network.dial),
		grpc.WithUnaryInterceptor(c.network.interceptor),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(c.t, err)
	defer connection.Close()
	_, err = pb.NewRateLimiterClient(connection).GossipState(c.ctx, &pb.GossipRequest{
		SourceNodeId: sourceID,
		State:        c.servers[sourceID].GenerateDelta(time.Time{}),
	})
	require.NoError(c.t, err)
}

func (c *chaosCluster) close() {
	c.cancel()
	for nodeID, server := range c.servers {
		server.Stop()
		c.grpcServers[nodeID].Stop()
	}
	c.network.mu.Lock()
	defer c.network.mu.Unlock()
	for _, listener := range c.network.listeners {
		_ = listener.Close()
	}
}

func newPriorityStats() *priorityStats {
	return &priorityStats{
		accepted: make(map[pb.Priority]int),
		total:    make(map[pb.Priority]int),
	}
}

func (s *priorityStats) acceptance(priority pb.Priority) float64 {
	return float64(s.accepted[priority]) / float64(s.total[priority])
}

func (s *priorityStats) merge(other *priorityStats) {
	for priority, total := range other.total {
		s.total[priority] += total
		s.accepted[priority] += other.accepted[priority]
	}
}

func testConfig(nodeID string, limit int64) node.Config {
	return node.Config{
		NodeID:             nodeID,
		ListenAddr:         nodeID + ":1",
		EpochSize:          24 * time.Hour,
		WindowSize:         48 * time.Hour,
		GossipInterval:     50 * time.Millisecond,
		SuspectTimeout:     500 * time.Millisecond,
		PingTimeout:        200 * time.Millisecond,
		Fanout:             2,
		RateLimit:          limit,
		SoftThresholdRatio: 0.70,
	}
}

func setRemoteCount(t *testing.T, server *node.RateLimiterServer, clientID string, load remoteLoad) {
	t.Helper()
	_, err := server.GossipState(context.Background(), &pb.GossipRequest{
		SourceNodeId: "load-source",
		State: map[string]*pb.ClientState{
			clientID: {
				Epochs: map[int64]*pb.EpochState{
					load.epoch: {NodeCounts: map[string]int64{"load-source": load.count}},
				},
			},
		},
	})
	require.NoError(t, err)
}

func setRemoteTotal(t *testing.T, server *node.RateLimiterServer, clientID string, epochSize time.Duration, target int64) remoteLoad {
	t.Helper()
	load := remoteLoad{
		epoch: time.Now().UnixNano() / int64(epochSize),
		count: target,
	}
	setRemoteCount(t, server, clientID, load)
	return load
}

func moveToUtilization(t *testing.T, server *node.RateLimiterServer, clientID string, epochSize time.Duration, load remoteLoad, target int64) remoteLoad {
	t.Helper()
	now := time.Now()
	total := server.Windows()[clientID].Total(now)
	next := remoteLoad{
		epoch: now.UnixNano() / int64(epochSize),
		count: target - total,
	}
	if next.epoch == load.epoch {
		next.count += load.count
	}
	setRemoteCount(t, server, clientID, next)
	return next
}

func sendPriorityMix(t *testing.T, servers []*node.RateLimiterServer, clientID string, requestsPerPriority int, stats *priorityStats) {
	t.Helper()
	priorities := []pb.Priority{
		pb.Priority_PRIORITY_CRITICAL,
		pb.Priority_PRIORITY_STANDARD,
		pb.Priority_PRIORITY_BEST_EFFORT,
	}
	for _, priority := range priorities {
		for i := 0; i < requestsPerPriority; i++ {
			server := servers[i%len(servers)]
			response, err := server.Allow(context.Background(), &pb.AllowRequest{
				ClientId: clientID,
				Cost:     1,
				Priority: priority,
			})
			require.NoError(t, err)
			stats.total[priority]++
			if response.Allowed {
				stats.accepted[priority]++
			}
		}
	}
}

func TestPriorityAwareDegradationUnderPartition(t *testing.T) {
	const limit = int64(100000)
	firstConfig := testConfig("tier-a", limit)
	firstConfig.SoftThresholdRatio = 0.99
	secondConfig := testConfig("tier-b", limit)
	secondConfig.SoftThresholdRatio = 0.99
	cluster := newChaosCluster(t, firstConfig, secondConfig)
	servers := []*node.RateLimiterServer{
		cluster.servers[firstConfig.NodeID],
		cluster.servers[secondConfig.NodeID],
	}
	response, err := servers[0].Allow(context.Background(), &pb.AllowRequest{
		ClientId: "partition-connectivity",
		Cost:     1,
		Priority: pb.Priority_PRIORITY_CRITICAL,
	})
	require.NoError(t, err)
	require.True(t, response.Allowed)
	require.Eventually(t, func() bool {
		window := servers[1].Windows()["partition-connectivity"]
		return window != nil && window.Total(time.Now()) == 1
	}, time.Second, 10*time.Millisecond)
	cluster.network.setBlocked(firstConfig.NodeID, secondConfig.ListenAddr, true)
	cluster.network.setBlocked(secondConfig.NodeID, firstConfig.ListenAddr, true)
	clientID := "partition-client"
	loads := make([]remoteLoad, len(servers))
	for i := range servers {
		loads[i] = setRemoteTotal(t, servers[i], clientID, firstConfig.EpochSize, int64(0.65*float64(limit)))
	}
	stats := newPriorityStats()
	phaseStats := newPriorityStats()
	sendPriorityMix(t, servers, clientID, 200, phaseStats)
	for _, priority := range []pb.Priority{pb.Priority_PRIORITY_CRITICAL, pb.Priority_PRIORITY_STANDARD, pb.Priority_PRIORITY_BEST_EFFORT} {
		assert.GreaterOrEqual(t, phaseStats.acceptance(priority), 0.99)
	}
	stats.merge(phaseStats)
	initialBestEffortShed := 1 - stats.acceptance(pb.Priority_PRIORITY_BEST_EFFORT)
	for i, server := range servers {
		loads[i] = moveToUtilization(
			t,
			server,
			clientID,
			firstConfig.EpochSize,
			loads[i],
			int64(0.75*float64(limit)),
		)
	}
	phaseStats = newPriorityStats()
	sendPriorityMix(t, servers, clientID, 200, phaseStats)
	assert.GreaterOrEqual(t, phaseStats.acceptance(pb.Priority_PRIORITY_CRITICAL), 0.99)
	assert.GreaterOrEqual(t, phaseStats.acceptance(pb.Priority_PRIORITY_STANDARD), 0.99)
	assert.LessOrEqual(t, phaseStats.acceptance(pb.Priority_PRIORITY_BEST_EFFORT), 0.01)
	stats.merge(phaseStats)
	assert.Greater(t, 1-stats.acceptance(pb.Priority_PRIORITY_BEST_EFFORT), initialBestEffortShed)

	for _, highUtilization := range []float64{0.92, 0.95, 0.98} {
		for i, server := range servers {
			loads[i] = moveToUtilization(
				t,
				server,
				clientID,
				firstConfig.EpochSize,
				loads[i],
				int64(highUtilization*float64(limit)),
			)
		}
		phaseStats = newPriorityStats()
		sendPriorityMix(t, servers, clientID, 200, phaseStats)
		assert.GreaterOrEqual(t, phaseStats.acceptance(pb.Priority_PRIORITY_CRITICAL), 0.99)
		assert.LessOrEqual(t, phaseStats.acceptance(pb.Priority_PRIORITY_STANDARD), 0.01)
		assert.LessOrEqual(t, phaseStats.acceptance(pb.Priority_PRIORITY_BEST_EFFORT), 0.01)
		stats.merge(phaseStats)
		bestEffort := stats.acceptance(pb.Priority_PRIORITY_BEST_EFFORT)
		standard := stats.acceptance(pb.Priority_PRIORITY_STANDARD)
		critical := stats.acceptance(pb.Priority_PRIORITY_CRITICAL)
		assert.Less(t, bestEffort, standard)
		assert.Less(t, standard, critical)
		assert.GreaterOrEqual(t, critical, 0.99)
	}
	require.Eventually(t, func() bool {
		return cluster.network.intercepted.Load() > 0
	}, time.Second, 5*time.Millisecond)
}

func TestSmoothRejectCurveUnderTrafficRamp(t *testing.T) {
	const (
		limit   = int64(1000000000)
		samples = 20000
	)
	points := []float64{0.50, 0.60, 0.70, 0.73, 0.76, 0.79, 0.82, 0.85, 0.88, 0.91, 0.94, 0.97, 1.00, 1.10, 1.20, 1.30, 1.40, 1.50}
	rejectRates := make([]float64, 0, len(points))
	cfg := testConfig("ramp", limit)
	server := node.NewRateLimiterServer(cfg)
	clientID := "ramp-client"
	var load remoteLoad

	for i, point := range points {
		target := int64(point * float64(limit))
		if i == 0 {
			load = setRemoteTotal(t, server, clientID, cfg.EpochSize, target)
		} else {
			load = moveToUtilization(t, server, clientID, cfg.EpochSize, load, target)
		}
		rejected := 0
		for request := 0; request < samples; request++ {
			response, err := server.Allow(context.Background(), &pb.AllowRequest{
				ClientId: clientID,
				Cost:     1,
				Priority: pb.Priority_PRIORITY_CRITICAL,
			})
			require.NoError(t, err)
			if !response.Allowed {
				rejected++
			}
		}
		rejectRates = append(rejectRates, float64(rejected)/samples)
	}

	for i := 1; i < len(rejectRates); i++ {
		assert.GreaterOrEqual(t, rejectRates[i], rejectRates[i-1])
		assert.LessOrEqual(t, rejectRates[i]-rejectRates[i-1], 0.15)
	}
}

func TestAIMDBudgetRecoveryAfterNodeFailure(t *testing.T) {
	survivorConfig := testConfig("recovery-a", 100)
	failedConfig := testConfig("recovery-b", 100)
	relayConfig := testConfig("recovery-c", 100)
	for _, config := range []*node.Config{&survivorConfig, &failedConfig, &relayConfig} {
		config.EpochSize = 200 * time.Millisecond
		config.WindowSize = 5 * time.Second
		config.GossipInterval = 50 * time.Millisecond
	}
	cluster := newChaosCluster(t, survivorConfig, failedConfig, relayConfig)
	survivor := cluster.servers[survivorConfig.NodeID]
	failed := cluster.servers[failedConfig.NodeID]
	relay := cluster.servers[relayConfig.NodeID]
	for _, other := range []node.Config{failedConfig, relayConfig} {
		cluster.network.setBlocked(survivorConfig.NodeID, other.ListenAddr, true)
		cluster.network.setBlocked(other.NodeID, survivorConfig.ListenAddr, true)
	}

	require.Eventually(t, func() bool {
		return time.Now().UnixNano()%int64(survivorConfig.EpochSize) < int64(20*time.Millisecond)
	}, survivorConfig.EpochSize, time.Millisecond)
	localEpoch := time.Now().UnixNano() / int64(survivorConfig.EpochSize)
	response, err := survivor.Allow(context.Background(), &pb.AllowRequest{
		ClientId: "recovery-client",
		Cost:     20,
		Priority: pb.Priority_PRIORITY_CRITICAL,
	})
	require.NoError(t, err)
	require.True(t, response.Allowed)
	response, err = failed.Allow(context.Background(), &pb.AllowRequest{
		ClientId: "recovery-client",
		Cost:     90,
		Priority: pb.Priority_PRIORITY_CRITICAL,
	})
	require.NoError(t, err)
	require.True(t, response.Allowed)
	require.Eventually(t, func() bool {
		window := relay.Windows()["recovery-client"]
		return window != nil && window.Total(time.Now()) >= 90
	}, 150*time.Millisecond, 5*time.Millisecond)
	cluster.stop(failedConfig.NodeID)
	require.Eventually(t, func() bool {
		return time.Now().UnixNano()/int64(survivorConfig.EpochSize) == localEpoch+1
	}, 2*survivorConfig.EpochSize, time.Millisecond)
	cluster.network.setBlocked(survivorConfig.NodeID, relayConfig.ListenAddr, false)
	cluster.network.setBlocked(relayConfig.NodeID, survivorConfig.ListenAddr, false)
	cluster.gossip(relayConfig.NodeID, survivorConfig.NodeID)
	require.Eventually(t, func() bool {
		window := survivor.Windows()["recovery-client"]
		return window != nil && window.Total(time.Now()) >= 110
	}, 500*time.Millisecond, 5*time.Millisecond)
	require.Eventually(t, func() bool {
		return survivor.CurrentBudget() == 50
	}, 3*survivorConfig.EpochSize, 5*time.Millisecond)

	cluster.network.setBlocked(survivorConfig.NodeID, failedConfig.ListenAddr, false)
	cluster.network.setBlocked(failedConfig.NodeID, survivorConfig.ListenAddr, false)
	restored := cluster.restore(failedConfig.NodeID)
	response, err = restored.Allow(context.Background(), &pb.AllowRequest{
		ClientId: "restored-client",
		Cost:     1,
		Priority: pb.Priority_PRIORITY_CRITICAL,
	})
	require.NoError(t, err)
	require.True(t, response.Allowed)
	require.Eventually(t, func() bool {
		window := survivor.Windows()["restored-client"]
		return window != nil && window.Total(time.Now()) == 1
	}, time.Second, 10*time.Millisecond)
	require.Eventually(t, func() bool {
		return survivor.CurrentBudget() == 100
	}, 5*time.Second, 10*time.Millisecond)
}
