package gossip

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/sakthivel-p-cse/crdt-rate-limiter/crdt"
	pb "github.com/sakthivel-p-cse/crdt-rate-limiter/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

// mockStateProvider is a StateProvider backed by a simple window map.
// It lives here, local to the test file that uses it.
type mockStateProvider struct {
	windows map[string]*crdt.EpochWindow
}

func (m *mockStateProvider) GenerateDelta(since time.Time) map[string]*pb.ClientState {
	out := make(map[string]*pb.ClientState)
	for clientID, window := range m.windows {
		if cs := window.ToProto(since); cs != nil {
			out[clientID] = cs
		}
	}
	return out
}

// gossipCapture is a minimal gRPC server that records the last GossipState call.
type gossipCapture struct {
	pb.UnimplementedRateLimiterServer
	mu  sync.Mutex
	req *pb.GossipRequest
}

func (s *gossipCapture) GossipState(_ context.Context, req *pb.GossipRequest) (*pb.GossipResponse, error) {
	s.mu.Lock()
	s.req = req
	s.mu.Unlock()
	return &pb.GossipResponse{Success: true}, nil
}

func (s *gossipCapture) last() *pb.GossipRequest {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.req
}

func TestDisseminator_DeltaGenerationAndTick(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := lis.Addr().String()

	capture := &gossipCapture{}
	grpcSrv := grpc.NewServer()
	pb.RegisterRateLimiterServer(grpcSrv, capture)
	go func() { _ = grpcSrv.Serve(lis) }()
	defer grpcSrv.Stop()
	time.Sleep(10 * time.Millisecond)

	transport := NewTransport()
	defer transport.Close()

	membership := NewMembership()
	membership.Add(&Member{
		NodeID:   "peer-1",
		Address:  addr,
		Status:   StatusAlive,
		LastSeen: time.Now(),
	})

	window := crdt.NewEpochWindow(1*time.Second, 60*time.Second)
	window.AllowAndIncrement("test-node", 100, 5, time.Now())

	provider := &mockStateProvider{
		windows: map[string]*crdt.EpochWindow{"client-1": window},
	}

	d := NewDisseminator("test-node", transport, membership, provider, 50*time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	d.Start(ctx)

	assert.Eventually(t, func() bool {
		return capture.last() != nil
	}, 2*time.Second, 20*time.Millisecond, "mock server never received gossip")

	got := capture.last()
	require.NotNil(t, got)
	assert.Equal(t, "test-node", got.SourceNodeId)
	cs, ok := got.State["client-1"]
	assert.True(t, ok)
	assert.NotEmpty(t, cs.Epochs)
}
