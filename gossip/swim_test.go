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

// pingCapture records the node ID from the most recent Ping call.
type pingCapture struct {
	pb.UnimplementedRateLimiterServer
	mu     sync.Mutex
	lastID string
}

func (s *pingCapture) Ping(_ context.Context, req *pb.PingRequest) (*pb.PingResponse, error) {
	s.mu.Lock()
	s.lastID = req.NodeId
	s.mu.Unlock()
	return &pb.PingResponse{Ack: true}, nil
}

func (s *pingCapture) LastPingNode() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastID
}

func newTestSWIM(t *testing.T, peerAddr, nodeID string) (*SWIM, *Membership, *Transport) {
	t.Helper()
	transport := NewTransport()
	t.Cleanup(func() { transport.Close() })

	membership := NewMembership()
	membership.Add(&Member{
		NodeID:   peerAddr, // use addr as ID in unit tests
		Address:  peerAddr,
		Status:   StatusAlive,
		LastSeen: time.Now(),
	})

	provider := &mockStateProvider{
		windows: map[string]*crdt.EpochWindow{},
	}
	disseminator := NewDisseminator(nodeID, transport, membership, provider, 50*time.Millisecond)

	swim := NewSWIM(
		disseminator, membership, transport,
		nodeID, "127.0.0.1:0",
		50*time.Millisecond,
		200*time.Millisecond,
		100*time.Millisecond,
		3,
	)
	return swim, membership, transport
}

func TestSWIM_LifecycleAndTicks(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := lis.Addr().String()

	capture := &pingCapture{}
	grpcSrv := grpc.NewServer()
	pb.RegisterRateLimiterServer(grpcSrv, capture)
	go func() { _ = grpcSrv.Serve(lis) }()
	defer grpcSrv.Stop()
	time.Sleep(10 * time.Millisecond)

	swim, membership, _ := newTestSWIM(t, addr, "test-node")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	swim.Start(ctx)

	time.Sleep(150 * time.Millisecond)
	assert.Equal(t, "test-node", capture.LastPingNode(), "SWIM should have pinged peer")

	alive := membership.GetAlive()
	assert.Len(t, alive, 1)

	// Simulate failure by stopping the gRPC server.
	grpcSrv.Stop()
	time.Sleep(500 * time.Millisecond)

	all := membership.GetAll()
	require.Len(t, all, 1)
	assert.Equal(t, StatusDead, all[0].Status, "peer should be confirmed dead after suspect timeout")
}

func TestSWIM_JoinAndLeave(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := lis.Addr().String()

	capture := &pingCapture{}
	grpcSrv := grpc.NewServer()
	pb.RegisterRateLimiterServer(grpcSrv, capture)
	go func() { _ = grpcSrv.Serve(lis) }()
	defer grpcSrv.Stop()
	time.Sleep(10 * time.Millisecond)

	transport := NewTransport()
	defer transport.Close()

	membership := NewMembership()
	membership.Add(&Member{NodeID: "test-node", Address: "127.0.0.1:9999", Status: StatusAlive, LastSeen: time.Now()})
	membership.Add(&Member{NodeID: "peer-1", Address: addr, Status: StatusAlive, LastSeen: time.Now()})

	provider := &mockStateProvider{windows: map[string]*crdt.EpochWindow{}}
	disseminator := NewDisseminator("test-node", transport, membership, provider, 50*time.Millisecond)

	swim := NewSWIM(
		disseminator, membership, transport,
		"test-node", "localhost:50051",
		50*time.Millisecond, 200*time.Millisecond, 100*time.Millisecond, 3,
	)

	require.NoError(t, swim.Join([]string{addr}))
	assert.Equal(t, "test-node", capture.LastPingNode())

	swim.Leave()

	all := membership.GetAll()
	assert.Len(t, all, 1)
	assert.Equal(t, "peer-1", all[0].NodeID)
}
