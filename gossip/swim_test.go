package gossip

import (
	"bytes"
	"context"
	"log/slog"
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

type pingCapture struct {
	pb.UnimplementedRateLimiterServer
	mu     sync.Mutex
	lastID string
}

type indirectProbeCapture struct {
	pb.UnimplementedRateLimiterServer
	transport *Transport
	mu        sync.Mutex
	targetID  string
	targets   map[string]string
}

func (s *indirectProbeCapture) Ping(ctx context.Context, req *pb.PingRequest) (*pb.PingResponse, error) {
	if req.TargetNodeId == "" {
		return &pb.PingResponse{Ack: true}, nil
	}
	s.mu.Lock()
	s.targetID = req.TargetNodeId
	s.mu.Unlock()
	address := s.targets[req.TargetNodeId]
	ack, _, err := s.transport.Ping(ctx, address, "via", "via", nil)
	return &pb.PingResponse{Ack: err == nil && ack}, nil
}

func (s *indirectProbeCapture) TargetID() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.targetID
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
		NodeID:   peerAddr,
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

func TestSWIM_Join_AllFails(t *testing.T) {
	s, _, tport := newTestSWIM(t, "127.0.0.1:65535", "node1")
	defer tport.Close()

	err := s.Join([]string{"127.0.0.1:65535", "127.0.0.1:65534"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to join cluster")
}

func TestSWIM_Leave_ErrorLogged(t *testing.T) {
	var buf bytes.Buffer
	handler := slog.NewTextHandler(&buf, nil)
	logger := slog.New(handler)
	slog.SetDefault(logger)

	s, m, tport := newTestSWIM(t, "127.0.0.1:65535", "node1")
	defer tport.Close()

	m.Add(&Member{
		NodeID:   "peer-1",
		Address:  "127.0.0.1:65535",
		Status:   StatusAlive,
		LastSeen: time.Now(),
	})

	s.Leave()

	assert.Contains(t, buf.String(), "SWIM Leave: failed to notify peer")
}

func TestSWIM_IndirectRecoveryAndFreshDeath(t *testing.T) {
	targetListener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	targetServer := grpc.NewServer()
	pb.RegisterRateLimiterServer(targetServer, &pingCapture{})
	go func() { _ = targetServer.Serve(targetListener) }()

	viaListener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	viaTransport := NewTransport()
	defer viaTransport.Close()
	viaCapture := &indirectProbeCapture{transport: viaTransport, targets: map[string]string{"target": targetListener.Addr().String()}}
	viaServer := grpc.NewServer()
	pb.RegisterRateLimiterServer(viaServer, viaCapture)
	go func() { _ = viaServer.Serve(viaListener) }()
	defer viaServer.Stop()

	membership := NewMembership()
	target := Member{NodeID: "target", Address: targetListener.Addr().String(), Status: StatusAlive, LastSeen: time.Now()}
	via := Member{NodeID: "via", Address: viaListener.Addr().String(), Status: StatusAlive, LastSeen: time.Now()}
	membership.Add(&target)
	membership.Add(&via)
	transport := NewTransport()
	defer transport.Close()
	swim := NewSWIM(nil, membership, transport, "origin", "origin", 10*time.Millisecond, 20*time.Millisecond, 100*time.Millisecond, 1)
	membership.Add(&Member{NodeID: "origin", Address: "origin", Status: StatusAlive, LastSeen: time.Now()})
	swim.mergeMemberDigest([]Member{{NodeID: "origin", Address: "origin", Status: StatusSuspect, Incarnation: 0}})
	self := targetMember(t, membership, "origin")
	assert.Equal(t, StatusAlive, self.Status)
	assert.Equal(t, 1, self.Incarnation)

	membership.MarkSuspect(target.NodeID)
	suspect := membership.GetAll()[0]
	for _, member := range membership.GetAll() {
		if member.NodeID == target.NodeID {
			suspect = member
		}
	}
	swim.indirectPing(context.Background(), suspect)
	assert.Equal(t, target.NodeID, viaCapture.TargetID())
	for _, member := range membership.GetAll() {
		if member.NodeID == target.NodeID {
			assert.Equal(t, StatusAlive, member.Status)
		}
	}

	membership.Update(&Member{NodeID: target.NodeID, Address: target.Address, Status: StatusSuspect, LastSeen: time.Now().Add(-time.Second)})
	swim.checkSuspects(context.Background())
	for _, member := range membership.GetAll() {
		if member.NodeID == target.NodeID {
			assert.Equal(t, StatusAlive, member.Status)
		}
	}

	membership.Update(&Member{NodeID: target.NodeID, Address: target.Address, Status: StatusSuspect, LastSeen: time.Now().Add(-time.Second)})
	stale := targetMember(t, membership, target.NodeID)
	freshTime := time.Now()
	membership.Update(&Member{NodeID: target.NodeID, Address: target.Address, Status: StatusSuspect, LastSeen: freshTime})
	assert.False(t, membership.ConfirmDead(target.NodeID, StatusSuspect, stale.Incarnation, stale.LastSeen))

	targetServer.Stop()
	_ = targetListener.Close()
	membership.Update(&Member{NodeID: target.NodeID, Address: target.Address, Status: StatusSuspect, LastSeen: time.Now().Add(-time.Second)})
	swim.checkSuspects(context.Background())
	assert.Equal(t, StatusDead, targetMember(t, membership, target.NodeID).Status)
}

func targetMember(t *testing.T, membership *Membership, nodeID string) Member {
	t.Helper()
	for _, member := range membership.GetAll() {
		if member.NodeID == nodeID {
			return member
		}
	}
	t.Fatalf("member %s not found", nodeID)
	return Member{}
}
