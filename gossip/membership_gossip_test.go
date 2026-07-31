package gossip

import (
	"context"
	"net"
	"testing"
	"time"

	pb "github.com/sakthivel-p-cse/crdt-rate-limiter/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

type minimalServer struct {
	pb.UnimplementedRateLimiterServer
	nodeID     string
	listenAddr string
	membership *Membership
}

func (s *minimalServer) Ping(_ context.Context, req *pb.PingRequest) (*pb.PingResponse, error) {

	if req.NodeId != "" && req.ListenAddress != "" {
		s.membership.Add(&Member{
			NodeID:      req.NodeId,
			Address:     req.ListenAddress,
			Status:      StatusAlive,
			Incarnation: int(0),
			LastSeen:    time.Now(),
		})
	}

	for _, info := range req.Members {
		if info.NodeId == "" || info.Address == "" {
			continue
		}
		s.membership.Add(&Member{
			NodeID:      info.NodeId,
			Address:     info.Address,
			Status:      Status(info.Status),
			Incarnation: int(info.Incarnation),
			LastSeen:    time.Now(),
		})
	}

	all := s.membership.GetAll()
	resp := make([]*pb.MemberInfo, 0, len(all))
	for _, m := range all {
		m2 := m
		resp = append(resp, &pb.MemberInfo{
			NodeId:      m2.NodeID,
			Address:     m2.Address,
			Status:      int32(m2.Status),
			Incarnation: int32(m2.Incarnation),
		})
	}
	return &pb.PingResponse{Ack: true, Members: resp}, nil
}

func TestTransitiveMembershipDiscovery(t *testing.T) {

	startServer := func(t *testing.T, nodeID string) (*minimalServer, string) {
		t.Helper()
		lis, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)

		srv := &minimalServer{
			nodeID:     nodeID,
			listenAddr: lis.Addr().String(),
			membership: NewMembership(),
		}

		srv.membership.Add(&Member{NodeID: nodeID, Address: lis.Addr().String(), Status: StatusAlive, LastSeen: time.Now()})

		grpcSrv := grpc.NewServer()
		pb.RegisterRateLimiterServer(grpcSrv, srv)
		go func() { _ = grpcSrv.Serve(lis) }()
		t.Cleanup(grpcSrv.Stop)

		return srv, lis.Addr().String()
	}

	srvA, addrA := startServer(t, "A")
	srvB, addrB := startServer(t, "B")
	_, addrC := startServer(t, "C")

	srvA.membership.Add(&Member{NodeID: "B", Address: addrB, Status: StatusAlive, LastSeen: time.Now()})
	srvB.membership.Add(&Member{NodeID: "C", Address: addrC, Status: StatusAlive, LastSeen: time.Now()})

	transportA := NewTransport()
	t.Cleanup(transportA.Close)
	membershipA := srvA.membership

	dissA := NewDisseminator("A", transportA, membershipA, &noopProvider{}, 500*time.Millisecond)
	swimA := NewSWIM(dissA, membershipA, transportA, "A", addrA,
		50*time.Millisecond,
		500*time.Millisecond,
		200*time.Millisecond,
		3,
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	swimA.Start(ctx)

	assert.Eventually(t, func() bool {
		for _, m := range membershipA.GetAll() {
			if m.NodeID == "C" || m.Address == addrC {
				return true
			}
		}
		return false
	}, 2*time.Second, 50*time.Millisecond,
		"node A never discovered node C transitively via B's membership digest")

	ids := make(map[string]bool)
	for _, m := range membershipA.GetAll() {
		ids[m.NodeID] = true
	}
	assert.True(t, ids["B"], "A should know about B (direct seed)")
	assert.True(t, ids["C"], "A should know about C (transitive via B)")
	t.Logf("A's membership after %d gossip rounds: %v", 2000/50, ids)
}

type noopProvider struct{}

func (n *noopProvider) GenerateDelta(_ time.Time) map[string]*pb.ClientState {
	return nil
}
