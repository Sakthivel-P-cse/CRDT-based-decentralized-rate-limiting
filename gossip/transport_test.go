package gossip

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	pb "github.com/sakthivel-p-cse/crdt-rate-limiter/proto"
	"google.golang.org/grpc"
)

// MockServer for testing transport
type MockServer struct {
	pb.UnimplementedRateLimiterServer
	mu           sync.Mutex
	lastPingNode string
}

func (s *MockServer) Ping(ctx context.Context, req *pb.PingRequest) (*pb.PingResponse, error) {
	s.mu.Lock()
	s.lastPingNode = req.NodeId
	s.mu.Unlock()
	return &pb.PingResponse{Ack: true}, nil
}

func (s *MockServer) LastPingNode() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastPingNode
}

func (s *MockServer) GossipState(ctx context.Context, req *pb.GossipRequest) (*pb.GossipResponse, error) {
	return &pb.GossipResponse{Success: true}, nil
}

func TestTransport_PingAndGossip(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	addr := lis.Addr().String()

	grpcServer := grpc.NewServer()
	mockSrv := &MockServer{}
	pb.RegisterRateLimiterServer(grpcServer, mockSrv)

	go func() {
		_ = grpcServer.Serve(lis)
	}()
	defer grpcServer.Stop()

	// Give it a moment to start
	time.Sleep(10 * time.Millisecond)

	transport := NewTransport()
	defer transport.Close()

	ctx := context.Background()

	// Test Ping
	ack, err := transport.Ping(ctx, addr, "test-node", "test-listen-addr")
	if err != nil {
		t.Fatalf("ping failed: %v", err)
	}
	if !ack {
		t.Fatal("expected ack=true")
	}
	if mockSrv.LastPingNode() != "test-node" {
		t.Fatalf("expected ping from test-node, got %s", mockSrv.LastPingNode())
	}

	// Test GossipState
	success, err := transport.SendState(ctx, addr, &pb.GossipRequest{
		SourceNodeId: "test-node",
	})
	if err != nil {
		t.Fatalf("send state failed: %v", err)
	}
	if !success {
		t.Fatal("expected success=true")
	}
}