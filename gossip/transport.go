package gossip

import (
	"context"
	"fmt"
	"sync"
	"time"

	pb "github.com/sakthivel-p-cse/crdt-rate-limiter/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Transport manages a pool of gRPC client connections to peer nodes.
// Connections are created lazily and reused across calls. Failed connections
// are evicted so the next call triggers a fresh dial.
//
// All methods are safe for concurrent use.
type Transport struct {
	mu       sync.Mutex
	conns    map[string]*grpc.ClientConn
	dialOpts []grpc.DialOption
}

// NewTransport creates a Transport. Any additional grpc.DialOption values
// (e.g. a bufconn dialer for testing) are applied to every new connection.
func NewTransport(opts ...grpc.DialOption) *Transport {
	return &Transport{
		conns:    make(map[string]*grpc.ClientConn),
		dialOpts: opts,
	}
}

func (t *Transport) getClient(addr string) (pb.RateLimiterClient, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if conn, ok := t.conns[addr]; ok {
		return pb.NewRateLimiterClient(conn), nil
	}

	opts := make([]grpc.DialOption, 0, 1+len(t.dialOpts))
	opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	opts = append(opts, t.dialOpts...)

	//nolint:staticcheck // grpc.Dial is deprecated in newer versions; replace with grpc.NewClient once the project upgrades past Go 1.22.
	target := addr
	if target != "" && target[0] != 'p' && target[0] != 'd' && target[0] != 'h' { // naive check for scheme
		target = "passthrough:///" + addr
	}
	conn, err := grpc.NewClient(target, opts...)
	if err != nil {
		return nil, fmt.Errorf("transport: dial %s: %w", addr, err)
	}
	t.conns[addr] = conn
	return pb.NewRateLimiterClient(conn), nil
}

func (t *Transport) removeClient(addr string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if conn, ok := t.conns[addr]; ok {
		_ = conn.Close()
		delete(t.conns, addr)
	}
}

// Ping sends a liveness ping to addr, advertising this node as nodeID with the
// given listenAddr so the receiver can add us to its membership table.
func (t *Transport) Ping(ctx context.Context, addr, nodeID, listenAddr string) (bool, error) {
	client, err := t.getClient(addr)
	if err != nil {
		return false, err
	}

	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	resp, err := client.Ping(ctx, &pb.PingRequest{NodeId: nodeID, ListenAddress: listenAddr})
	if err != nil {
		t.removeClient(addr)
		return false, err
	}
	return resp.Ack, nil
}

// SendState delivers a gossip payload to addr.
func (t *Transport) SendState(ctx context.Context, addr string, req *pb.GossipRequest) (bool, error) {
	client, err := t.getClient(addr)
	if err != nil {
		return false, err
	}

	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	resp, err := client.GossipState(ctx, req)
	if err != nil {
		t.removeClient(addr)
		return false, err
	}
	return resp.Success, nil
}

// Close closes all pooled connections.
func (t *Transport) Close() {
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, conn := range t.conns {
		_ = conn.Close()
	}
	t.conns = make(map[string]*grpc.ClientConn)
}