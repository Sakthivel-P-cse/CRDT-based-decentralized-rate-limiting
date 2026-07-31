package gossip

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	pb "github.com/sakthivel-p-cse/crdt-rate-limiter/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Transport struct {
	mu       sync.Mutex
	conns    map[string]*grpc.ClientConn
	dialOpts []grpc.DialOption
}

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

	target := addr
	if target != "" && target[0] != 'p' && target[0] != 'd' && target[0] != 'h' {
		target = "passthrough:" + strings.Repeat("/", 3) + addr
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

func (t *Transport) Ping(ctx context.Context, addr, nodeID, listenAddr string, digest []Member) (bool, []Member, error) {
	return t.ping(ctx, addr, nodeID, listenAddr, digest, "")
}

func (t *Transport) IndirectPing(ctx context.Context, addr, nodeID, listenAddr string, digest []Member, target Member) (bool, []Member, error) {
	return t.ping(ctx, addr, nodeID, listenAddr, digest, target.NodeID)
}

func (t *Transport) ping(ctx context.Context, addr, nodeID, listenAddr string, digest []Member, targetNodeID string) (bool, []Member, error) {
	client, err := t.getClient(addr)
	if err != nil {
		return false, nil, err
	}

	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	req := &pb.PingRequest{
		NodeId:        nodeID,
		ListenAddress: listenAddr,
		Members:       membersToProto(digest),
		TargetNodeId:  targetNodeID,
	}
	resp, err := client.Ping(ctx, req)
	if err != nil {
		t.removeClient(addr)
		return false, nil, err
	}
	return resp.Ack, membersFromProto(resp.Members), nil
}

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

func (t *Transport) Close() {
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, conn := range t.conns {
		_ = conn.Close()
	}
	t.conns = make(map[string]*grpc.ClientConn)
}

func membersToProto(members []Member) []*pb.MemberInfo {
	if len(members) == 0 {
		return nil
	}
	out := make([]*pb.MemberInfo, len(members))
	for i, m := range members {
		out[i] = &pb.MemberInfo{
			NodeId:      m.NodeID,
			Address:     m.Address,
			Status:      int32(m.Status),
			Incarnation: int32(m.Incarnation),
		}
	}
	return out
}

func membersFromProto(protos []*pb.MemberInfo) []Member {
	if len(protos) == 0 {
		return nil
	}
	out := make([]Member, len(protos))
	for i, p := range protos {
		out[i] = Member{
			NodeID:      p.NodeId,
			Address:     p.Address,
			Status:      Status(p.Status),
			Incarnation: int(p.Incarnation),
		}
	}
	return out
}
