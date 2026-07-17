package node

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"sync"
	"time"

	"github.com/sakthivel-p-cse/crdt-rate-limiter/crdt"
	"github.com/sakthivel-p-cse/crdt-rate-limiter/gossip"
	pb "github.com/sakthivel-p-cse/crdt-rate-limiter/proto"
	"google.golang.org/grpc"
)

// RateLimiterServer is the central orchestrator for a single node in the
// decentralised rate-limiting cluster. It manages:
//   - Per-client sliding-window G-Counter state (EpochWindow).
//   - The SWIM failure-detection protocol.
//   - The gossip disseminator that propagates CRDT state to peers.
//   - A Prometheus metrics HTTP endpoint (optional; skipped when MetricsAddr is "").
type RateLimiterServer struct {
	pb.UnimplementedRateLimiterServer
	config Config

	mu      sync.RWMutex
	windows map[string]*crdt.EpochWindow

	transport    *gossip.Transport
	membership   *gossip.Membership
	disseminator *gossip.Disseminator
	swim         *gossip.SWIM
	grpcServer   *grpc.Server
	metricsServer *MetricsServer
}

// NewRateLimiterServer constructs a server from cfg. Additional grpc.DialOption
// values are forwarded to the transport (useful for injecting bufconn in tests).
func NewRateLimiterServer(cfg Config, dialOpts ...grpc.DialOption) *RateLimiterServer {
	s := &RateLimiterServer{
		config:  cfg,
		windows: make(map[string]*crdt.EpochWindow),
	}

	s.transport = gossip.NewTransport(dialOpts...)
	s.membership = gossip.NewMembership()

	s.disseminator = gossip.NewDisseminator(
		cfg.NodeID,
		s.transport,
		s.membership,
		s, // RateLimiterServer implements gossip.StateProvider
		cfg.GossipInterval,
	)
	s.swim = gossip.NewSWIM(
		s.disseminator,
		s.membership,
		s.transport,
		cfg.NodeID,
		cfg.ListenAddr,
		cfg.GossipInterval,
		cfg.SuspectTimeout,
		cfg.PingTimeout,
		cfg.Fanout,
	)

	if cfg.MetricsAddr != "" {
		s.metricsServer = NewMetricsServer(cfg.NodeID, cfg.MetricsAddr)
	}

	return s
}

// getWindow returns the EpochWindow for clientID, creating it if necessary.
func (s *RateLimiterServer) getWindow(clientID string) *crdt.EpochWindow {
	s.mu.RLock()
	w, ok := s.windows[clientID]
	s.mu.RUnlock()
	if ok {
		return w
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if w, ok = s.windows[clientID]; ok {
		return w
	}
	w = crdt.NewEpochWindow(s.config.EpochSize, s.config.WindowSize)
	s.windows[clientID] = w
	return w
}

// Allow implements pb.RateLimiterServer.
func (s *RateLimiterServer) Allow(ctx context.Context, req *pb.AllowRequest) (*pb.AllowResponse, error) {
	start := time.Now()
	window := s.getWindow(req.ClientId)
	allowed, remaining := window.AllowAndIncrement(s.config.NodeID, s.config.RateLimit, req.Cost, time.Now())
	RecordAllow(s.config.NodeID, allowed, time.Since(start))
	return &pb.AllowResponse{Allowed: allowed, Remaining: remaining}, nil
}

// Ping implements pb.RateLimiterServer. It adds the pinging node to the local
// membership table so gossip can discover peers via the join protocol.
func (s *RateLimiterServer) Ping(_ context.Context, req *pb.PingRequest) (*pb.PingResponse, error) {
	if req.NodeId != "" && req.ListenAddress != "" {
		s.membership.Add(&gossip.Member{
			NodeID:      req.NodeId,
			Address:     req.ListenAddress,
			Status:      gossip.StatusAlive,
			LastSeen:    time.Now(),
		})
	}
	return &pb.PingResponse{Ack: true}, nil
}

// GenerateDelta implements gossip.StateProvider. It serialises all client windows
// that have epoch data newer than since (pass zero to get everything).
func (s *RateLimiterServer) GenerateDelta(since time.Time) map[string]*pb.ClientState {
	s.mu.RLock()
	defer s.mu.RUnlock()

	out := make(map[string]*pb.ClientState, len(s.windows))
	for clientID, window := range s.windows {
		if cs := window.ToProto(since); cs != nil {
			out[clientID] = cs
		}
	}
	return out
}

// GossipState implements pb.RateLimiterServer. It merges incoming CRDT state
// from a peer into the local window store.
func (s *RateLimiterServer) GossipState(_ context.Context, req *pb.GossipRequest) (*pb.GossipResponse, error) {
	start := time.Now()
	for clientID, cs := range req.State {
		slog.Info("GossipState RECV", "node", s.config.NodeID, "from", req.SourceNodeId, "client", clientID, "epochs", len(cs.Epochs))
		incoming := crdt.NewEpochWindow(s.config.EpochSize, s.config.WindowSize)
		incoming.MergeFromProto(cs)

		s.mu.Lock()
		if existing, ok := s.windows[clientID]; ok {
			existing.Merge(incoming)
		} else {
			s.windows[clientID] = incoming
		}
		s.mu.Unlock()
	}
	RecordGossipRound(s.config.NodeID, time.Since(start))
	return &pb.GossipResponse{Success: true}, nil
}

// Start validates the configuration and launches all background subsystems.
// Peers in cfg.Peers are added to the membership table immediately; the SWIM
// join protocol will then exchange membership information with them.
func (s *RateLimiterServer) Start(ctx context.Context) error {
	if err := s.config.Validate(); err != nil {
		return fmt.Errorf("invalid configuration: %w", err)
	}

	// Seed membership with known peers and self.
	for _, peerAddr := range s.config.Peers {
		s.membership.Add(&gossip.Member{
			NodeID:   peerAddr,
			Address:  peerAddr,
			Status:   gossip.StatusAlive,
			LastSeen: time.Now(),
		})
	}
	s.membership.Add(&gossip.Member{
		NodeID:   s.config.NodeID,
		Address:  s.config.ListenAddr,
		Status:   gossip.StatusAlive,
		LastSeen: time.Now(),
	})

	s.swim.Start(ctx)
	s.disseminator.Start(ctx)

	if s.metricsServer != nil {
		if err := s.metricsServer.Start(); err != nil {
			return fmt.Errorf("metrics server: %w", err)
		}
	}

	if err := s.swim.Join(s.config.Peers); err != nil {
		slog.Warn("cluster join encountered errors", "node", s.config.NodeID, "err", err)
	}

	return nil
}

// Stop performs a graceful shutdown: announces departure to peers, closes
// transport connections, stops the gRPC server, and shuts down metrics.
func (s *RateLimiterServer) Stop() {
	s.swim.Leave()
	s.transport.Close()
	if s.grpcServer != nil {
		s.grpcServer.GracefulStop()
	}
	if s.metricsServer != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := s.metricsServer.Stop(ctx); err != nil {
			log.Printf("metrics server shutdown: %v", err)
		}
	}
}

// SetGRPCServer registers the gRPC server so Stop can call GracefulStop on it.
func (s *RateLimiterServer) SetGRPCServer(grpcServer *grpc.Server) {
	s.grpcServer = grpcServer
}

// Windows returns a snapshot of all current EpochWindow references.
// Intended for testing and introspection; callers must not write to the map.
func (s *RateLimiterServer) Windows() map[string]*crdt.EpochWindow {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make(map[string]*crdt.EpochWindow, len(s.windows))
	for k, v := range s.windows {
		out[k] = v
	}
	return out
}
