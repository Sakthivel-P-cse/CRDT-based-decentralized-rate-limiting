package node

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sakthivel-p-cse/crdt-rate-limiter/crdt"
	"github.com/sakthivel-p-cse/crdt-rate-limiter/gossip"
	pb "github.com/sakthivel-p-cse/crdt-rate-limiter/proto"
	"github.com/sakthivel-p-cse/crdt-rate-limiter/util"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
)

type RateLimiterServer struct {
	pb.UnimplementedRateLimiterServer
	config Config

	mu      sync.RWMutex
	windows map[string]*crdt.EpochWindow
	shedder TierShedder
	budget  *AdaptiveBudget

	contributedToOvershoot atomic.Bool
	utilizationMu          sync.RWMutex
	clientUtilization      map[string]utilizationState
	utilizationChanged     chan struct{}
	epochWG                sync.WaitGroup

	transport       *gossip.Transport
	membership      *gossip.Membership
	disseminator    *gossip.Disseminator
	swim            *gossip.SWIM
	grpcServer      *grpc.Server
	metricsServer   *MetricsServer
	lifecycleMu     sync.Mutex
	lifecycleCancel context.CancelFunc
	started         bool
	stopped         bool
}

type utilizationState struct {
	ratio   float64
	version uint64
}

func NewRateLimiterServer(cfg Config, dialOpts ...grpc.DialOption) *RateLimiterServer {
	if cfg.SoftThresholdRatio == 0 {
		cfg.SoftThresholdRatio = 0.70
	}
	s := &RateLimiterServer{
		config:             cfg,
		windows:            make(map[string]*crdt.EpochWindow),
		budget:             NewAdaptiveBudget(cfg.RateLimit),
		clientUtilization:  make(map[string]utilizationState),
		utilizationChanged: make(chan struct{}, 1),
	}

	s.transport = gossip.NewTransport(dialOpts...)
	s.membership = gossip.NewMembership()

	s.disseminator = gossip.NewDisseminator(
		cfg.NodeID,
		s.transport,
		s.membership,
		s,
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
	SetActiveClients(s.config.NodeID, len(s.windows))
	return w
}

func (s *RateLimiterServer) Allow(ctx context.Context, req *pb.AllowRequest) (*pb.AllowResponse, error) {
	if req == nil || req.Cost <= 0 {
		return nil, status.Error(codes.InvalidArgument, "cost must be greater than zero")
	}
	start := time.Now()
	window := s.getWindow(req.ClientId)
	now := time.Now()
	limit := s.budget.Current()
	currentCount := window.Total(now)
	utilizationRatio := ratio(currentCount, limit)
	s.raiseClientUtilization(req.ClientId, utilizationRatio)
	priority := priorityFromProto(req.Priority)

	if s.shedder.ShouldShed(priority, utilizationRatio) {
		RecordAllow(s.config.NodeID, false, time.Since(start))
		s.recordClientOvershootMetric(req.ClientId, window)
		return s.rejectedResponse(limit-currentCount, "priority_tier_shed"), nil
	}

	softThreshold := int64(float64(limit) * s.config.SoftThresholdRatio)
	if rand.Float64() < RejectProbability(currentCount, softThreshold, limit) {
		RecordAllow(s.config.NodeID, false, time.Since(start))
		s.recordClientOvershootMetric(req.ClientId, window)
		return s.rejectedResponse(limit-currentCount, "soft_shed_probabilistic"), nil
	}

	allowed, remaining := window.AllowAndIncrement(s.config.NodeID, limit, req.Cost, now)
	RecordAllow(s.config.NodeID, allowed, time.Since(start))
	s.recordClientOvershootMetric(req.ClientId, window)
	if !allowed {
		s.raiseClientUtilization(req.ClientId, ratio(limit-remaining, limit))
		return s.rejectedResponse(remaining, "limit_exceeded"), nil
	}
	s.raiseClientUtilization(req.ClientId, ratio(limit-remaining, limit))
	return &pb.AllowResponse{Allowed: true, Remaining: remaining}, nil
}

func priorityFromProto(priority pb.Priority) Priority {
	switch priority {
	case pb.Priority_PRIORITY_CRITICAL:
		return PriorityCritical
	case pb.Priority_PRIORITY_BEST_EFFORT:
		return PriorityBestEffort
	default:
		return PriorityStandard
	}
}

func ratio(currentCount, limit int64) float64 {
	if limit <= 0 {
		return 1
	}
	return float64(currentCount) / float64(limit)
}

func (s *RateLimiterServer) rejectedResponse(remaining int64, reason string) *pb.AllowResponse {
	interval := gossip.GossipInterval(s.config.GossipInterval, s.UtilizationRatio())
	retryAfter := EstimatedRetryAfter(interval, gossip.ConvergenceRounds)
	return &pb.AllowResponse{
		Allowed:    false,
		Remaining:  remaining,
		Reason:     reason,
		RetryAfter: durationpb.New(retryAfter),
	}
}

func (s *RateLimiterServer) Ping(ctx context.Context, req *pb.PingRequest) (*pb.PingResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "ping request is required")
	}
	ack := req.TargetNodeId == ""
	var probeTarget *gossip.Member
	if req.TargetNodeId == s.config.NodeID {
		ack = true
	} else if req.TargetNodeId != "" {
		for _, member := range s.membership.GetAll() {
			if member.NodeID == req.TargetNodeId && member.Status != gossip.StatusDead {
				memberCopy := member
				probeTarget = &memberCopy
				break
			}
		}
	}

	if req.NodeId != "" && req.ListenAddress != "" {
		s.membership.Add(&gossip.Member{
			NodeID:   req.NodeId,
			Address:  req.ListenAddress,
			Status:   gossip.StatusAlive,
			LastSeen: time.Now(),
		})
	}

	for _, info := range req.Members {
		if info.NodeId == "" || info.Address == "" {
			continue
		}
		if info.NodeId == s.config.NodeID {
			if gossip.Status(info.Status) != gossip.StatusAlive {
				s.membership.Refute(s.config.NodeID, int(info.Incarnation))
			}
			continue
		}
		s.membership.Add(&gossip.Member{
			NodeID:      info.NodeId,
			Address:     info.Address,
			Status:      gossip.Status(info.Status),
			Incarnation: int(info.Incarnation),
			LastSeen:    time.Now(),
		})
	}

	if probeTarget != nil {
		targetAck, targetDigest, err := s.transport.Ping(ctx, probeTarget.Address, s.config.NodeID, s.config.ListenAddr, s.membership.GetAll())
		ack = err == nil && targetAck
		if ack {
			for _, member := range targetDigest {
				memberCopy := member
				s.membership.Add(&memberCopy)
			}
		}
	}

	all := s.membership.GetAll()
	max := s.config.Fanout * 2
	if max == 0 {
		max = 6
	}
	if len(all) > max {
		all = all[:max]
	}
	respMembers := make([]*pb.MemberInfo, len(all))
	for i, m := range all {
		m2 := m
		respMembers[i] = &pb.MemberInfo{
			NodeId:      m2.NodeID,
			Address:     m2.Address,
			Status:      int32(m2.Status),
			Incarnation: int32(m2.Incarnation),
		}
	}
	return &pb.PingResponse{Ack: ack, Members: respMembers}, nil
}

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

func (s *RateLimiterServer) GossipState(_ context.Context, req *pb.GossipRequest) (*pb.GossipResponse, error) {
	start := time.Now()
	for clientID, cs := range req.State {
		slog.Info("GossipState RECV", "node", s.config.NodeID, "from", req.SourceNodeId, "client", clientID, "epochs", len(cs.Epochs))
		incoming := crdt.NewEpochWindow(s.config.EpochSize, s.config.WindowSize)
		incoming.MergeFromProto(cs)

		var existing *crdt.EpochWindow
		s.mu.Lock()
		if e, ok := s.windows[clientID]; ok {
			existing = e
		} else {
			s.windows[clientID] = incoming
			SetActiveClients(s.config.NodeID, len(s.windows))
		}
		s.mu.Unlock()

		if existing != nil {
			existing.Merge(incoming)
		}
	}

	elapsed := time.Since(start)

	RecordPayloadProcessing(s.config.NodeID, elapsed)

	s.recordOvershootMetric()

	return &pb.GossipResponse{Success: true}, nil
}

func (s *RateLimiterServer) recordOvershootMetric() {
	s.mu.RLock()
	defer s.mu.RUnlock()
	contributed := false
	limit := s.budget.Current()
	for clientID, window := range s.windows {
		total, clientContributed := s.recordClientOvershootMetric(clientID, window)
		contributed = contributed || clientContributed
		s.raiseClientUtilization(clientID, ratio(total, limit))
	}
	if contributed {
		s.contributedToOvershoot.Store(true)
	}
}

func (s *RateLimiterServer) recordClientOvershootMetric(clientID string, window *crdt.EpochWindow) (int64, bool) {
	now := time.Now()
	total := window.Total(now)
	if s.config.RateLimit > 0 && total > s.config.RateLimit {
		overshootRatio := float64(total-s.config.RateLimit) / float64(s.config.RateLimit)
		SetOvershoot(s.config.NodeID, clientID, overshootRatio)
		return total, window.ContributedInPreviousEpoch(s.config.NodeID, now)
	}
	SetOvershoot(s.config.NodeID, clientID, 0)
	return total, false
}

func (s *RateLimiterServer) Start(ctx context.Context) error {
	if err := s.config.Validate(); err != nil {
		return fmt.Errorf("invalid configuration: %w", err)
	}
	s.lifecycleMu.Lock()
	if s.started {
		s.lifecycleMu.Unlock()
		return fmt.Errorf("server already started")
	}
	if s.stopped {
		s.lifecycleMu.Unlock()
		return fmt.Errorf("server cannot be restarted after stop")
	}
	if s.metricsServer != nil {
		if err := s.metricsServer.Start(); err != nil {
			s.lifecycleMu.Unlock()
			return fmt.Errorf("metrics server: %w", err)
		}
	}

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

	runCtx, cancel := context.WithCancel(ctx)
	s.lifecycleCancel = cancel
	s.started = true
	s.swim.Start(runCtx)
	s.disseminator.Start(runCtx)
	s.startEpochLoop(runCtx)

	if err := s.swim.Join(s.config.Peers); err != nil {
		slog.Warn("cluster join encountered errors", "node", s.config.NodeID, "err", err)
	}

	s.lifecycleMu.Unlock()
	return nil
}

func (s *RateLimiterServer) Stop() {
	s.lifecycleMu.Lock()
	if !s.started {
		s.stopped = true
		s.lifecycleMu.Unlock()
		return
	}
	cancel := s.lifecycleCancel
	s.lifecycleCancel = nil
	if cancel != nil {
		cancel()
	}
	s.swim.Stop()
	s.disseminator.Stop()
	s.epochWG.Wait()
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
	s.started = false
	s.stopped = true
	s.lifecycleMu.Unlock()
}

func (s *RateLimiterServer) SetGRPCServer(grpcServer *grpc.Server) {
	s.grpcServer = grpcServer
}

func (s *RateLimiterServer) Windows() map[string]*crdt.EpochWindow {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make(map[string]*crdt.EpochWindow, len(s.windows))
	for k, v := range s.windows {
		out[k] = v
	}
	return out
}

func (s *RateLimiterServer) startEpochLoop(ctx context.Context) {
	s.epochWG.Add(1)
	go util.SafeGo("adaptive-budget-epoch-loop", func() {
		defer s.epochWG.Done()
		untilNextEpoch := s.config.EpochSize - time.Duration(time.Now().UnixNano()%int64(s.config.EpochSize))
		timer := time.NewTimer(untilNextEpoch)
		defer timer.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-timer.C:
				s.budget.OnEpochComplete(s.contributedToOvershoot.Swap(false))
				s.refreshUtilizationRatios()
				timer.Reset(s.config.EpochSize)
			}
		}
	})
}

func (s *RateLimiterServer) raiseClientUtilization(clientID string, utilizationRatio float64) {
	s.utilizationMu.Lock()
	state := s.clientUtilization[clientID]
	state.version++
	changed := utilizationRatio > state.ratio
	if changed {
		state.ratio = utilizationRatio
	}
	s.clientUtilization[clientID] = state
	s.utilizationMu.Unlock()
	if changed {
		select {
		case s.utilizationChanged <- struct{}{}:
		default:
		}
	}
}

func (s *RateLimiterServer) UtilizationRatio() float64 {
	s.utilizationMu.RLock()
	defer s.utilizationMu.RUnlock()
	var maximum float64
	for _, state := range s.clientUtilization {
		maximum = max(maximum, state.ratio)
	}
	return maximum
}

func (s *RateLimiterServer) UtilizationChanges() <-chan struct{} {
	return s.utilizationChanged
}

func (s *RateLimiterServer) refreshUtilizationRatios() {
	s.mu.RLock()
	defer s.mu.RUnlock()
	limit := s.budget.Current()
	now := time.Now()
	for clientID, window := range s.windows {
		s.utilizationMu.RLock()
		version := s.clientUtilization[clientID].version
		s.utilizationMu.RUnlock()
		utilizationRatio := ratio(window.Total(now), limit)
		s.utilizationMu.Lock()
		state := s.clientUtilization[clientID]
		changed := false
		if state.version == version {
			changed = state.ratio != utilizationRatio
			state.ratio = utilizationRatio
			s.clientUtilization[clientID] = state
		}
		s.utilizationMu.Unlock()
		if changed {
			select {
			case s.utilizationChanged <- struct{}{}:
			default:
			}
		}
	}
}

func (s *RateLimiterServer) CurrentBudget() int64 {
	return s.budget.Current()
}
