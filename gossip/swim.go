package gossip

import (
	"context"
	"log/slog"
	"sync"
	"time"
)

// SWIM (Scalable Weakly-consistent Infection-style Membership) implements the
// failure-detection and membership protocol.
//
// Each tick it selects up to k random alive peers (excluding itself), pings
// each directly, and falls back to indirect pings via other peers if the direct
// ping fails. Members that remain suspect for longer than suspectTimeout are
// declared dead.
type SWIM struct {
	disseminator   *Disseminator
	membership     *Membership
	transport      *Transport
	tickInterval   time.Duration
	suspectTimeout time.Duration
	pingTimeout    time.Duration
	k              int
	nodeID         string
	listenAddr     string
	wg             sync.WaitGroup
}

// NewSWIM constructs a SWIM failure detector.
func NewSWIM(
	disseminator *Disseminator,
	membership *Membership,
	transport *Transport,
	nodeID string,
	listenAddr string,
	tickInterval time.Duration,
	suspectTimeout time.Duration,
	pingTimeout time.Duration,
	k int,
) *SWIM {
	return &SWIM{
		disseminator:   disseminator,
		membership:     membership,
		transport:      transport,
		tickInterval:   tickInterval,
		suspectTimeout: suspectTimeout,
		pingTimeout:    pingTimeout,
		k:              k,
		nodeID:         nodeID,
		listenAddr:     listenAddr,
	}
}

// Start launches the SWIM background goroutine.
func (s *SWIM) Start(ctx context.Context) {
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		ticker := time.NewTicker(s.tickInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				s.tick(ctx)
			}
		}
	}()
}

// Stop waits for all in-flight SWIM goroutines to finish.
func (s *SWIM) Stop() {
	s.wg.Wait()
}

func (s *SWIM) tick(ctx context.Context) {
	s.checkSuspects()

	// Exclude self so we never ping ourselves.
	peers := s.membership.GetRandomKAlive(s.k, s.nodeID)
	for _, peer := range peers {
		p := peer
		s.wg.Add(1)
		go func(peer *Member) {
			defer s.wg.Done()
			select {
			case <-ctx.Done():
				return
			default:
				s.pingPeer(ctx, peer)
			}
		}(p)
	}
}

func (s *SWIM) pingPeer(ctx context.Context, peer *Member) {
	pingCtx, cancel := context.WithTimeout(ctx, s.pingTimeout)
	defer cancel()

	ack, err := s.transport.Ping(pingCtx, peer.Address, s.nodeID, s.listenAddr)
	if err != nil || !ack {
		slog.Debug("direct ping failed, trying indirect", "from", s.nodeID, "to", peer.Address, "err", err)
		s.indirectPing(ctx, peer)
		return
	}

	s.membership.Update(&Member{
		NodeID:      peer.NodeID,
		Address:     peer.Address,
		Incarnation: peer.Incarnation,
		Status:      StatusAlive,
		LastSeen:    time.Now(),
	})
}

func (s *SWIM) indirectPing(ctx context.Context, target *Member) {
	intermediaries := s.membership.GetRandomKAlive(s.k, s.nodeID, target.NodeID)
	for _, via := range intermediaries {
		pingCtx, cancel := context.WithTimeout(ctx, s.pingTimeout)
		ack, err := s.transport.Ping(pingCtx, via.Address, s.nodeID, s.listenAddr)
		cancel()
		if err == nil && ack {
			s.membership.Update(&Member{
				NodeID:      target.NodeID,
				Address:     target.Address,
				Incarnation: target.Incarnation,
				Status:      StatusAlive,
				LastSeen:    time.Now(),
			})
			return
		}
	}
	s.membership.MarkSuspect(target.NodeID)
}

func (s *SWIM) checkSuspects() {
	for _, member := range s.membership.GetAll() {
		if member.Status == StatusSuspect && time.Since(member.LastSeen) > s.suspectTimeout {
			s.membership.ConfirmDead(member.NodeID)
		}
	}
}

// Join contacts each seed address, announcing this node's presence.
// It is best-effort: if all seeds are unreachable the node starts isolated.
func (s *SWIM) Join(seeds []string) error {
	for _, addr := range seeds {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_, _ = s.transport.Ping(ctx, addr, s.nodeID, s.listenAddr)
		cancel()
	}
	return nil
}

// Leave announces departure to all alive peers before shutting down.
func (s *SWIM) Leave() {
	var wg sync.WaitGroup
	for _, peer := range s.membership.GetAlive() {
		wg.Add(1)
		go func(p *Member) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			_, _ = s.transport.Ping(ctx, p.Address, s.nodeID, s.listenAddr)
		}(peer)
	}
	wg.Wait()
	s.membership.Remove(s.nodeID)
}