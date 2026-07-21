package gossip

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/sakthivel-p-cse/crdt-rate-limiter/util"
)

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

func (s *SWIM) Start(ctx context.Context) {
	s.wg.Add(1)
	go util.SafeGo("swim-supervisor", func() {
		defer s.wg.Done()
		util.SafeGoLoop("swim-tick-loop", ctx, func() {
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
		})
	})
}

func (s *SWIM) Stop() {
	s.wg.Wait()
}

func (s *SWIM) tick(ctx context.Context) {
	s.checkSuspects(ctx)

	peers := s.membership.GetRandomKAlive(s.k, s.nodeID)
	for _, peer := range peers {
		p := peer
		s.wg.Add(1)
		go util.SafeGo("swim-ping-peer", func() {
			defer s.wg.Done()
			select {
			case <-ctx.Done():
				return
			default:
				s.pingPeer(ctx, p)
			}
		})
	}
}

func (s *SWIM) membershipDigest() []Member {
	all := s.membership.GetAll()
	max := 2 * s.k
	if len(all) <= max {
		return all
	}
	return all[:max]
}

func (s *SWIM) mergeMemberDigest(digest []Member) {
	for _, m := range digest {
		if m.NodeID == "" || m.Address == "" {
			continue
		}
		if m.NodeID == s.nodeID {
			if m.Status != StatusAlive {
				s.membership.Refute(s.nodeID, m.Incarnation)
			}
			continue
		}
		m2 := m
		s.membership.Add(&m2)
	}
}

func (s *SWIM) pingPeer(ctx context.Context, peer Member) {
	pingCtx, cancel := context.WithTimeout(ctx, s.pingTimeout)
	defer cancel()

	digest := s.membershipDigest()
	ack, respDigest, err := s.transport.Ping(pingCtx, peer.Address, s.nodeID, s.listenAddr, digest)
	if err != nil || !ack {
		slog.Debug("direct ping failed, trying indirect", "from", s.nodeID, "to", peer.Address, "err", err)
		s.indirectPing(ctx, peer)
		return
	}

	s.mergeMemberDigest(respDigest)

	s.membership.Update(&Member{
		NodeID:      peer.NodeID,
		Address:     peer.Address,
		Incarnation: peer.Incarnation,
		Status:      StatusAlive,
		LastSeen:    time.Now(),
	})
}

func (s *SWIM) indirectPing(ctx context.Context, target Member) {
	intermediaries := s.membership.GetRandomKAlive(s.k, s.nodeID, target.NodeID)
	for _, via := range intermediaries {
		pingCtx, cancel := context.WithTimeout(ctx, s.pingTimeout)
		digest := s.membershipDigest()
		ack, respDigest, err := s.transport.IndirectPing(pingCtx, via.Address, s.nodeID, s.listenAddr, digest, target)
		cancel()
		if err == nil && ack {
			s.mergeMemberDigest(respDigest)
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

func (s *SWIM) checkSuspects(ctx context.Context) {
	for _, member := range s.membership.GetAll() {
		if member.Status == StatusSuspect && time.Since(member.LastSeen) > s.suspectTimeout {
			pingCtx, cancel := context.WithTimeout(ctx, s.pingTimeout)
			ack, respDigest, err := s.transport.Ping(pingCtx, member.Address, s.nodeID, s.listenAddr, s.membershipDigest())
			cancel()
			if err == nil && ack {
				s.mergeMemberDigest(respDigest)
				s.membership.Update(&Member{
					NodeID:      member.NodeID,
					Address:     member.Address,
					Incarnation: member.Incarnation,
					Status:      StatusAlive,
					LastSeen:    time.Now(),
				})
				continue
			}
			s.membership.ConfirmDead(member.NodeID, StatusSuspect, member.Incarnation, member.LastSeen)
		}
	}
}

func (s *SWIM) Join(seeds []string) error {
	var successes int
	for _, addr := range seeds {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		digest := s.membershipDigest()
		_, respDigest, err := s.transport.Ping(ctx, addr, s.nodeID, s.listenAddr, digest)
		cancel()
		if err != nil {
			slog.Warn("SWIM Join: failed to ping seed", "seed", addr, "error", err)
			continue
		}
		s.mergeMemberDigest(respDigest)
		successes++
	}
	if len(seeds) > 0 && successes == 0 {
		return fmt.Errorf("failed to join cluster: all %d seed nodes unreachable", len(seeds))
	}
	return nil
}

func (s *SWIM) Leave() {
	var wg sync.WaitGroup
	for _, peer := range s.membership.GetAlive() {
		p := peer
		if p.NodeID == s.nodeID {
			continue
		}
		wg.Add(1)
		go util.SafeGo("swim-leave-peer", func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			if _, _, err := s.transport.Ping(ctx, p.Address, s.nodeID, s.listenAddr, nil); err != nil {
				slog.Warn("SWIM Leave: failed to notify peer", "peer", p.NodeID, "address", p.Address, "error", err)
			}
		})
	}
	wg.Wait()
	s.membership.Remove(s.nodeID)
}
