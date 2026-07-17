package gossip

import (
	"context"
	"log/slog"
	"sync"
	"time"

	pb "github.com/sakthivel-p-cse/crdt-rate-limiter/proto"
)

// StateProvider is implemented by the node server. It gives the Disseminator
// a read-only view of the local CRDT state without coupling it to the server.
type StateProvider interface {
	// GenerateDelta returns the full proto-encoded state for all clients that
	// have activity since the given time. Pass a zero time to get everything.
	GenerateDelta(since time.Time) map[string]*pb.ClientState
}

// Disseminator runs a background loop that periodically pushes CRDT state to a
// random subset of alive peers (anti-entropy gossip). Every fullSyncEvery ticks
// it sends the entire local state; otherwise it sends only the delta since the
// last send to each peer.
//
// It does not read from or write to the state directly — state access is
// delegated to the StateProvider, keeping the Disseminator decoupled from the
// node server's lock hierarchy.
type Disseminator struct {
	mu       sync.Mutex
	nodeID   string
	provider StateProvider

	membership *Membership
	transport  *Transport

	// lastSentNs maps peer NodeID → Unix nanoseconds of the last successful
	// send. A value of 0 means "never sent" and triggers a full state push.
	lastSentNs map[string]int64

	tickInterval  time.Duration
	fanout        int
	fullSyncEvery int // full sync every N ticks
	deltaCount    int

	wg sync.WaitGroup
}

// NewDisseminator constructs a Disseminator. tickInterval controls how often
// the gossip loop fires; fanout is the number of random peers contacted per
// tick.
func NewDisseminator(
	nodeID string,
	transport *Transport,
	membership *Membership,
	provider StateProvider,
	tickInterval time.Duration,
) *Disseminator {
	return &Disseminator{
		nodeID:        nodeID,
		transport:     transport,
		membership:    membership,
		provider:      provider,
		lastSentNs:    make(map[string]int64),
		tickInterval:  tickInterval,
		fanout:        3,
		fullSyncEvery: 10,
	}
}

// Start launches the gossip background goroutine. It returns immediately; use
// Stop to wait for it to finish after the context is cancelled.
func (d *Disseminator) Start(ctx context.Context) {
	d.wg.Add(1)
	go func() {
		defer d.wg.Done()
		ticker := time.NewTicker(d.tickInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				d.disseminate(ctx)
			}
		}
	}()
}

// Stop waits for all in-flight gossip goroutines to finish.
func (d *Disseminator) Stop() {
	d.wg.Wait()
}

func (d *Disseminator) disseminate(ctx context.Context) {
	// Exclude self so we never gossip to ourselves.
	peers := d.membership.GetRandomKAlive(d.fanout, d.nodeID)
	if len(peers) == 0 {
		return
	}

	slog.Debug("disseminating", "node", d.nodeID, "peers", len(peers))
	d.mu.Lock()
	d.deltaCount++
	sendFull := d.fullSyncEvery > 0 && d.deltaCount >= d.fullSyncEvery
	if sendFull {
		d.deltaCount = 0
	}
	d.mu.Unlock()

	for _, peer := range peers {
		p := peer
		d.wg.Add(1)
		go func(peer *Member) {
			defer d.wg.Done()
			select {
			case <-ctx.Done():
				return
			default:
				d.sendToPeer(ctx, peer, sendFull)
			}
		}(p)
	}
}

func (d *Disseminator) sendToPeer(ctx context.Context, peer *Member, sendFull bool) {
	d.mu.Lock()
	var since time.Time // zero → send everything
	if !sendFull {
		if ns := d.lastSentNs[peer.NodeID]; ns != 0 {
			since = time.Unix(0, ns)
		}
	}
	state := d.provider.GenerateDelta(since)
	slog.Debug("generated delta", "node", d.nodeID, "peer", peer.NodeID, "since", since, "clients", len(state))
	d.lastSentNs[peer.NodeID] = time.Now().UnixNano()
	d.mu.Unlock()

	if len(state) == 0 {
		return
	}

	req := &pb.GossipRequest{
		SourceNodeId: d.nodeID,
		State:        state,
	}
	if _, err := d.transport.SendState(ctx, peer.Address, req); err != nil {
		slog.Debug("gossip send failed", "from", d.nodeID, "to", peer.Address, "err", err)
	}
}
