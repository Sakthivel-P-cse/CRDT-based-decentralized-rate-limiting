package gossip

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	pb "github.com/sakthivel-p-cse/crdt-rate-limiter/proto"
	"github.com/sakthivel-p-cse/crdt-rate-limiter/util"
)

type StateProvider interface {
	GenerateDelta(since time.Time) map[string]*pb.ClientState
}

type utilizationProvider interface {
	UtilizationRatio() float64
	UtilizationChanges() <-chan struct{}
}

type Disseminator struct {
	mu       sync.Mutex
	nodeID   string
	provider StateProvider

	membership *Membership
	transport  *Transport

	lastSentNs map[string]int64

	tickInterval  time.Duration
	fanout        int
	fullSyncEvery int
	deltaCount    int

	wg sync.WaitGroup
}

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

func (d *Disseminator) Start(ctx context.Context) {
	d.wg.Add(1)
	go util.SafeGo("dissemination-supervisor", func() {
		defer d.wg.Done()
		util.SafeGoLoop("dissemination-tick-loop", ctx, func() {
			lastRound := time.Now()
			timer := time.NewTimer(d.currentTickInterval())
			defer timer.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-timer.C:
					d.disseminate(ctx)
					lastRound = time.Now()
					timer.Reset(d.currentTickInterval())
				case <-d.utilizationChanges():
					resetTimer(timer, time.Until(lastRound.Add(d.currentTickInterval())))
				}
			}
		})
	})
}

func (d *Disseminator) Stop() {
	d.wg.Wait()
}

var testTickPanicHook atomic.Pointer[func()]
var testSyncPanicHook atomic.Pointer[func()]

func (d *Disseminator) disseminate(ctx context.Context) {
	if hook := testTickPanicHook.Load(); hook != nil {
		(*hook)()
	}

	peers := d.membership.GetRandomKReachable(d.fanout, d.nodeID)
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
		go util.SafeGo("dissemination-peer-sync", func() {
			defer d.wg.Done()
			select {
			case <-ctx.Done():
				return
			default:

				sendCtx, cancel := context.WithTimeout(ctx, 3*d.currentTickInterval())
				defer cancel()
				d.sendToPeer(sendCtx, p, sendFull)
			}
		})
	}
}

func (d *Disseminator) sendToPeer(ctx context.Context, peer Member, sendFull bool) {
	if hook := testSyncPanicHook.Load(); hook != nil {
		(*hook)()
	}
	d.mu.Lock()
	var since time.Time
	if !sendFull {
		if ns := d.lastSentNs[peer.NodeID]; ns != 0 {
			since = time.Unix(0, ns)
		}
	}
	state := d.provider.GenerateDelta(since)
	slog.Debug("generated delta", "node", d.nodeID, "peer", peer.NodeID, "since", since, "clients", len(state))

	sendTime := time.Now().UnixNano()
	d.mu.Unlock()

	if len(state) == 0 {

		d.advanceLastSent(peer.NodeID, sendTime)
		return
	}

	req := &pb.GossipRequest{
		SourceNodeId: d.nodeID,
		State:        state,
	}
	if _, err := d.transport.SendState(ctx, peer.Address, req); err != nil {
		slog.Debug("gossip send failed", "from", d.nodeID, "to", peer.Address, "err", err)

		return
	}

	d.advanceLastSent(peer.NodeID, sendTime)
}

func (d *Disseminator) advanceLastSent(nodeID string, sendTime int64) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if sendTime > d.lastSentNs[nodeID] {
		d.lastSentNs[nodeID] = sendTime
	}
}

func (d *Disseminator) currentTickInterval() time.Duration {
	provider, ok := d.provider.(utilizationProvider)
	if !ok {
		return d.tickInterval
	}
	return GossipInterval(d.tickInterval, provider.UtilizationRatio())
}

func (d *Disseminator) utilizationChanges() <-chan struct{} {
	provider, ok := d.provider.(utilizationProvider)
	if !ok {
		return nil
	}
	return provider.UtilizationChanges()
}

func resetTimer(timer *time.Timer, delay time.Duration) {
	if delay < 0 {
		delay = 0
	}
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
	timer.Reset(delay)
}
