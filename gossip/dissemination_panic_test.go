package gossip

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sakthivel-p-cse/crdt-rate-limiter/crdt"
	"github.com/stretchr/testify/assert"
)

func TestDisseminator_PeerSync_PanicRecovery(t *testing.T) {

	defer testSyncPanicHook.Store(nil)

	var panics int32
	hook := func() {
		if atomic.AddInt32(&panics, 1) == 1 {
			panic("deliberate test panic in peer-sync")
		}
	}
	testSyncPanicHook.Store(&hook)

	transport := NewTransport()
	defer transport.Close()

	membership := NewMembership()
	membership.Add(&Member{
		NodeID:   "peer-1",
		Address:  "127.0.0.1:9999",
		Status:   StatusAlive,
		LastSeen: time.Now(),
	})

	window := crdt.NewEpochWindow(1*time.Second, 60*time.Second)
	provider := &mockStateProvider{
		windows: map[string]*crdt.EpochWindow{"client-1": window},
	}

	d := NewDisseminator("test-node", transport, membership, provider, 10*time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	d.Start(ctx)

	assert.Eventually(t, func() bool {
		return atomic.LoadInt32(&panics) >= 3
	}, 1*time.Second, 10*time.Millisecond, "dissemination loop stopped functioning after panic")
	cancel()
	d.Stop()
}

func TestDisseminator_TickLoop_PanicRecovery(t *testing.T) {

	defer testTickPanicHook.Store(nil)

	var panics int32
	hook := func() {
		if atomic.AddInt32(&panics, 1) == 1 {
			panic("deliberate test panic in tick-loop")
		}
	}
	testTickPanicHook.Store(&hook)

	transport := NewTransport()
	defer transport.Close()

	membership := NewMembership()
	d := NewDisseminator("test-node", transport, membership, &mockStateProvider{}, 10*time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	d.Start(ctx)
	assert.Eventually(t, func() bool {
		return atomic.LoadInt32(&panics) >= 3
	}, time.Second, 10*time.Millisecond)

	cancel()
	d.Stop()
	stoppedAt := atomic.LoadInt32(&panics)
	time.Sleep(30 * time.Millisecond)
	assert.Equal(t, stoppedAt, atomic.LoadInt32(&panics))
}
