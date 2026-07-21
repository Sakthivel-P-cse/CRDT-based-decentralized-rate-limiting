package gossip

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestMembership_Operations(t *testing.T) {
	m := NewMembership()

	m.Add(&Member{NodeID: "node-1", Address: "localhost:1", Status: StatusAlive, LastSeen: time.Now()})
	assert.Len(t, m.GetAlive(), 1)

	m.Update(&Member{NodeID: "node-1", Address: "localhost:1", Incarnation: 1, Status: StatusAlive, LastSeen: time.Now()})
	assert.Equal(t, 1, m.GetAll()[0].Incarnation)

	m.Update(&Member{NodeID: "node-1", Address: "localhost:1", Incarnation: 0, Status: StatusAlive, LastSeen: time.Now()})
	assert.Equal(t, 1, m.GetAll()[0].Incarnation)

	m.Remove("node-1")
	assert.Empty(t, m.GetAll())
}

func TestMembership_StatusTransitions(t *testing.T) {
	m := NewMembership()
	m.Add(&Member{NodeID: "node-1", Address: "localhost:1", Status: StatusAlive, LastSeen: time.Now()})

	m.MarkSuspect("node-1")
	assert.Empty(t, m.GetAlive())
	assert.Equal(t, StatusSuspect, m.GetAll()[0].Status)

	suspect := m.GetAll()[0]
	m.ConfirmDead("node-1", StatusSuspect, suspect.Incarnation, suspect.LastSeen)
	assert.Equal(t, StatusDead, m.GetAll()[0].Status)

	m.Revive("node-1")
	assert.Len(t, m.GetAlive(), 1)
	assert.Equal(t, StatusAlive, m.GetAll()[0].Status)
	assert.Equal(t, 1, m.GetAll()[0].Incarnation)
}

func TestMembership_EqualIncarnationForwardOnly(t *testing.T) {
	m := NewMembership()
	m.Add(&Member{NodeID: "node-1", Address: "localhost:1", Incarnation: 3, Status: StatusAlive, LastSeen: time.Now()})
	m.Add(&Member{NodeID: "node-1", Address: "localhost:1", Incarnation: 3, Status: StatusSuspect, LastSeen: time.Now()})
	assert.Equal(t, StatusSuspect, m.GetAll()[0].Status)

	m.Add(&Member{NodeID: "node-1", Address: "localhost:1", Incarnation: 3, Status: StatusAlive, LastSeen: time.Now()})
	assert.Equal(t, StatusSuspect, m.GetAll()[0].Status)

	m.Add(&Member{NodeID: "node-1", Address: "localhost:1", Incarnation: 3, Status: StatusDead, LastSeen: time.Now()})
	assert.Equal(t, StatusDead, m.GetAll()[0].Status)
}

func TestMembership_ExcludeSelf(t *testing.T) {
	m := NewMembership()
	m.Add(&Member{NodeID: "self", Address: "localhost:1", Status: StatusAlive, LastSeen: time.Now()})
	m.Add(&Member{NodeID: "peer1", Address: "localhost:2", Status: StatusAlive, LastSeen: time.Now()})
	m.Add(&Member{NodeID: "peer2", Address: "localhost:3", Status: StatusAlive, LastSeen: time.Now()})

	peers := m.GetRandomKAlive(10, "self")
	for _, p := range peers {
		assert.NotEqual(t, "self", p.NodeID, "self should be excluded")
	}
	assert.Len(t, peers, 2)
}

func TestMembership_Concurrency(t *testing.T) {
	m := NewMembership()
	var wg sync.WaitGroup
	for i := range 100 {
		wg.Add(1)
		go func(incarnation int) {
			defer wg.Done()
			m.Add(&Member{
				NodeID:      "node-concurrent",
				Address:     "localhost:1",
				Incarnation: incarnation,
				Status:      StatusAlive,
				LastSeen:    time.Now(),
			})
		}(i)
	}
	wg.Wait()
	all := m.GetAll()
	assert.Len(t, all, 1)
	assert.Equal(t, 99, all[0].Incarnation)
}

func TestMembership_GetAll_Race(t *testing.T) {
	m := NewMembership()
	m.Add(&Member{NodeID: "node-race", Address: "localhost:1", Status: StatusAlive, LastSeen: time.Now()})

	done := make(chan struct{})
	const iters = 500

	go func() {
		for i := 0; i < iters; i++ {
			m.MarkSuspect("node-race")
			m.Revive("node-race")
		}
		close(done)
	}()

	for i := 0; i < iters; i++ {
		for _, mem := range m.GetAll() {

			_ = mem.Status
		}
	}
	<-done
}

func TestMembership_ShuffleUniform(t *testing.T) {
	const (
		numPeers = 5
		trials   = 2000
		k        = 1
	)
	m := NewMembership()
	peers := []string{"p1", "p2", "p3", "p4", "p5"}
	for _, id := range peers {
		m.Add(&Member{NodeID: id, Address: id + ":1", Status: StatusAlive, LastSeen: time.Now()})
	}

	counts := make(map[string]int, numPeers)
	for i := 0; i < trials; i++ {
		selected := m.GetRandomKAlive(k)
		if len(selected) > 0 {
			counts[selected[0].NodeID]++
		}
	}

	expected := float64(trials) / float64(numPeers)
	for _, id := range peers {
		c := float64(counts[id])

		if c < expected*0.60 || c > expected*1.40 {
			t.Errorf("peer %s selected %d times (expected ~%.0f ±40%%); distribution is biased", id, counts[id], expected)
		}
	}
	t.Logf("shuffle uniformity counts (expected ~%.0f each): %v", expected, counts)
}
