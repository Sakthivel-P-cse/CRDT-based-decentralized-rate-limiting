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

	// Update with higher incarnation should apply.
	m.Update(&Member{NodeID: "node-1", Address: "localhost:1", Incarnation: 1, Status: StatusAlive, LastSeen: time.Now()})
	assert.Equal(t, 1, m.GetAll()[0].Incarnation)

	// Update with lower incarnation should be ignored.
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

	m.ConfirmDead("node-1")
	assert.Equal(t, StatusDead, m.GetAll()[0].Status)

	m.Revive("node-1")
	assert.Len(t, m.GetAlive(), 1)
	assert.Equal(t, StatusAlive, m.GetAll()[0].Status)
	assert.Equal(t, 1, m.GetAll()[0].Incarnation) // bumped on revive
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
	assert.Equal(t, 99, all[0].Incarnation) // must settle on highest incarnation
}
