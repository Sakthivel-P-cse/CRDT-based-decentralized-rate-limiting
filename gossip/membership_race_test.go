package gossip

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestConfirmDead_Race(t *testing.T) {
	m := NewMembership()

	nodeID := "node-a"
	m.Add(&Member{
		NodeID:      nodeID,
		Address:     "10.0.0.1:5050",
		Incarnation: 1,
		Status:      StatusAlive,
		LastSeen:    time.Now(),
	})

	m.MarkSuspect(nodeID)

	all := m.GetAll()
	var suspect *Member
	for i := range all {
		if all[i].NodeID == nodeID {
			suspect = &all[i]
			break
		}
	}
	assert.NotNil(t, suspect)
	assert.Equal(t, StatusSuspect, suspect.Status)

	m.Revive(nodeID)

	aliveRead := m.GetAll()[0]
	assert.Equal(t, StatusAlive, aliveRead.Status)

	m.ConfirmDead(nodeID, StatusSuspect, suspect.Incarnation, suspect.LastSeen)

	finalRead := m.GetAll()[0]
	assert.Equal(t, StatusAlive, finalRead.Status, "Node should remain Alive; ConfirmDead incorrectly overwrote state due to TOC-TOU race")
}
