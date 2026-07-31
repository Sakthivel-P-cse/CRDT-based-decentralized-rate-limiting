package gossip

import (
	"log/slog"
	"math/rand"
	"sync"
	"time"
)

type Status int

const (
	StatusAlive Status = iota

	StatusSuspect

	StatusDead
)

type Member struct {
	NodeID      string
	Address     string
	Incarnation int
	Status      Status
	LastSeen    time.Time
}

type Membership struct {
	mu      sync.RWMutex
	members map[string]*Member
}

func NewMembership() *Membership {
	return &Membership{members: make(map[string]*Member)}
}

func (m *Membership) Add(member *Member) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for id, existing := range m.members {
		if existing.Address == member.Address && id != member.NodeID {
			delete(m.members, id)
		}
	}

	if existing, ok := m.members[member.NodeID]; ok {
		if member.Incarnation > existing.Incarnation ||
			(member.Incarnation == existing.Incarnation && member.Status > existing.Status) {
			slog.Info("MEMBER UPDATED (ADD)", "node", member.NodeID)
			cp := *member
			m.members[member.NodeID] = &cp
		}
		return
	}
	slog.Info("MEMBER ADDED", "node", member.NodeID)
	cp := *member
	m.members[member.NodeID] = &cp
}

func (m *Membership) Update(member *Member) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for id, existing := range m.members {
		if existing.Address == member.Address && id != member.NodeID {
			delete(m.members, id)
		}
	}

	if existing, ok := m.members[member.NodeID]; ok {
		if member.Incarnation >= existing.Incarnation {
			cp := *member
			m.members[member.NodeID] = &cp
		}
		return
	}
	slog.Info("MEMBER ADDED (UPDATE)", "node", member.NodeID)
	cp := *member
	m.members[member.NodeID] = &cp
}

func (m *Membership) Remove(nodeID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.members, nodeID)
}

func (m *Membership) MarkSuspect(nodeID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if member, ok := m.members[nodeID]; ok && member.Status == StatusAlive {
		member.Status = StatusSuspect
		member.LastSeen = time.Now()
	}
}

func (m *Membership) ConfirmDead(nodeID string, expectedStatus Status, expectedIncarnation int, expectedLastSeen time.Time) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if member, ok := m.members[nodeID]; ok {
		if member.Status == expectedStatus && member.Incarnation == expectedIncarnation && member.LastSeen.Equal(expectedLastSeen) {
			member.Status = StatusDead
			member.LastSeen = time.Now()
			return true
		}
	}
	return false
}

func (m *Membership) Revive(nodeID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if member, ok := m.members[nodeID]; ok {
		member.Status = StatusAlive
		member.Incarnation++
		member.LastSeen = time.Now()
	}
}

func (m *Membership) Refute(nodeID string, incomingIncarnation int) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	member, ok := m.members[nodeID]
	if !ok || incomingIncarnation < member.Incarnation {
		return false
	}
	member.Status = StatusAlive
	member.Incarnation = incomingIncarnation + 1
	member.LastSeen = time.Now()
	return true
}

func (m *Membership) GetAlive() []Member {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var alive []Member
	for _, member := range m.members {
		if member.Status == StatusAlive {
			alive = append(alive, *member)
		}
	}
	return alive
}

func (m *Membership) GetRandomKAlive(k int, excludeIDs ...string) []Member {
	exclude := make(map[string]bool, len(excludeIDs))
	for _, id := range excludeIDs {
		exclude[id] = true
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	var alive []Member
	for _, member := range m.members {
		if member.Status == StatusAlive && !exclude[member.NodeID] {
			alive = append(alive, *member)
		}
	}
	if len(alive) <= k {
		return alive
	}

	n := len(alive)
	for i := 0; i < k; i++ {
		j := i + rand.Intn(n-i)
		alive[i], alive[j] = alive[j], alive[i]
	}
	return alive[:k]
}

func (m *Membership) GetRandomKReachable(k int, excludeIDs ...string) []Member {
	exclude := make(map[string]bool, len(excludeIDs))
	for _, id := range excludeIDs {
		exclude[id] = true
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	var reachable []Member
	for _, member := range m.members {
		if member.Status != StatusDead && !exclude[member.NodeID] {
			reachable = append(reachable, *member)
		}
	}
	if len(reachable) <= k {
		return reachable
	}

	n := len(reachable)
	for i := 0; i < k; i++ {
		j := i + rand.Intn(n-i)
		reachable[i], reachable[j] = reachable[j], reachable[i]
	}
	return reachable[:k]
}

func (m *Membership) GetAll() []Member {
	m.mu.RLock()
	defer m.mu.RUnlock()
	all := make([]Member, 0, len(m.members))
	for _, member := range m.members {
		all = append(all, *member)
	}
	return all
}
