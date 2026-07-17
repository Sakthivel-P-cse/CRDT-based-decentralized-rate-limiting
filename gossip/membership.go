package gossip

import (
	"log/slog"
	"math/rand"
	"sync"
	"time"
)

// Status represents the SWIM liveness state of a cluster member.
type Status int

const (
	// StatusAlive indicates the node is reachable and healthy.
	StatusAlive Status = iota
	// StatusSuspect indicates the node failed a direct ping but has not yet
	// been confirmed dead (indirect pings may still succeed).
	StatusSuspect
	// StatusDead indicates the node has been confirmed unreachable and is no
	// longer counted as a peer for gossip or failure detection.
	StatusDead
)

// Member holds the known state of a single cluster node.
type Member struct {
	NodeID      string
	Address     string
	Incarnation int
	Status      Status
	LastSeen    time.Time
}

// Membership is a thread-safe registry of known cluster members.
// It is updated by SWIM and queried by the Disseminator.
type Membership struct {
	mu      sync.RWMutex
	members map[string]*Member
}

// NewMembership returns an empty Membership.
func NewMembership() *Membership {
	return &Membership{members: make(map[string]*Member)}
}

// Add inserts or conditionally updates a member.
// If a member with the same NodeID already exists, the update is only applied
// when the incoming incarnation number is strictly greater. This prevents
// stale gossip from overwriting newer state.
func (m *Membership) Add(member *Member) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Evict any existing record that shares the same address under a different ID.
	for id, existing := range m.members {
		if existing.Address == member.Address && id != member.NodeID {
			delete(m.members, id)
		}
	}

	if existing, ok := m.members[member.NodeID]; ok {
		if member.Incarnation > existing.Incarnation {
			slog.Info("MEMBER UPDATED (ADD)", "node", member.NodeID)
			m.members[member.NodeID] = member
		}
		return
	}
	slog.Info("MEMBER ADDED", "node", member.NodeID)
	m.members[member.NodeID] = member
}

// Update is like Add but applies the update when the incarnation number is
// greater-than-or-equal (used by SWIM when refreshing LastSeen on a live ping).
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
			m.members[member.NodeID] = member
		}
		return
	}
	slog.Info("MEMBER ADDED (UPDATE)", "node", member.NodeID)
	m.members[member.NodeID] = member
}

// Remove deletes a member from the registry.
func (m *Membership) Remove(nodeID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.members, nodeID)
}

// MarkSuspect transitions an Alive member to Suspect.
// No-op if the member does not exist or is already Suspect/Dead.
func (m *Membership) MarkSuspect(nodeID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if member, ok := m.members[nodeID]; ok && member.Status == StatusAlive {
		member.Status = StatusSuspect
		member.LastSeen = time.Now()
	}
}

// ConfirmDead transitions a member to Dead.
func (m *Membership) ConfirmDead(nodeID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if member, ok := m.members[nodeID]; ok {
		member.Status = StatusDead
		member.LastSeen = time.Now()
	}
}

// Revive transitions a Dead or Suspect member back to Alive and bumps its
// incarnation number to supersede stale gossip in transit.
func (m *Membership) Revive(nodeID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if member, ok := m.members[nodeID]; ok {
		member.Status = StatusAlive
		member.Incarnation++
		member.LastSeen = time.Now()
	}
}

// GetAlive returns all members currently in the Alive state.
func (m *Membership) GetAlive() []*Member {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var alive []*Member
	for _, member := range m.members {
		if member.Status == StatusAlive {
			alive = append(alive, member)
		}
	}
	return alive
}

// GetRandomKAlive returns up to k randomly selected Alive members.
// Any node IDs listed in excludeIDs are omitted from consideration, which
// prevents nodes from gossiping or pinging themselves.
func (m *Membership) GetRandomKAlive(k int, excludeIDs ...string) []*Member {
	exclude := make(map[string]bool, len(excludeIDs))
	for _, id := range excludeIDs {
		exclude[id] = true
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	var alive []*Member
	for _, member := range m.members {
		if member.Status == StatusAlive && !exclude[member.NodeID] {
			alive = append(alive, member)
		}
	}
	if len(alive) <= k {
		return alive
	}
	// Fisher-Yates partial shuffle — O(k).
	for i := len(alive) - 1; i > 0 && len(alive)-i <= k; i-- {
		j := rand.Intn(i + 1)
		alive[i], alive[j] = alive[j], alive[i]
	}
	return alive[:k]
}

// GetAll returns all known members regardless of status.
func (m *Membership) GetAll() []*Member {
	m.mu.RLock()
	defer m.mu.RUnlock()
	all := make([]*Member, 0, len(m.members))
	for _, member := range m.members {
		all = append(all, member)
	}
	return all
}