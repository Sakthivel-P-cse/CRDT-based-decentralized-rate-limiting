package node

import (
	"fmt"
	"time"
)

// Config holds all tunable parameters for a RateLimiterServer node.
type Config struct {
	// NodeID is a unique identifier for this node within the cluster.
	NodeID string

	// ListenAddr is the host:port this node's gRPC server binds to.
	// It is advertised to peers during the SWIM join handshake.
	ListenAddr string

	// MetricsAddr is the host:port for the Prometheus /metrics endpoint.
	// Leave empty to disable the metrics server.
	MetricsAddr string

	// Peers is the list of seed addresses contacted during startup to
	// bootstrap cluster membership.
	Peers []string

	// EpochSize is the granularity of each time bucket in the sliding window.
	EpochSize time.Duration

	// WindowSize is the total rolling duration used for rate-limit decisions.
	// Must be >= EpochSize.
	WindowSize time.Duration

	// GossipInterval controls how often the disseminator and SWIM tick.
	GossipInterval time.Duration

	// SuspectTimeout is how long a node remains in StatusSuspect before it is
	// declared dead.
	SuspectTimeout time.Duration

	// PingTimeout is the per-attempt deadline for SWIM pings.
	PingTimeout time.Duration

	// Fanout is the number of random peers contacted per gossip/SWIM tick.
	Fanout int

	// RateLimit is the maximum number of requests (by total cost) allowed per
	// client within one WindowSize.
	RateLimit int64
}

// Validate returns an error if the configuration contains invalid or missing values.
func (c *Config) Validate() error {
	if c.NodeID == "" {
		return fmt.Errorf("NodeID cannot be empty")
	}
	if c.ListenAddr == "" {
		return fmt.Errorf("ListenAddr cannot be empty")
	}
	// MetricsAddr is optional; when empty the Prometheus endpoint is not started.
	if c.RateLimit <= 0 {
		return fmt.Errorf("RateLimit must be > 0")
	}
	if c.EpochSize <= 0 {
		return fmt.Errorf("EpochSize must be > 0")
	}
	if c.WindowSize <= 0 {
		return fmt.Errorf("WindowSize must be > 0")
	}
	if c.WindowSize < c.EpochSize {
		return fmt.Errorf("WindowSize must be >= EpochSize")
	}
	if c.GossipInterval <= 0 || c.SuspectTimeout <= 0 || c.PingTimeout <= 0 {
		return fmt.Errorf("network timeouts and intervals must be > 0")
	}
	if c.Fanout <= 0 {
		return fmt.Errorf("Fanout must be > 0")
	}

	seen := make(map[string]bool, len(c.Peers))
	for _, peer := range c.Peers {
		if peer == "" {
			return fmt.Errorf("peer address cannot be empty")
		}
		if seen[peer] {
			return fmt.Errorf("duplicate peer address: %s", peer)
		}
		seen[peer] = true
	}
	return nil
}