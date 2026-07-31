package node

import (
	"fmt"
	"math"
	"time"
)

type Config struct {
	NodeID string

	ListenAddr string

	MetricsAddr string

	Peers []string

	EpochSize time.Duration

	WindowSize time.Duration

	GossipInterval time.Duration

	SuspectTimeout time.Duration

	PingTimeout time.Duration

	Fanout int

	RateLimit int64

	SoftThresholdRatio float64
}

func (c *Config) Validate() error {
	if c.NodeID == "" {
		return fmt.Errorf("NodeID cannot be empty")
	}
	if c.ListenAddr == "" {
		return fmt.Errorf("ListenAddr cannot be empty")
	}

	if c.RateLimit <= 0 {
		return fmt.Errorf("RateLimit must be > 0")
	}
	if math.IsNaN(c.SoftThresholdRatio) || math.IsInf(c.SoftThresholdRatio, 0) || c.SoftThresholdRatio <= 0 || c.SoftThresholdRatio >= 1 {
		return fmt.Errorf("SoftThresholdRatio must be finite, > 0, and < 1")
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
