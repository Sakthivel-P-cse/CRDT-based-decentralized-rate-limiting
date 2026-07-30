package gossip

import "time"

const ConvergenceRounds = 4

func GossipInterval(baseInterval time.Duration, proximityToLimit float64) time.Duration {
	factor := 1.0 - proximityToLimit
	if factor < 0.2 {
		factor = 0.2
	}
	return time.Duration(float64(baseInterval) * factor)
}
