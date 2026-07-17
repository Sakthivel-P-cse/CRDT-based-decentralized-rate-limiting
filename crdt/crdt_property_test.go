package crdt

import (
	"testing"

	"github.com/leanovate/gopter"
	"github.com/leanovate/gopter/prop"
)

// genGCounter is a gopter generator that produces random GCounters.
func genGCounter() gopter.Gen {
	return func(params *gopter.GenParameters) *gopter.GenResult {
		c := NewGCounter()
		nodes := []string{"nodeA", "nodeB", "nodeC"}
		for range params.Rng.Intn(20) {
			c.Increment(nodes[params.Rng.Intn(len(nodes))], int64(params.Rng.Intn(100)+1))
		}
		return gopter.NewGenResult(c, gopter.NoShrinker)
	}
}

// snapshot returns the current counts map for deep comparison.
func snapshot(c *GCounter) map[string]int64 {
	return c.Counts()
}

// cloneCounter creates a deep copy of c for property tests.
func cloneCounter(c *GCounter) *GCounter {
	out := NewGCounter()
	for k, v := range c.Counts() {
		out.Set(k, v)
	}
	return out
}

func mapsEqual(a, b map[string]int64) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}

func TestGCounter_Properties(t *testing.T) {
	params := gopter.DefaultTestParameters()
	params.MinSuccessfulTests = 1000
	props := gopter.NewProperties(params)

	// Commutativity: merge(A, B) == merge(B, A)
	props.Property("Commutativity", prop.ForAll(
		func(a, b *GCounter) bool {
			ab := cloneCounter(a)
			ab.Merge(b)
			ba := cloneCounter(b)
			ba.Merge(a)
			return mapsEqual(snapshot(ab), snapshot(ba))
		},
		genGCounter(), genGCounter(),
	))

	// Associativity: merge(merge(A,B),C) == merge(A,merge(B,C))
	props.Property("Associativity", prop.ForAll(
		func(a, b, c *GCounter) bool {
			left := cloneCounter(a)
			left.Merge(b)
			left.Merge(c)

			right := cloneCounter(b)
			right.Merge(c)
			ar := cloneCounter(a)
			ar.Merge(right)

			return mapsEqual(snapshot(left), snapshot(ar))
		},
		genGCounter(), genGCounter(), genGCounter(),
	))

	// Idempotency: merge(A, A) == A
	props.Property("Idempotency", prop.ForAll(
		func(a *GCounter) bool {
			dup := cloneCounter(a)
			dup.Merge(a)
			return mapsEqual(snapshot(a), snapshot(dup))
		},
		genGCounter(),
	))

	// Monotonic Growth: merge(A,B).Value() >= max(A.Value(), B.Value())
	props.Property("Monotonic Growth", prop.ForAll(
		func(a, b *GCounter) bool {
			av, bv := a.Value(), b.Value()
			m := cloneCounter(a)
			m.Merge(b)
			mv := m.Value()
			return mv >= av && mv >= bv
		},
		genGCounter(), genGCounter(),
	))

	props.TestingRun(t)
}
