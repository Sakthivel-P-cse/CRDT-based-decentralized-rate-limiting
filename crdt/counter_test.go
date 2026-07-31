package crdt

import (
	"math/rand"
	"testing"
)

func TestGCounter_IncrementAndValue(t *testing.T) {
	c := NewGCounter()
	c.Increment("nodeA", 5)
	c.Increment("nodeB", 3)
	c.Increment("nodeA", 2)

	if val := c.Value(); val != 10 {
		t.Fatalf("expected 10, got %d", val)
	}
}

func TestGCounter_Merge(t *testing.T) {
	c1 := NewGCounter()
	c1.Increment("nodeA", 5)
	c1.Increment("nodeB", 3)

	c2 := NewGCounter()
	c2.Increment("nodeB", 8)
	c2.Increment("nodeC", 2)

	c1.Merge(c2)

	if val := c1.Value(); val != 15 {
		t.Fatalf("expected 15, got %d", val)
	}
}

func TestGCounter_CommutativeAssociativeIdempotent(t *testing.T) {

	gen := func(seed int64) *GCounter {
		r := rand.New(rand.NewSource(seed))
		c := NewGCounter()
		c.Increment("nodeA", r.Int63n(100))
		c.Increment("nodeB", r.Int63n(100))
		c.Increment("nodeC", r.Int63n(100))
		return c
	}

	c1 := gen(1)
	c2 := gen(2)
	c3 := gen(3)

	c1Copy := gen(1)
	c1Copy.Merge(c1)
	if c1Copy.Value() != c1.Value() {
		t.Fatal("Idempotency failed")
	}

	c1Copy2 := gen(1)
	c1Copy2.Merge(c2)

	c2Copy := gen(2)
	c2Copy.Merge(c1)

	if c1Copy2.Value() != c2Copy.Value() {
		t.Fatal("Commutativity failed")
	}

	leftGroup := gen(1)
	leftGroup.Merge(c2)
	leftGroup.Merge(c3)

	rightGroup := gen(2)
	rightGroup.Merge(c3)
	finalRightGroup := gen(1)
	finalRightGroup.Merge(rightGroup)

	if leftGroup.Value() != finalRightGroup.Value() {
		t.Fatal("Associativity failed")
	}
}
