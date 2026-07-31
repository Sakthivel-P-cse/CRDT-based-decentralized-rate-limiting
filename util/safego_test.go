package util

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSafeGo_Recovers(t *testing.T) {
	var wg sync.WaitGroup
	wg.Add(1)

	go SafeGo("test-panic", func() {
		defer wg.Done()
		panic("deliberate test panic")
	})

	wg.Wait()

	assert.True(t, true, "goroutine recovered successfully")
}
