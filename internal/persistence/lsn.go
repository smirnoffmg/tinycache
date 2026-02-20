package persistence

import "sync/atomic"

// LSNGenerator produces monotonically increasing Log Sequence Numbers.
type LSNGenerator struct {
	counter atomic.Uint64
}

// Next returns the next LSN.
func (g *LSNGenerator) Next() uint64 {
	return g.counter.Add(1)
}

// Current returns the current LSN without incrementing.
func (g *LSNGenerator) Current() uint64 {
	return g.counter.Load()
}

// Reset sets the LSN counter to a specific value (used during recovery).
func (g *LSNGenerator) Reset(val uint64) {
	g.counter.Store(val)
}
