package persistence_test

import (
	"sync"
	"testing"
	"tinycache/internal/persistence"
)

func TestLSN_StartsAtZero(t *testing.T) {
	t.Parallel()
	g := &persistence.LSNGenerator{}
	if got := g.Current(); got != 0 {
		t.Errorf("expected 0, got %d", got)
	}
}

func TestLSN_NextIncrementsMonotonically(t *testing.T) {
	t.Parallel()
	g := &persistence.LSNGenerator{}

	v1 := g.Next()
	v2 := g.Next()
	v3 := g.Next()

	if v1 != 1 || v2 != 2 || v3 != 3 {
		t.Errorf("expected 1,2,3 got %d,%d,%d", v1, v2, v3)
	}
	if g.Current() != 3 {
		t.Errorf("Current() = %d, want 3", g.Current())
	}
}

func TestLSN_Reset(t *testing.T) {
	t.Parallel()
	g := &persistence.LSNGenerator{}

	g.Next()
	g.Next()
	g.Reset(100)

	if g.Current() != 100 {
		t.Errorf("after Reset(100), Current() = %d", g.Current())
	}

	next := g.Next()
	if next != 101 {
		t.Errorf("after Reset(100), Next() = %d, want 101", next)
	}
}

func TestLSN_ConcurrentNext(t *testing.T) {
	t.Parallel()
	g := &persistence.LSNGenerator{}

	const goroutines = 100
	var wg sync.WaitGroup
	seen := make(chan uint64, goroutines)

	for range goroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			seen <- g.Next()
		}()
	}

	wg.Wait()
	close(seen)

	unique := make(map[uint64]bool)
	for v := range seen {
		if unique[v] {
			t.Errorf("duplicate LSN: %d", v)
		}
		unique[v] = true
	}

	if len(unique) != goroutines {
		t.Errorf("expected %d unique LSNs, got %d", goroutines, len(unique))
	}
	if g.Current() != goroutines {
		t.Errorf("Current() = %d, want %d", g.Current(), goroutines)
	}
}
