package cache_test

import (
	"fmt"
	"sync"
	"testing"
	"time"
	"tinycache/internal/cache"
)

func TestSetAndGet(t *testing.T) {
	t.Parallel()
	s := cache.NewStore(256, 0)

	s.Set("foo", []byte("bar"), 0, 0)
	e, ok := s.Get("foo")
	if !ok {
		t.Fatal("expected key foo to exist")
	}
	assertEqual(t, string(e.Value), "bar")
	assertEqual(t, e.Flags, uint32(0))
}

func TestGet_Miss(t *testing.T) {
	t.Parallel()
	s := cache.NewStore(256, 0)

	_, ok := s.Get("nonexistent")
	assertEqual(t, ok, false)
}

func TestSet_OverwriteIncrementsVersion(t *testing.T) {
	t.Parallel()
	s := cache.NewStore(256, 0)

	v1 := s.Set("key", []byte("a"), 0, 0)
	v2 := s.Set("key", []byte("b"), 0, 0)
	if v2 <= v1 {
		t.Errorf("expected version to increase: v1=%d, v2=%d", v1, v2)
	}
}

func TestGets_ReturnsCASToken(t *testing.T) {
	t.Parallel()
	s := cache.NewStore(256, 0)

	s.Set("foo", []byte("bar"), 0, 0)
	e, ok := s.Gets("foo")
	if !ok {
		t.Fatal("expected key to exist")
	}
	if e.CasToken == 0 {
		t.Error("expected non-zero CAS token")
	}
}

func TestAdd_SucceedsWhenKeyMissing(t *testing.T) {
	t.Parallel()
	s := cache.NewStore(256, 0)

	_, ok := s.Add("k", []byte("v"), 0, 0)
	assertEqual(t, ok, true)

	e, found := s.Get("k")
	if !found {
		t.Fatal("expected key to exist after add")
	}
	assertEqual(t, string(e.Value), "v")
}

func TestAdd_FailsWhenKeyExists(t *testing.T) {
	t.Parallel()
	s := cache.NewStore(256, 0)

	s.Set("k", []byte("v1"), 0, 0)
	_, ok := s.Add("k", []byte("v2"), 0, 0)
	assertEqual(t, ok, false)

	e, _ := s.Get("k")
	assertEqual(t, string(e.Value), "v1")
}

func TestReplace_SucceedsWhenKeyExists(t *testing.T) {
	t.Parallel()
	s := cache.NewStore(256, 0)

	s.Set("k", []byte("old"), 0, 0)
	_, ok := s.Replace("k", []byte("new"), 0, 0)
	assertEqual(t, ok, true)

	e, _ := s.Get("k")
	assertEqual(t, string(e.Value), "new")
}

func TestReplace_FailsWhenKeyMissing(t *testing.T) {
	t.Parallel()
	s := cache.NewStore(256, 0)

	_, ok := s.Replace("k", []byte("v"), 0, 0)
	assertEqual(t, ok, false)
}

func TestCas_SucceedsOnMatchingToken(t *testing.T) {
	t.Parallel()
	s := cache.NewStore(256, 0)

	s.Set("k", []byte("old"), 0, 0)
	e, _ := s.Gets("k")

	result := s.Cas("k", []byte("new"), 0, 0, e.CasToken)
	assertEqual(t, result, cache.CasStored)

	updated, _ := s.Get("k")
	assertEqual(t, string(updated.Value), "new")
}

func TestCas_ExistsOnMismatch(t *testing.T) {
	t.Parallel()
	s := cache.NewStore(256, 0)

	s.Set("k", []byte("v"), 0, 0)
	result := s.Cas("k", []byte("new"), 0, 0, 99999)
	assertEqual(t, result, cache.CasExists)
}

func TestCas_NotFoundOnMissingKey(t *testing.T) {
	t.Parallel()
	s := cache.NewStore(256, 0)

	result := s.Cas("nope", []byte("v"), 0, 0, 1)
	assertEqual(t, result, cache.CasNotFound)
}

func TestDelete(t *testing.T) {
	t.Parallel()
	s := cache.NewStore(256, 0)

	s.Set("k", []byte("v"), 0, 0)
	ok := s.Delete("k")
	assertEqual(t, ok, true)

	_, found := s.Get("k")
	assertEqual(t, found, false)
}

func TestDelete_MissingKey(t *testing.T) {
	t.Parallel()
	s := cache.NewStore(256, 0)

	ok := s.Delete("nope")
	assertEqual(t, ok, false)
}

func TestFlushAll(t *testing.T) {
	t.Parallel()
	s := cache.NewStore(256, 0)

	s.Set("a", []byte("1"), 0, 0)
	s.Set("b", []byte("2"), 0, 0)
	s.FlushAll()

	assertEqual(t, s.Len(), 0)
	_, ok := s.Get("a")
	assertEqual(t, ok, false)
}

func TestTTL_Expiry(t *testing.T) {
	t.Parallel()
	s := cache.NewStore(256, 0)

	s.Set("ephemeral", []byte("data"), 0, 1)
	e, ok := s.Get("ephemeral")
	if !ok {
		t.Fatal("expected key to exist immediately after set")
	}
	assertEqual(t, string(e.Value), "data")

	time.Sleep(1100 * time.Millisecond)

	_, ok = s.Get("ephemeral")
	assertEqual(t, ok, false)
}

func TestMemoryUsage_Grows(t *testing.T) {
	t.Parallel()
	s := cache.NewStore(256, 0)

	before := s.MemoryUsage()
	s.Set("key1", []byte("value1"), 0, 0)
	after := s.MemoryUsage()

	if after <= before {
		t.Errorf("expected memory to grow: before=%d, after=%d", before, after)
	}
}

func TestLRU_EvictsOldestAccessed(t *testing.T) {
	t.Parallel()

	maxMB := 1
	s := cache.NewStore(int64(maxMB), 0)

	bigVal := make([]byte, 300*1024)
	s.Set("old", bigVal, 0, 0)
	s.Set("mid", bigVal, 0, 0)

	s.Get("mid")

	s.Set("new", bigVal, 0, 0)
	s.Set("newest", bigVal, 0, 0)

	s.EvictLRU()

	_, oldExists := s.Get("old")
	_, midExists := s.Get("mid")

	if oldExists && midExists {
		t.Error("expected LRU eviction to remove at least one old key")
	}
}

func TestLen(t *testing.T) {
	t.Parallel()
	s := cache.NewStore(256, 0)

	assertEqual(t, s.Len(), 0)
	s.Set("a", []byte("1"), 0, 0)
	s.Set("b", []byte("2"), 0, 0)
	assertEqual(t, s.Len(), 2)
	s.Delete("a")
	assertEqual(t, s.Len(), 1)
}

func TestSet_Flags(t *testing.T) {
	t.Parallel()
	s := cache.NewStore(256, 0)

	s.Set("k", []byte("v"), 42, 0)
	e, _ := s.Get("k")
	assertEqual(t, e.Flags, uint32(42))
}

func TestConcurrency(t *testing.T) {
	t.Parallel()
	s := cache.NewStore(256, 0)

	var wg sync.WaitGroup
	for i := range 100 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			key := fmt.Sprintf("key-%d", i)
			s.Set(key, []byte("val"), 0, 0)
			s.Get(key)
			s.Delete(key)
		}()
	}
	wg.Wait()
}

func TestStats(t *testing.T) {
	t.Parallel()
	s := cache.NewStore(256, 0)

	s.Set("a", []byte("1"), 0, 0)
	s.Set("b", []byte("2"), 0, 0)
	s.Get("a")
	s.Get("missing")

	stats := s.Stats()
	if stats.Items != 2 {
		t.Errorf("expected 2 items, got %d", stats.Items)
	}
}

func TestCas_ConcurrentRace(t *testing.T) {
	t.Parallel()
	s := cache.NewStore(256, 0)

	s.Set("race", []byte("initial"), 0, 0)
	e, _ := s.Gets("race")
	token := e.CasToken

	const goroutines = 50
	wins := make(chan bool, goroutines)

	var wg sync.WaitGroup
	for range goroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			result := s.Cas("race", []byte("updated"), 0, 0, token)
			wins <- (result == cache.CasStored)
		}()
	}
	wg.Wait()
	close(wins)

	winCount := 0
	for w := range wins {
		if w {
			winCount++
		}
	}

	if winCount != 1 {
		t.Errorf("expected exactly 1 CAS winner, got %d", winCount)
	}
}

func TestEvictExpired_Direct(t *testing.T) {
	t.Parallel()
	s := cache.NewStore(256, 0)

	s.Set("live", []byte("val"), 0, 0)
	s.Set("expiring", []byte("val"), 0, 1)

	time.Sleep(1100 * time.Millisecond)

	evicted := s.EvictExpired()
	if evicted != 1 {
		t.Errorf("expected 1 eviction, got %d", evicted)
	}

	if _, ok := s.Get("live"); !ok {
		t.Error("live key should still exist")
	}
	if _, ok := s.Get("expiring"); ok {
		t.Error("expiring key should be gone")
	}
}

func TestEvictLRU_MemoryPressure(t *testing.T) {
	t.Parallel()
	s := cache.NewStore(1, 0) // 1 MB limit

	val := make([]byte, 200*1024) // 200KB each
	for i := range 10 {
		s.Set(fmt.Sprintf("key-%d", i), val, 0, 0)
	}

	evicted := s.EvictLRU()
	if evicted == 0 {
		t.Error("expected at least some keys to be evicted under memory pressure")
	}

	if s.MemoryUsage() > 1024*1024 {
		t.Errorf("memory usage %d should be under 1MB after eviction", s.MemoryUsage())
	}
}

func TestStore_LargeValue(t *testing.T) {
	t.Parallel()
	s := cache.NewStore(256, 0)

	large := make([]byte, 1024*1024) // 1MB
	for i := range large {
		large[i] = byte(i % 256)
	}

	s.Set("big", large, 0, 0)
	e, ok := s.Get("big")
	if !ok {
		t.Fatal("expected big key to exist")
	}
	if len(e.Value) != len(large) {
		t.Errorf("value length %d, want %d", len(e.Value), len(large))
	}
	for i := range 100 {
		if e.Value[i] != large[i] {
			t.Errorf("byte mismatch at index %d", i)
			break
		}
	}
}

func TestStore_HighConcurrency(t *testing.T) {
	t.Parallel()
	s := cache.NewStore(256, 0)

	const goroutines = 200
	var wg sync.WaitGroup

	for i := range goroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			key := fmt.Sprintf("key-%d", i)
			val := []byte(fmt.Sprintf("val-%d", i))

			s.Set(key, val, uint32(i), 0)
			if e, ok := s.Get(key); ok {
				_ = e.Value
			}
			s.Gets(key)
			if i%3 == 0 {
				s.Delete(key)
			}
			if i%5 == 0 {
				s.Add(key, val, 0, 0)
			}
			if i%7 == 0 {
				s.Replace(key, val, 0, 0)
			}
		}()
	}
	wg.Wait()
}

func TestAdd_OnExpiredKey(t *testing.T) {
	t.Parallel()
	s := cache.NewStore(256, 0)

	s.Set("k", []byte("old"), 0, 1)
	time.Sleep(1100 * time.Millisecond)

	_, ok := s.Add("k", []byte("new"), 0, 0)
	if !ok {
		t.Error("add should succeed when existing key is expired")
	}

	e, found := s.Get("k")
	if !found {
		t.Fatal("key should exist after add")
	}
	assertEqual(t, string(e.Value), "new")
}

func TestEvictExpired_NoFalsePositives(t *testing.T) {
	t.Parallel()
	s := cache.NewStore(256, 0)

	s.Set("a", []byte("1"), 0, 0)
	s.Set("b", []byte("2"), 0, 3600)

	evicted := s.EvictExpired()
	if evicted != 0 {
		t.Errorf("expected 0 evictions for non-expired keys, got %d", evicted)
	}
	assertEqual(t, s.Len(), 2)
}

func TestMemoryUsage_DecreasesOnDelete(t *testing.T) {
	t.Parallel()
	s := cache.NewStore(256, 0)

	s.Set("k", []byte("value"), 0, 0)
	afterSet := s.MemoryUsage()

	s.Delete("k")
	afterDelete := s.MemoryUsage()

	if afterDelete >= afterSet {
		t.Errorf("memory should decrease after delete: before=%d, after=%d", afterSet, afterDelete)
	}
}

func TestSnapshot_ReturnsAllLiveItems(t *testing.T) {
	t.Parallel()
	s := cache.NewStore(256, 0)

	s.Set("a", []byte("1"), 0, 0)
	s.Set("b", []byte("2"), 42, 3600)
	s.Set("expired", []byte("x"), 0, 1)

	time.Sleep(1100 * time.Millisecond)

	items := s.Snapshot()

	keys := make(map[string]bool)
	for _, item := range items {
		keys[item.Key] = true
	}

	if !keys["a"] {
		t.Error("expected key 'a' in snapshot")
	}
	if !keys["b"] {
		t.Error("expected key 'b' in snapshot")
	}
	if keys["expired"] {
		t.Error("expired key should not be in snapshot")
	}
}

func TestMemoryUsage_DecreasesOnOverwrite(t *testing.T) {
	t.Parallel()
	s := cache.NewStore(256, 0)

	bigVal := make([]byte, 10000)
	s.Set("k", bigVal, 0, 0)
	afterBig := s.MemoryUsage()

	s.Set("k", []byte("tiny"), 0, 0)
	afterSmall := s.MemoryUsage()

	if afterSmall >= afterBig {
		t.Errorf("memory should decrease after smaller overwrite: big=%d, small=%d", afterBig, afterSmall)
	}
}

func assertEqual[T comparable](t *testing.T, got, want T) {
	t.Helper()
	if got != want {
		t.Errorf("got %v, want %v", got, want)
	}
}
