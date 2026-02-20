package cache

import (
	"math/rand/v2"
	"time"
)

const lruSampleSize = 5

// EvictExpired removes all entries that have exceeded their TTL.
func (s *Store) EvictExpired() int {
	evicted := 0
	now := time.Now().Unix()

	for i := range s.shards {
		sh := &s.shards[i]
		sh.mu.Lock()
		for key, it := range sh.items {
			if it.exptime > 0 && now >= it.exptime {
				s.memUsage.Add(-it.size())
				delete(sh.items, key)
				evicted++
			}
		}
		sh.mu.Unlock()
	}
	return evicted
}

// EvictLRU performs approximate LRU eviction using random sampling.
// It repeatedly samples random keys across shards and evicts the
// least-recently-accessed until memory usage drops below the configured limit.
func (s *Store) EvictLRU() int {
	if s.maxMemory <= 0 {
		return 0
	}

	evicted := 0
	for s.memUsage.Load() > s.maxMemory {
		removed := s.evictOneSample()
		if !removed {
			break
		}
		evicted++
	}
	return evicted
}

func (s *Store) evictOneSample() bool {
	var (
		bestKey    string
		bestAccess int64 = 1<<63 - 1
		bestShard  *shard
		bestSize   int64
		found      int
	)

	startShard := rand.IntN(shardCount) //nolint:gosec // not crypto
	for i := range shardCount {
		if found >= lruSampleSize {
			break
		}
		idx := (startShard + i) % shardCount
		sh := &s.shards[idx]

		sh.mu.RLock()
		for key, it := range sh.items {
			found++
			if it.accessedAt < bestAccess {
				bestAccess = it.accessedAt
				bestKey = key
				bestShard = sh
				bestSize = it.size()
			}
			if found >= lruSampleSize {
				break
			}
		}
		sh.mu.RUnlock()
	}

	if bestKey == "" {
		return false
	}

	bestShard.mu.Lock()
	if _, ok := bestShard.items[bestKey]; ok {
		delete(bestShard.items, bestKey)
		s.memUsage.Add(-bestSize)
		bestShard.mu.Unlock()
		return true
	}
	bestShard.mu.Unlock()
	return false
}

// StartEvictionLoop starts a background goroutine that periodically evicts
// expired entries and performs LRU eviction when memory exceeds the limit.
// It stops when the done channel is closed.
func (s *Store) StartEvictionLoop(interval time.Duration, done <-chan struct{}) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				s.EvictExpired()
				s.EvictLRU()
			}
		}
	}()
}
