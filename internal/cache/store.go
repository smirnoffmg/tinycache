package cache

import (
	"hash/fnv"
	"sync"
	"sync/atomic"
	"time"
)

type CasResult int

const (
	CasStored   CasResult = iota
	CasExists             // token mismatch
	CasNotFound           // key does not exist
)

type Entry struct {
	Value    []byte
	Flags    uint32
	Exptime  int64 // absolute unix timestamp in seconds; 0 means no expiry
	CasToken uint64
}

type StoreStats struct {
	Items       int
	MemoryBytes int64
}

const shardCount = 256

type shard struct {
	mu    sync.RWMutex
	items map[string]*item
}

type item struct {
	value      []byte
	flags      uint32
	exptime    int64
	casToken   uint64
	accessedAt int64 // unix nano for LRU
}

func (it *item) expired() bool {
	return it.exptime > 0 && time.Now().Unix() >= it.exptime
}

func (it *item) size() int64 {
	return int64(len(it.value)) + 64 // value bytes + fixed overhead estimate
}

// Store is a sharded in-memory key-value store with CAS, TTL, and LRU eviction.
type Store struct {
	shards     [shardCount]shard
	maxMemory  int64 // bytes
	casCounter atomic.Uint64
	memUsage   atomic.Int64
}

// NewStore creates a new Store. maxMemoryMB sets the LRU eviction threshold (0 = unlimited).
// defaultTTL is currently reserved for future use.
func NewStore(maxMemoryMB int64, _ int) *Store {
	s := &Store{
		maxMemory: maxMemoryMB * 1024 * 1024,
	}
	for i := range s.shards {
		s.shards[i].items = make(map[string]*item)
	}
	return s
}

func (s *Store) getShard(key string) *shard {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return &s.shards[h.Sum32()&(shardCount-1)]
}

func (s *Store) nextCas() uint64 {
	return s.casCounter.Add(1)
}

func (s *Store) Get(key string) (*Entry, bool) {
	sh := s.getShard(key)
	sh.mu.RLock()
	it, ok := sh.items[key]
	sh.mu.RUnlock()

	if !ok || it.expired() {
		return nil, false
	}
	atomic.StoreInt64(&it.accessedAt, time.Now().UnixNano())
	return &Entry{
		Value:    it.value,
		Flags:    it.flags,
		Exptime:  it.exptime,
		CasToken: it.casToken,
	}, true
}

func (s *Store) Gets(key string) (*Entry, bool) {
	return s.Get(key)
}

func (s *Store) Set(key string, value []byte, flags uint32, exptime int64) uint64 {
	sh := s.getShard(key)
	cas := s.nextCas()
	now := time.Now()

	sh.mu.Lock()
	old, existed := sh.items[key]
	if existed {
		s.memUsage.Add(-old.size())
	}

	it := &item{
		value:      value,
		flags:      flags,
		exptime:    toAbsExptime(exptime, now),
		casToken:   cas,
		accessedAt: now.UnixNano(),
	}
	sh.items[key] = it
	sh.mu.Unlock()

	s.memUsage.Add(it.size())
	return cas
}

func (s *Store) Add(key string, value []byte, flags uint32, exptime int64) (uint64, bool) {
	sh := s.getShard(key)
	sh.mu.Lock()

	if existing, ok := sh.items[key]; ok && !existing.expired() {
		sh.mu.Unlock()
		return 0, false
	}

	cas := s.nextCas()
	now := time.Now()
	it := &item{
		value:      value,
		flags:      flags,
		exptime:    toAbsExptime(exptime, now),
		casToken:   cas,
		accessedAt: now.UnixNano(),
	}
	sh.items[key] = it
	sh.mu.Unlock()

	s.memUsage.Add(it.size())
	return cas, true
}

func (s *Store) Replace(key string, value []byte, flags uint32, exptime int64) (uint64, bool) {
	sh := s.getShard(key)
	sh.mu.Lock()

	old, ok := sh.items[key]
	if !ok || old.expired() {
		sh.mu.Unlock()
		return 0, false
	}

	s.memUsage.Add(-old.size())

	cas := s.nextCas()
	now := time.Now()
	it := &item{
		value:      value,
		flags:      flags,
		exptime:    toAbsExptime(exptime, now),
		casToken:   cas,
		accessedAt: now.UnixNano(),
	}
	sh.items[key] = it
	sh.mu.Unlock()

	s.memUsage.Add(it.size())
	return cas, true
}

func (s *Store) Delete(key string) bool {
	sh := s.getShard(key)
	sh.mu.Lock()
	old, ok := sh.items[key]
	if ok {
		delete(sh.items, key)
		s.memUsage.Add(-old.size())
	}
	sh.mu.Unlock()
	return ok
}

func (s *Store) Cas(key string, value []byte, flags uint32, exptime int64, casUnique uint64) CasResult {
	sh := s.getShard(key)
	sh.mu.Lock()

	existing, ok := sh.items[key]
	if !ok || existing.expired() {
		sh.mu.Unlock()
		return CasNotFound
	}

	if existing.casToken != casUnique {
		sh.mu.Unlock()
		return CasExists
	}

	s.memUsage.Add(-existing.size())

	cas := s.nextCas()
	now := time.Now()
	it := &item{
		value:      value,
		flags:      flags,
		exptime:    toAbsExptime(exptime, now),
		casToken:   cas,
		accessedAt: now.UnixNano(),
	}
	sh.items[key] = it
	sh.mu.Unlock()

	s.memUsage.Add(it.size())
	return CasStored
}

func (s *Store) FlushAll() {
	for i := range s.shards {
		sh := &s.shards[i]
		sh.mu.Lock()
		sh.items = make(map[string]*item)
		sh.mu.Unlock()
	}
	s.memUsage.Store(0)
}

func (s *Store) Stats() StoreStats {
	return StoreStats{
		Items:       s.Len(),
		MemoryBytes: s.MemoryUsage(),
	}
}

func (s *Store) MemoryUsage() int64 {
	return s.memUsage.Load()
}

func (s *Store) Len() int {
	total := 0
	for i := range s.shards {
		sh := &s.shards[i]
		sh.mu.RLock()
		total += len(sh.items)
		sh.mu.RUnlock()
	}
	return total
}

// SnapshotItem represents a single key-value pair exported for snapshotting.
type SnapshotItem struct {
	Key     string
	Value   []byte
	Flags   uint32
	Exptime int64
	CAS     uint64
}

// Snapshot returns a consistent copy of all live (non-expired) items.
func (s *Store) Snapshot() []SnapshotItem {
	now := time.Now().Unix()
	var items []SnapshotItem

	for i := range s.shards {
		sh := &s.shards[i]
		sh.mu.RLock()
		for key, it := range sh.items {
			if it.exptime > 0 && now >= it.exptime {
				continue
			}
			items = append(items, SnapshotItem{
				Key:     key,
				Value:   append([]byte(nil), it.value...),
				Flags:   it.flags,
				Exptime: it.exptime,
				CAS:     it.casToken,
			})
		}
		sh.mu.RUnlock()
	}
	return items
}

func toAbsExptime(exptime int64, now time.Time) int64 {
	if exptime == 0 {
		return 0
	}
	// Memcached convention: values <= 30 days (2592000 seconds) are relative
	const thirtyDays = 30 * 24 * 60 * 60
	if exptime <= thirtyDays {
		return now.Unix() + exptime
	}
	return exptime
}
