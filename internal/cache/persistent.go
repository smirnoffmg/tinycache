package cache

import (
	"log"
	"tinycache/internal/persistence"
)

// AOFAppender is the interface for appending entries to an AOF.
// Matches the Append method on persistence.AOFWriter.
type AOFAppender interface {
	Append(entry persistence.AOFEntry) error
}

// PersistentStore wraps a ReadWriter and writes mutations to an AOF.
// It follows the Decorator pattern: all reads delegate directly,
// all writes delegate then append to the AOF log.
type PersistentStore struct {
	inner ReadWriter
	aof   AOFAppender
	lsn   *persistence.LSNGenerator
}

// NewPersistentStore creates a PersistentStore decorating the given store.
func NewPersistentStore(inner ReadWriter, aof AOFAppender, lsn *persistence.LSNGenerator) *PersistentStore {
	return &PersistentStore{inner: inner, aof: aof, lsn: lsn}
}

func (p *PersistentStore) Get(key string) (*Entry, bool) { return p.inner.Get(key) }

func (p *PersistentStore) Gets(key string) (*Entry, bool) { return p.inner.Gets(key) }

func (p *PersistentStore) Stats() StoreStats { return p.inner.Stats() }

func (p *PersistentStore) Set(key string, value []byte, flags uint32, exptime int64) uint64 {
	cas := p.inner.Set(key, value, flags, exptime)
	p.appendSet(key, value, flags, exptime)
	return cas
}

func (p *PersistentStore) Add(key string, value []byte, flags uint32, exptime int64) (uint64, bool) {
	cas, ok := p.inner.Add(key, value, flags, exptime)
	if ok {
		p.appendSet(key, value, flags, exptime)
	}
	return cas, ok
}

func (p *PersistentStore) Replace(key string, value []byte, flags uint32, exptime int64) (uint64, bool) {
	cas, ok := p.inner.Replace(key, value, flags, exptime)
	if ok {
		p.appendSet(key, value, flags, exptime)
	}
	return cas, ok
}

func (p *PersistentStore) Cas(key string, value []byte, flags uint32, exptime int64, casUnique uint64) CasResult {
	result := p.inner.Cas(key, value, flags, exptime, casUnique)
	if result == CasStored {
		p.appendSet(key, value, flags, exptime)
	}
	return result
}

func (p *PersistentStore) Delete(key string) bool {
	ok := p.inner.Delete(key)
	if ok {
		p.append(persistence.AOFEntry{
			LSN: p.lsn.Next(),
			Op:  persistence.OpDelete,
			Key: key,
		})
	}
	return ok
}

func (p *PersistentStore) FlushAll() {
	p.inner.FlushAll()
	p.append(persistence.AOFEntry{
		LSN: p.lsn.Next(),
		Op:  persistence.OpFlushAll,
	})
}

func (p *PersistentStore) appendSet(key string, value []byte, flags uint32, exptime int64) {
	p.append(persistence.AOFEntry{
		LSN:   p.lsn.Next(),
		Op:    persistence.OpSet,
		Key:   key,
		Value: value,
		Flags: flags,
		TTL:   exptime,
	})
}

func (p *PersistentStore) append(entry persistence.AOFEntry) {
	if err := p.aof.Append(entry); err != nil {
		log.Printf("AOF append error: %v", err)
	}
}
