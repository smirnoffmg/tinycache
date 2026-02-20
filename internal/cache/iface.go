package cache

// ReadWriter defines the interface for cache operations.
// The handler depends on this interface rather than the concrete Store type (Dependency Inversion).
type ReadWriter interface {
	Get(key string) (*Entry, bool)
	Gets(key string) (*Entry, bool)
	Set(key string, value []byte, flags uint32, exptime int64) uint64
	Add(key string, value []byte, flags uint32, exptime int64) (uint64, bool)
	Replace(key string, value []byte, flags uint32, exptime int64) (uint64, bool)
	Cas(key string, value []byte, flags uint32, exptime int64, casUnique uint64) CasResult
	Delete(key string) bool
	FlushAll()
	Stats() StoreStats
}
