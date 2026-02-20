package cache_test

import (
	"bytes"
	"fmt"
	"math/rand/v2"
	"sync/atomic"
	"testing"
	"tinycache/internal/cache"
)

var valueSizes = []struct {
	name string
	size int
}{
	{"64B", 64},
	{"1KB", 1024},
	{"64KB", 64 * 1024},
}

func makeValue(size int) []byte {
	return bytes.Repeat([]byte("x"), size)
}

func BenchmarkStore_Set(b *testing.B) {
	for _, vs := range valueSizes {
		b.Run(vs.name, func(b *testing.B) {
			s := cache.NewStore(512, 0)
			val := makeValue(vs.size)
			b.ReportAllocs()
			b.ResetTimer()
			for i := range b.N {
				s.Set(fmt.Sprintf("key:%d", i), val, 0, 0)
			}
		})
	}
}

func BenchmarkStore_Get_Hit(b *testing.B) {
	for _, vs := range valueSizes {
		b.Run(vs.name, func(b *testing.B) {
			s := cache.NewStore(512, 0)
			val := makeValue(vs.size)
			const preload = 10000
			for i := range preload {
				s.Set(fmt.Sprintf("key:%d", i), val, 0, 0)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := range b.N {
				s.Get(fmt.Sprintf("key:%d", i%preload))
			}
		})
	}
}

func BenchmarkStore_Get_Miss(b *testing.B) {
	s := cache.NewStore(512, 0)
	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		s.Get(fmt.Sprintf("miss:%d", i))
	}
}

func BenchmarkStore_Delete(b *testing.B) {
	s := cache.NewStore(512, 0)
	val := makeValue(64)
	for i := range b.N {
		s.Set(fmt.Sprintf("key:%d", i), val, 0, 0)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		s.Delete(fmt.Sprintf("key:%d", i))
	}
}

func BenchmarkStore_CAS(b *testing.B) {
	for _, vs := range valueSizes {
		b.Run(vs.name, func(b *testing.B) {
			s := cache.NewStore(512, 0)
			val := makeValue(vs.size)
			const preload = 10000
			tokens := make([]uint64, preload)
			for i := range preload {
				tokens[i] = s.Set(fmt.Sprintf("key:%d", i), val, 0, 0)
			}
			newVal := makeValue(vs.size)
			b.ReportAllocs()
			b.ResetTimer()
			for i := range b.N {
				idx := i % preload
				key := fmt.Sprintf("key:%d", idx)
				e, ok := s.Gets(key)
				if ok {
					s.Cas(key, newVal, 0, 0, e.CasToken)
				}
			}
		})
	}
}

func BenchmarkStore_ParallelSet(b *testing.B) {
	for _, vs := range valueSizes {
		b.Run(vs.name, func(b *testing.B) {
			s := cache.NewStore(512, 0)
			val := makeValue(vs.size)
			var counter atomic.Int64
			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					i := counter.Add(1)
					s.Set(fmt.Sprintf("key:%d", i), val, 0, 0)
				}
			})
		})
	}
}

func BenchmarkStore_ParallelGet(b *testing.B) {
	for _, vs := range valueSizes {
		b.Run(vs.name, func(b *testing.B) {
			s := cache.NewStore(512, 0)
			val := makeValue(vs.size)
			const preload = 100000
			for i := range preload {
				s.Set(fmt.Sprintf("key:%d", i), val, 0, 0)
			}
			var counter atomic.Int64
			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					i := counter.Add(1)
					s.Get(fmt.Sprintf("key:%d", i%preload))
				}
			})
		})
	}
}

func BenchmarkStore_ParallelMixed(b *testing.B) {
	for _, vs := range valueSizes {
		b.Run(vs.name, func(b *testing.B) {
			s := cache.NewStore(512, 0)
			val := makeValue(vs.size)
			const preload = 100000
			for i := range preload {
				s.Set(fmt.Sprintf("key:%d", i), val, 0, 0)
			}
			var counter atomic.Int64
			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				r := rand.New(rand.NewPCG(rand.Uint64(), rand.Uint64()))
				for pb.Next() {
					i := counter.Add(1)
					key := fmt.Sprintf("key:%d", i%preload)
					if r.IntN(100) < 80 {
						s.Get(key)
					} else {
						s.Set(key, val, 0, 0)
					}
				}
			})
		})
	}
}
