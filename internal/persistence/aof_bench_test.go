package persistence_test

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"tinycache/internal/persistence"
)

func BenchmarkAOF_Append(b *testing.B) {
	sizes := []struct {
		name string
		size int
	}{
		{"64B", 64},
		{"1KB", 1024},
	}
	for _, vs := range sizes {
		b.Run(vs.name, func(b *testing.B) {
			dir := b.TempDir()
			path := filepath.Join(dir, fmt.Sprintf("bench-%s.aof", vs.name))
			w, err := persistence.NewAOFWriter(path, "no")
			if err != nil {
				b.Fatal(err)
			}
			b.Cleanup(func() {
				_ = w.Close()
				_ = os.Remove(path)
			})

			entry := persistence.AOFEntry{
				LSN:     1,
				Op:      persistence.OpSet,
				TTL:     0,
				Version: 1,
				Key:     "bench-key",
				Value:   bytes.Repeat([]byte("x"), vs.size),
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := range b.N {
				entry.LSN = uint64(i) //nolint:gosec // benchmark counter
				if err := w.Append(entry); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
