package protocol_test

import (
	"bufio"
	"bytes"
	"fmt"
	"strings"
	"testing"
	"tinycache/internal/protocol"
)

func BenchmarkParse_Get(b *testing.B) {
	raw := []byte("get mykey\r\n")
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		r := bufio.NewReader(bytes.NewReader(raw))
		_, _ = protocol.Parse(r)
	}
}

func BenchmarkParse_Set(b *testing.B) {
	sizes := []struct {
		name string
		size int
	}{
		{"64B", 64},
		{"1KB", 1024},
	}
	for _, vs := range sizes {
		b.Run(vs.name, func(b *testing.B) {
			val := bytes.Repeat([]byte("x"), vs.size)
			raw := []byte(fmt.Sprintf("set mykey 0 0 %d\r\n%s\r\n", vs.size, val))
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				r := bufio.NewReader(bytes.NewReader(raw))
				_, _ = protocol.Parse(r)
			}
		})
	}
}

func BenchmarkParse_Gets(b *testing.B) {
	raw := []byte("gets key1 key2 key3\r\n")
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		r := bufio.NewReader(bytes.NewReader(raw))
		_, _ = protocol.Parse(r)
	}
}

func BenchmarkMarshal_Get(b *testing.B) {
	cmd := protocol.Command{
		Type: protocol.CmdGet,
		Keys: []string{"mykey"},
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		_ = protocol.Marshal(cmd)
	}
}

func BenchmarkMarshal_Set(b *testing.B) {
	sizes := []struct {
		name string
		size int
	}{
		{"64B", 64},
		{"1KB", 1024},
	}
	for _, vs := range sizes {
		b.Run(vs.name, func(b *testing.B) {
			cmd := protocol.Command{
				Type: protocol.CmdSet,
				Keys: []string{"mykey"},
				Data: []byte(strings.Repeat("x", vs.size)),
			}
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				_ = protocol.Marshal(cmd)
			}
		})
	}
}
