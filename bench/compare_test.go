//go:build bench_compare

package bench_test

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

var targets = []struct {
	name string
	env  string
	def  string
}{
	{"tinycache", "TINYCACHE_ADDR", "localhost:11211"},
	{"memcached", "MEMCACHED_ADDR", "localhost:11212"},
}

type mcConn struct {
	conn net.Conn
	r    *bufio.Reader
	w    *bufio.Writer
}

func dial(addr string) *mcConn {
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		panic(fmt.Sprintf("dial %s: %v", addr, err))
	}
	if tc, ok := conn.(*net.TCPConn); ok {
		_ = tc.SetNoDelay(true)
	}
	return &mcConn{
		conn: conn,
		r:    bufio.NewReaderSize(conn, 65536),
		w:    bufio.NewWriterSize(conn, 65536),
	}
}

func (c *mcConn) close() { _ = c.conn.Close() }

func (c *mcConn) set(key string, val []byte) {
	fmt.Fprintf(c.w, "set %s 0 0 %d\r\n", key, len(val))
	c.w.Write(val)
	c.w.WriteString("\r\n")
	c.w.Flush()
	line, _ := c.r.ReadString('\n')
	if !strings.HasPrefix(line, "STORED") {
		panic(fmt.Sprintf("set %s: unexpected: %q", key, line))
	}
}

func (c *mcConn) get(key string) {
	fmt.Fprintf(c.w, "get %s\r\n", key)
	c.w.Flush()
	for {
		line, err := c.r.ReadString('\n')
		if err != nil {
			return
		}
		line = strings.TrimRight(line, "\r\n")
		if line == "END" {
			return
		}
		if strings.HasPrefix(line, "VALUE") {
			fields := strings.Fields(line)
			if len(fields) < 4 {
				return
			}
			n, _ := strconv.Atoi(fields[3])
			_, _ = io.ReadFull(c.r, make([]byte, n+2))
		}
	}
}

func BenchmarkCompare_Set(b *testing.B) {
	sizes := []struct {
		name string
		size int
	}{
		{"64B", 64},
		{"1KB", 1024},
	}
	for _, tgt := range targets {
		for _, vs := range sizes {
			b.Run(fmt.Sprintf("%s/%s", tgt.name, vs.name), func(b *testing.B) {
				addr := envOr(tgt.env, tgt.def)
				c := dial(addr)
				defer c.close()
				val := bytes.Repeat([]byte("v"), vs.size)
				b.ReportAllocs()
				b.ResetTimer()
				for i := range b.N {
					c.set(fmt.Sprintf("k:%d", i), val)
				}
			})
		}
	}
}

func BenchmarkCompare_Get(b *testing.B) {
	sizes := []struct {
		name string
		size int
	}{
		{"64B", 64},
		{"1KB", 1024},
	}
	for _, tgt := range targets {
		for _, vs := range sizes {
			b.Run(fmt.Sprintf("%s/%s", tgt.name, vs.name), func(b *testing.B) {
				addr := envOr(tgt.env, tgt.def)
				c := dial(addr)
				defer c.close()
				val := bytes.Repeat([]byte("v"), vs.size)
				const preload = 1000
				for i := range preload {
					c.set(fmt.Sprintf("g:%d", i), val)
				}
				b.ReportAllocs()
				b.ResetTimer()
				for i := range b.N {
					c.get(fmt.Sprintf("g:%d", i%preload))
				}
			})
		}
	}
}

func BenchmarkCompare_Mixed(b *testing.B) {
	for _, tgt := range targets {
		b.Run(tgt.name, func(b *testing.B) {
			addr := envOr(tgt.env, tgt.def)
			c := dial(addr)
			defer c.close()
			val := bytes.Repeat([]byte("v"), 64)
			const preload = 1000
			for i := range preload {
				c.set(fmt.Sprintf("m:%d", i), val)
			}
			rng := rand.New(rand.NewPCG(42, 0))
			b.ReportAllocs()
			b.ResetTimer()
			for i := range b.N {
				key := fmt.Sprintf("m:%d", i%preload)
				if rng.IntN(100) < 80 {
					c.get(key)
				} else {
					c.set(key, val)
				}
			}
		})
	}
}

func BenchmarkCompare_ParallelSet(b *testing.B) {
	for _, tgt := range targets {
		b.Run(tgt.name, func(b *testing.B) {
			addr := envOr(tgt.env, tgt.def)
			val := bytes.Repeat([]byte("v"), 64)
			var counter atomic.Int64
			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				c := dial(addr)
				defer c.close()
				for pb.Next() {
					i := counter.Add(1)
					c.set(fmt.Sprintf("ps:%d", i), val)
				}
			})
		})
	}
}
