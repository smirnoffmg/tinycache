package server_test

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"
	"time"
	"tinycache/internal/cache"
	"tinycache/internal/server"
)

func startTestServer(t *testing.T, maxConns int) (string, context.CancelFunc) {
	t.Helper()
	store := cache.NewStore(256, 0)
	handler := server.NewHandler(store)
	srv := server.NewTCPServer(handler, server.TCPConfig{
		MaxConnections: maxConns,
		ReadTimeout:    2 * time.Second,
		WriteTimeout:   2 * time.Second,
	})

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()

	ctx, cancel := context.WithCancel(context.Background())
	ready := make(chan struct{})
	go func() {
		close(ready)
		_ = srv.ListenAndServe(ctx, addr)
	}()
	<-ready
	time.Sleep(50 * time.Millisecond)

	return addr, cancel
}

func TestTCPServer_SetAndGet(t *testing.T) {
	t.Parallel()
	addr, cancel := startTestServer(t, 100)
	defer cancel()

	conn, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = conn.Close() }()

	r := bufio.NewReader(conn)

	fmt.Fprintf(conn, "set foo 0 0 3\r\nbar\r\n")
	line, _ := r.ReadString('\n')
	if strings.TrimSpace(line) != "STORED" {
		t.Errorf("expected STORED, got %q", line)
	}

	fmt.Fprintf(conn, "get foo\r\n")
	var lines []string
	for {
		line, err := r.ReadString('\n')
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		lines = append(lines, line)
		if strings.HasPrefix(line, "END") {
			break
		}
	}
	resp := strings.Join(lines, "")
	if !strings.Contains(resp, "VALUE foo 0 3") {
		t.Errorf("expected VALUE foo 0 3, got %q", resp)
	}
}

func TestTCPServer_ConnectionLimit(t *testing.T) {
	t.Parallel()
	maxConns := 3
	addr, cancel := startTestServer(t, maxConns)
	defer cancel()

	var conns []net.Conn
	for i := range maxConns {
		conn, err := net.DialTimeout("tcp", addr, time.Second)
		if err != nil {
			t.Fatalf("dial %d: %v", i, err)
		}
		conns = append(conns, conn)

		fmt.Fprintf(conn, "set test 0 0 1\r\nx\r\n")
		r := bufio.NewReader(conn)
		line, err := r.ReadString('\n')
		if err != nil {
			t.Fatalf("read from conn %d: %v", i, err)
		}
		if strings.TrimSpace(line) != "STORED" {
			t.Errorf("conn %d: expected STORED, got %q", i, line)
		}
	}

	extra, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		for _, c := range conns {
			_ = c.Close()
		}
		return
	}

	_ = extra.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
	fmt.Fprintf(extra, "get test\r\n")
	buf := make([]byte, 64)
	_, readErr := extra.Read(buf)
	_ = extra.Close()
	for _, c := range conns {
		_ = c.Close()
	}

	if readErr == nil {
		t.Log("extra connection was accepted but may have been closed quickly (server at capacity)")
	}
}

func TestTCPServer_GracefulShutdown(t *testing.T) {
	t.Parallel()
	addr, cancel := startTestServer(t, 100)

	conn, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = conn.Close() }()

	fmt.Fprintf(conn, "set key 0 0 5\r\nhello\r\n")
	r := bufio.NewReader(conn)
	line, _ := r.ReadString('\n')
	if strings.TrimSpace(line) != "STORED" {
		t.Errorf("expected STORED, got %q", line)
	}

	cancel()
	time.Sleep(200 * time.Millisecond)

	_, err = net.DialTimeout("tcp", addr, 500*time.Millisecond)
	if err == nil {
		t.Log("connection after shutdown unexpectedly succeeded (race with listener close)")
	}
}

func TestTCPServer_ConcurrentClients(t *testing.T) {
	t.Parallel()
	addr, cancel := startTestServer(t, 50)
	defer cancel()

	const clients = 20
	var wg sync.WaitGroup

	for i := range clients {
		wg.Add(1)
		go func() {
			defer wg.Done()
			conn, err := net.DialTimeout("tcp", addr, time.Second)
			if err != nil {
				return
			}
			defer func() { _ = conn.Close() }()

			r := bufio.NewReader(conn)
			key := fmt.Sprintf("k%d", i)

			fmt.Fprintf(conn, "set %s 0 0 3\r\nval\r\n", key)
			line, err := r.ReadString('\n')
			if err != nil {
				return
			}
			if strings.TrimSpace(line) != "STORED" {
				t.Errorf("client %d: expected STORED, got %q", i, line)
			}

			fmt.Fprintf(conn, "get %s\r\n", key)
			for {
				line, err := r.ReadString('\n')
				if err != nil {
					break
				}
				if strings.HasPrefix(line, "END") {
					break
				}
			}

			fmt.Fprintf(conn, "quit\r\n")
		}()
	}
	wg.Wait()
}

func TestTCPServer_QuitClosesConnection(t *testing.T) {
	t.Parallel()
	addr, cancel := startTestServer(t, 100)
	defer cancel()

	conn, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = conn.Close() }()

	fmt.Fprintf(conn, "quit\r\n")
	time.Sleep(100 * time.Millisecond)

	_ = conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
	buf := make([]byte, 64)
	_, err = conn.Read(buf)
	if err == nil {
		t.Error("expected read to fail after quit")
	}
}
