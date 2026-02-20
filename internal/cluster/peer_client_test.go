package cluster_test

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"
	"tinycache/internal/cache"
	"tinycache/internal/cluster"
	"tinycache/internal/server"
)

func startPeerServer(t *testing.T) (string, context.CancelFunc) {
	t.Helper()
	store := cache.NewStore(256, 0)
	handler := server.NewHandler(store)
	srv := server.NewTCPServer(handler, server.TCPConfig{
		MaxConnections: 100,
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
	go func() {
		_ = srv.ListenAndServe(ctx, addr)
	}()
	time.Sleep(50 * time.Millisecond)

	return addr, cancel
}

func TestTCPPeerClient_Replicate(t *testing.T) {
	t.Parallel()
	addr, cancel := startPeerServer(t)
	defer cancel()

	client := cluster.NewTCPPeerClient(time.Second, time.Second, time.Second)

	node := cluster.NodeInfo{Name: "test-node", Addr: addr}
	entry := &cache.Entry{
		Value: []byte("hello"),
		Flags: 42,
	}

	err := client.Replicate(context.Background(), node, "mykey", entry)
	if err != nil {
		t.Fatalf("replicate: %v", err)
	}

	conn, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = conn.Close() }()

	fmt.Fprintf(conn, "get mykey\r\n")
	r := bufio.NewReader(conn)
	var lines []string
	for {
		line, err := r.ReadString('\n')
		if err != nil {
			break
		}
		lines = append(lines, line)
		if strings.HasPrefix(line, "END") {
			break
		}
	}
	resp := strings.Join(lines, "")
	if !strings.Contains(resp, "VALUE mykey 42 5") {
		t.Errorf("expected replicated value, got %q", resp)
	}
}

func TestTCPPeerClient_ReadFrom(t *testing.T) {
	t.Parallel()
	addr, cancel := startPeerServer(t)
	defer cancel()

	conn, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	fmt.Fprintf(conn, "set rkey 7 0 3\r\nval\r\n")
	r := bufio.NewReader(conn)
	_, _ = r.ReadString('\n')
	_ = conn.Close()

	client := cluster.NewTCPPeerClient(time.Second, time.Second, time.Second)
	node := cluster.NodeInfo{Name: "test-node", Addr: addr}

	entry, err := client.ReadFrom(context.Background(), node, "rkey")
	if err != nil {
		t.Fatalf("readFrom: %v", err)
	}
	if entry == nil {
		t.Fatal("expected non-nil entry")
	}
	if string(entry.Value) != "val" {
		t.Errorf("expected 'val', got %q", entry.Value)
	}
	if entry.Flags != 7 {
		t.Errorf("expected flags 7, got %d", entry.Flags)
	}
}

func TestTCPPeerClient_ReadFrom_Miss(t *testing.T) {
	t.Parallel()
	addr, cancel := startPeerServer(t)
	defer cancel()

	client := cluster.NewTCPPeerClient(time.Second, time.Second, time.Second)
	node := cluster.NodeInfo{Name: "test-node", Addr: addr}

	entry, err := client.ReadFrom(context.Background(), node, "nonexistent")
	if err != nil {
		t.Fatalf("readFrom: %v", err)
	}
	if entry != nil {
		t.Error("expected nil entry for missing key")
	}
}

func TestTCPPeerClient_DeleteFrom(t *testing.T) {
	t.Parallel()
	addr, cancel := startPeerServer(t)
	defer cancel()

	conn, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	fmt.Fprintf(conn, "set delme 0 0 1\r\nx\r\n")
	r := bufio.NewReader(conn)
	_, _ = r.ReadString('\n')
	_ = conn.Close()

	client := cluster.NewTCPPeerClient(time.Second, time.Second, time.Second)
	node := cluster.NodeInfo{Name: "test-node", Addr: addr}

	err = client.DeleteFrom(context.Background(), node, "delme")
	if err != nil {
		t.Fatalf("deleteFrom: %v", err)
	}

	conn2, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = conn2.Close() }()

	fmt.Fprintf(conn2, "get delme\r\n")
	r2 := bufio.NewReader(conn2)
	line, _ := r2.ReadString('\n')
	if strings.TrimSpace(line) != "END" {
		t.Errorf("expected END (key deleted), got %q", line)
	}
}
