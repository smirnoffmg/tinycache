package server_test

import (
	"bufio"
	"bytes"
	"context"
	"strings"
	"sync"
	"testing"
	"time"
	"tinycache/internal/cache"
	"tinycache/internal/cluster"
	"tinycache/internal/server"
)

type mockForwarder struct {
	responses map[string]string
	forwarded []string
}

func (m *mockForwarder) Forward(node cluster.NodeInfo, data []byte) ([]byte, error) {
	m.forwarded = append(m.forwarded, node.Name+":"+string(data))
	key := node.Name
	if resp, ok := m.responses[key]; ok {
		return []byte(resp), nil
	}
	return []byte("STORED\r\n"), nil
}

type alwaysRemoteRing struct {
	targetNode cluster.NodeInfo
}

func (r *alwaysRemoteRing) IsLocal(_ string) bool { return false }
func (r *alwaysRemoteRing) GetNodes(_ string, _ int) []cluster.NodeInfo {
	return []cluster.NodeInfo{r.targetNode}
}

type alwaysLocalRing struct{}

func (r *alwaysLocalRing) IsLocal(_ string) bool { return true }

func (r *alwaysLocalRing) GetNodes(_ string, _ int) []cluster.NodeInfo { return nil }

func TestHandler_LocalKey_GoesToStore(t *testing.T) {
	t.Parallel()
	store := cache.NewStore(256, 0)
	h := server.NewHandler(store,
		server.WithRing(&alwaysLocalRing{}),
	)

	r := bufio.NewReader(strings.NewReader("set foo 0 0 3\r\nbar\r\n"))
	var buf bytes.Buffer
	w := bufio.NewWriter(&buf)
	h.HandleCommand(r, w)
	_ = w.Flush()

	if buf.String() != "STORED\r\n" {
		t.Errorf("expected STORED, got %q", buf.String())
	}

	e, ok := store.Get("foo")
	if !ok {
		t.Fatal("expected key to be stored locally")
	}
	if string(e.Value) != "bar" {
		t.Errorf("expected 'bar', got %q", e.Value)
	}
}

func TestHandler_RemoteKey_ForwardsToNode(t *testing.T) {
	t.Parallel()
	store := cache.NewStore(256, 0)
	fwd := &mockForwarder{responses: map[string]string{"remote-node": "STORED\r\n"}}
	h := server.NewHandler(store,
		server.WithRing(&alwaysRemoteRing{targetNode: cluster.NodeInfo{Name: "remote-node", Addr: "1.2.3.4:11311"}}),
		server.WithForwarder(fwd),
	)

	r := bufio.NewReader(strings.NewReader("set foo 0 0 3\r\nbar\r\n"))
	var buf bytes.Buffer
	w := bufio.NewWriter(&buf)
	h.HandleCommand(r, w)
	_ = w.Flush()

	if buf.String() != "STORED\r\n" {
		t.Errorf("expected STORED, got %q", buf.String())
	}

	if len(fwd.forwarded) != 1 {
		t.Fatalf("expected 1 forward, got %d", len(fwd.forwarded))
	}
	if !strings.HasPrefix(fwd.forwarded[0], "remote-node:") {
		t.Errorf("unexpected forward target: %q", fwd.forwarded[0])
	}

	_, ok := store.Get("foo")
	if ok {
		t.Error("key should NOT be stored locally when routed to remote")
	}
}

func TestHandler_NilRing_AlwaysLocal(t *testing.T) {
	t.Parallel()
	store := cache.NewStore(256, 0)
	h := server.NewHandler(store)

	r := bufio.NewReader(strings.NewReader("set foo 0 0 3\r\nbar\r\n"))
	var buf bytes.Buffer
	w := bufio.NewWriter(&buf)
	h.HandleCommand(r, w)
	_ = w.Flush()

	if buf.String() != "STORED\r\n" {
		t.Errorf("expected STORED, got %q", buf.String())
	}
}

type mockReplicator struct {
	mu       sync.Mutex
	replicas []string
}

func (m *mockReplicator) ReplicateWrite(
	_ context.Context, key string, _ *cache.Entry, replicas []cluster.NodeInfo,
) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, n := range replicas {
		m.replicas = append(m.replicas, n.Name+":"+key)
	}
	return nil
}

type localRingWithReplicas struct {
	replicas []cluster.NodeInfo
}

func (r *localRingWithReplicas) IsLocal(_ string) bool { return true }
func (r *localRingWithReplicas) GetNodes(_ string, count int) []cluster.NodeInfo {
	if count > len(r.replicas) {
		count = len(r.replicas)
	}
	return r.replicas[:count]
}

func TestHandler_Replication_OnSet(t *testing.T) {
	t.Parallel()
	store := cache.NewStore(256, 0)
	rep := &mockReplicator{}
	ring := &localRingWithReplicas{replicas: []cluster.NodeInfo{
		{Name: "self"},
		{Name: "peer-1"},
		{Name: "peer-2"},
	}}
	h := server.NewHandler(store,
		server.WithRing(ring),
		server.WithReplicator(rep, 3),
	)

	r := bufio.NewReader(strings.NewReader("set foo 0 0 3\r\nbar\r\n"))
	var buf bytes.Buffer
	w := bufio.NewWriter(&buf)
	h.HandleCommand(r, w)
	_ = w.Flush()

	if buf.String() != "STORED\r\n" {
		t.Errorf("expected STORED, got %q", buf.String())
	}

	// Replication is async; give goroutine time to run
	time.Sleep(50 * time.Millisecond)

	rep.mu.Lock()
	defer rep.mu.Unlock()
	if len(rep.replicas) < 1 {
		t.Error("expected at least 1 replication call")
	}
}
