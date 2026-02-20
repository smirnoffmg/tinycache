package replication_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
	"tinycache/internal/cache"
	"tinycache/internal/cluster"
	"tinycache/internal/replication"
)

type mockPeerClient struct {
	mu           sync.Mutex
	replicateErr map[string]error
	replicated   map[string]*cache.Entry
	deleted      map[string]bool
	readEntries  map[string]*cache.Entry
}

func newMockPeerClient() *mockPeerClient {
	return &mockPeerClient{
		replicateErr: make(map[string]error),
		replicated:   make(map[string]*cache.Entry),
		deleted:      make(map[string]bool),
		readEntries:  make(map[string]*cache.Entry),
	}
}

func (m *mockPeerClient) Replicate(_ context.Context, node cluster.NodeInfo, _ string, entry *cache.Entry) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err, ok := m.replicateErr[node.Name]; ok {
		return err
	}
	m.replicated[node.Name] = entry
	return nil
}

func (m *mockPeerClient) DeleteFrom(_ context.Context, node cluster.NodeInfo, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.deleted[node.Name+":"+key] = true
	return nil
}

func (m *mockPeerClient) ReadFrom(_ context.Context, node cluster.NodeInfo, _ string) (*cache.Entry, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if e, ok := m.readEntries[node.Name]; ok {
		return e, nil
	}
	return nil, nil
}

func makeNodes(n int) []cluster.NodeInfo {
	nodes := make([]cluster.NodeInfo, n)
	for i := range n {
		nodes[i] = cluster.NodeInfo{
			Name: nodeNameForIdx(i),
			Addr: nodeNameForIdx(i) + ":11311",
		}
	}
	return nodes
}

func nodeNameForIdx(i int) string {
	return "node-" + string(rune('0'+i))
}

func TestQuorumWrite_Success(t *testing.T) {
	t.Parallel()

	peer := newMockPeerClient()
	nodes := makeNodes(3)
	rep := replication.NewReplicator(peer, 2, 50*time.Millisecond)

	entry := &cache.Entry{Value: []byte("bar"), CasToken: 1}
	err := rep.WriteQuorum(context.Background(), "foo", entry, nodes[1:])
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	peer.mu.Lock()
	defer peer.mu.Unlock()
	if len(peer.replicated) < 1 {
		t.Error("expected at least 1 replica write")
	}
}

func TestQuorumWrite_Failure_NotEnoughACKs(t *testing.T) {
	t.Parallel()

	peer := newMockPeerClient()
	nodes := makeNodes(3)

	peer.replicateErr["node-1"] = errors.New("down")
	peer.replicateErr["node-2"] = errors.New("down")

	rep := replication.NewReplicator(peer, 2, 50*time.Millisecond)

	entry := &cache.Entry{Value: []byte("bar"), CasToken: 1}
	err := rep.WriteQuorum(context.Background(), "foo", entry, nodes[1:])
	if err == nil {
		t.Fatal("expected quorum error when all replicas fail")
	}
}

func TestQuorumWrite_PartialSuccess_MeetsQuorum(t *testing.T) {
	t.Parallel()

	peer := newMockPeerClient()
	nodes := makeNodes(3)

	peer.replicateErr["node-2"] = errors.New("down")

	rep := replication.NewReplicator(peer, 2, 50*time.Millisecond)

	entry := &cache.Entry{Value: []byte("bar"), CasToken: 1}
	err := rep.WriteQuorum(context.Background(), "foo", entry, nodes[1:])
	if err != nil {
		t.Fatalf("expected quorum success with 1 ACK (primary counts as 1): %v", err)
	}
}

func TestReadQuorum_VersionMismatch_ReturnsHighest(t *testing.T) {
	t.Parallel()

	peer := newMockPeerClient()
	nodes := makeNodes(3)

	localEntry := &cache.Entry{Value: []byte("old"), CasToken: 3}
	peer.readEntries["node-1"] = &cache.Entry{Value: []byte("new"), CasToken: 5}

	rep := replication.NewReplicator(peer, 2, 50*time.Millisecond)
	result, err := rep.ReadQuorum(context.Background(), "foo", localEntry, nodes[1:2], 2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if result.CasToken != 5 {
		t.Errorf("expected highest version 5, got %d", result.CasToken)
	}
	if string(result.Value) != "new" {
		t.Errorf("expected value 'new', got %q", result.Value)
	}
}

func TestReadQuorum_AllAgree(t *testing.T) {
	t.Parallel()

	peer := newMockPeerClient()
	nodes := makeNodes(3)

	localEntry := &cache.Entry{Value: []byte("same"), CasToken: 5}
	peer.readEntries["node-1"] = &cache.Entry{Value: []byte("same"), CasToken: 5}

	rep := replication.NewReplicator(peer, 2, 50*time.Millisecond)
	result, err := rep.ReadQuorum(context.Background(), "foo", localEntry, nodes[1:2], 2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if result.CasToken != 5 {
		t.Errorf("expected version 5, got %d", result.CasToken)
	}
}
