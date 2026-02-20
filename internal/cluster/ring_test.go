package cluster_test

import (
	"fmt"
	"math"
	"testing"
	"tinycache/internal/cluster"
)

func TestRing_GetNodes_ReturnsDistinctPhysicalNodes(t *testing.T) {
	t.Parallel()

	ring := cluster.NewRing(150)
	ring.AddNode(cluster.NodeInfo{Name: "node-0", Addr: "node-0:11311"})
	ring.AddNode(cluster.NodeInfo{Name: "node-1", Addr: "node-1:11311"})
	ring.AddNode(cluster.NodeInfo{Name: "node-2", Addr: "node-2:11311"})

	nodes := ring.GetNodes("foo", 3)
	if len(nodes) != 3 {
		t.Fatalf("expected 3 nodes, got %d", len(nodes))
	}

	seen := make(map[string]bool)
	for _, n := range nodes {
		if seen[n.Name] {
			t.Errorf("duplicate physical node: %s", n.Name)
		}
		seen[n.Name] = true
	}
}

func TestRing_GetNodes_CapAtAvailableNodes(t *testing.T) {
	t.Parallel()

	ring := cluster.NewRing(150)
	ring.AddNode(cluster.NodeInfo{Name: "node-0", Addr: "node-0:11311"})
	ring.AddNode(cluster.NodeInfo{Name: "node-1", Addr: "node-1:11311"})

	nodes := ring.GetNodes("foo", 5)
	if len(nodes) != 2 {
		t.Errorf("expected 2 nodes (capped), got %d", len(nodes))
	}
}

func TestRing_Distribution_RoughlyEven(t *testing.T) {
	t.Parallel()

	ring := cluster.NewRing(150)
	nodeNames := []string{"node-0", "node-1", "node-2"}
	for _, name := range nodeNames {
		ring.AddNode(cluster.NodeInfo{Name: name, Addr: name + ":11311"})
	}

	counts := make(map[string]int)
	numKeys := 10000
	for i := range numKeys {
		key := fmt.Sprintf("key-%d", i)
		primary := ring.GetNodes(key, 1)
		counts[primary[0].Name]++
	}

	expected := float64(numKeys) / float64(len(nodeNames))
	tolerance := expected * 0.35

	for _, name := range nodeNames {
		count := float64(counts[name])
		if math.Abs(count-expected) > tolerance {
			t.Errorf("node %s: got %d keys, expected ~%.0f (tolerance ±%.0f)", name, counts[name], expected, tolerance)
		}
	}
}

func TestRing_AddNode_RebalancesApproximatelyOneFraction(t *testing.T) {
	t.Parallel()

	ring := cluster.NewRing(150)
	for i := range 3 {
		ring.AddNode(cluster.NodeInfo{Name: fmt.Sprintf("node-%d", i), Addr: fmt.Sprintf("node-%d:11311", i)})
	}

	numKeys := 10000
	before := make(map[string]string)
	for i := range numKeys {
		key := fmt.Sprintf("key-%d", i)
		before[key] = ring.GetNodes(key, 1)[0].Name
	}

	ring.AddNode(cluster.NodeInfo{Name: "node-3", Addr: "node-3:11311"})

	moved := 0
	for i := range numKeys {
		key := fmt.Sprintf("key-%d", i)
		after := ring.GetNodes(key, 1)[0].Name
		if before[key] != after {
			moved++
		}
	}

	movedPct := float64(moved) / float64(numKeys)
	if movedPct > 0.40 {
		t.Errorf("too many keys moved: %.1f%% (expected ~25%%)", movedPct*100)
	}
}

func TestRing_RemoveNode(t *testing.T) {
	t.Parallel()

	ring := cluster.NewRing(150)
	for i := range 3 {
		ring.AddNode(cluster.NodeInfo{Name: fmt.Sprintf("node-%d", i), Addr: fmt.Sprintf("node-%d:11311", i)})
	}

	ring.RemoveNode("node-1")

	nodes := ring.GetNodes("anykey", 3)
	for _, n := range nodes {
		if n.Name == "node-1" {
			t.Error("removed node should not appear in GetNodes")
		}
	}
}

func TestRing_IsLocal(t *testing.T) {
	t.Parallel()

	ring := cluster.NewRing(150)
	ring.AddNode(cluster.NodeInfo{Name: "node-0", Addr: "node-0:11311"})
	ring.AddNode(cluster.NodeInfo{Name: "node-1", Addr: "node-1:11311"})
	ring.SetLocal("node-0")

	localCount := 0
	for i := range 1000 {
		key := fmt.Sprintf("key-%d", i)
		if ring.IsLocal(key) {
			localCount++
		}
	}

	if localCount == 0 {
		t.Error("expected some keys to be local")
	}
	if localCount == 1000 {
		t.Error("expected some keys to be remote")
	}
}

func TestRing_LocalNode(t *testing.T) {
	t.Parallel()

	ring := cluster.NewRing(150)
	ring.AddNode(cluster.NodeInfo{Name: "node-0", Addr: "node-0:11311"})
	ring.SetLocal("node-0")

	local := ring.LocalNode()
	if local.Name != "node-0" {
		t.Errorf("expected local node node-0, got %s", local.Name)
	}
}

func TestRing_EmptyRing(t *testing.T) {
	t.Parallel()

	ring := cluster.NewRing(150)
	nodes := ring.GetNodes("foo", 3)
	if len(nodes) != 0 {
		t.Errorf("expected 0 nodes from empty ring, got %d", len(nodes))
	}
}
