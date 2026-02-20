package cluster_test

import (
	"fmt"
	"testing"
	"tinycache/internal/cluster"
)

func buildRing(nodeCount, vnodes int) *cluster.Ring {
	r := cluster.NewRing(vnodes)
	for i := range nodeCount {
		name := fmt.Sprintf("tinycache-%d", i)
		r.AddNode(cluster.NodeInfo{
			Name: name,
			Addr: fmt.Sprintf("%s.tinycache.default.svc.cluster.local:11311", name),
		})
	}
	r.SetLocal("tinycache-0")
	return r
}

func BenchmarkRing_GetNodes(b *testing.B) {
	r := buildRing(3, 150)
	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		r.GetNodes(fmt.Sprintf("key:%d", i), 3)
	}
}

func BenchmarkRing_IsLocal(b *testing.B) {
	r := buildRing(3, 150)
	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		r.IsLocal(fmt.Sprintf("key:%d", i))
	}
}

func BenchmarkRing_GetNodes_LargeCluster(b *testing.B) {
	r := buildRing(10, 150)
	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		r.GetNodes(fmt.Sprintf("key:%d", i), 3)
	}
}
