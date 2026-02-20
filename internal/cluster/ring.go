package cluster

import (
	"fmt"
	"hash/fnv"
	"sort"
	"sync"
)

// NodeInfo identifies a node in the cluster.
type NodeInfo struct {
	Name string // e.g. "tinycache-0"
	Addr string // internal address, e.g. "tinycache-0.tinycache.default.svc.cluster.local:11311"
}

type vnode struct {
	hash     uint32
	nodeName string
}

// Ring implements a consistent hash ring with virtual nodes.
type Ring struct {
	mu        sync.RWMutex
	vnodes    []vnode
	nodes     map[string]NodeInfo
	numVnodes int
	localName string
}

// NewRing creates a new consistent hash ring with the given number of virtual nodes per physical node.
func NewRing(vnodesPerNode int) *Ring {
	return &Ring{
		nodes:     make(map[string]NodeInfo),
		numVnodes: vnodesPerNode,
	}
}

// AddNode adds a physical node to the ring with its virtual nodes.
func (r *Ring) AddNode(node NodeInfo) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.nodes[node.Name] = node
	for i := range r.numVnodes {
		h := hashKey(fmt.Sprintf("%s-vnode-%d", node.Name, i))
		r.vnodes = append(r.vnodes, vnode{hash: h, nodeName: node.Name})
	}
	sort.Slice(r.vnodes, func(i, j int) bool {
		return r.vnodes[i].hash < r.vnodes[j].hash
	})
}

// RemoveNode removes a physical node and all its virtual nodes from the ring.
func (r *Ring) RemoveNode(name string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	delete(r.nodes, name)
	filtered := r.vnodes[:0]
	for _, vn := range r.vnodes {
		if vn.nodeName != name {
			filtered = append(filtered, vn)
		}
	}
	r.vnodes = filtered
}

// SetLocal marks a node name as the local node for IsLocal checks.
func (r *Ring) SetLocal(name string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.localName = name
}

// LocalNode returns the NodeInfo for the local node.
func (r *Ring) LocalNode() NodeInfo {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.nodes[r.localName]
}

// GetNodes returns up to count distinct physical nodes responsible for the given key.
// The first node is the primary; subsequent nodes are replicas in clockwise ring order.
func (r *Ring) GetNodes(key string, count int) []NodeInfo {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.vnodes) == 0 {
		return nil
	}

	h := hashKey(key)
	idx := sort.Search(len(r.vnodes), func(i int) bool {
		return r.vnodes[i].hash >= h
	})
	if idx >= len(r.vnodes) {
		idx = 0
	}

	if count > len(r.nodes) {
		count = len(r.nodes)
	}

	var result []NodeInfo
	seen := make(map[string]bool)

	for i := range len(r.vnodes) {
		if len(result) >= count {
			break
		}
		vn := r.vnodes[(idx+i)%len(r.vnodes)]
		if seen[vn.nodeName] {
			continue
		}
		seen[vn.nodeName] = true
		result = append(result, r.nodes[vn.nodeName])
	}
	return result
}

// IsLocal returns true if the given key's primary node is the local node.
func (r *Ring) IsLocal(key string) bool {
	nodes := r.GetNodes(key, 1)
	if len(nodes) == 0 {
		return false
	}
	return nodes[0].Name == r.localName
}

func hashKey(key string) uint32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return h.Sum32()
}
