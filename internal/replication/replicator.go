package replication

import (
	"context"
	"errors"
	"sync"
	"time"
	"tinycache/internal/cache"
	"tinycache/internal/cluster"
)

// PeerClient defines the interface for communicating with peer nodes.
type PeerClient interface {
	Replicate(ctx context.Context, node cluster.NodeInfo, key string, entry *cache.Entry) error
	DeleteFrom(ctx context.Context, node cluster.NodeInfo, key string) error
	ReadFrom(ctx context.Context, node cluster.NodeInfo, key string) (*cache.Entry, error)
}

// Replicator handles quorum-based writes and read-repair.
type Replicator struct {
	peer         PeerClient
	writeQuorum  int
	quorumTimout time.Duration
}

// NewReplicator creates a Replicator.
// writeQuorum is the number of ACKs needed (including the local/primary node).
func NewReplicator(peer PeerClient, writeQuorum int, quorumTimeout time.Duration) *Replicator {
	return &Replicator{
		peer:         peer,
		writeQuorum:  writeQuorum,
		quorumTimout: quorumTimeout,
	}
}

// WriteQuorum replicates an entry to the given replica nodes and waits for quorum.
// The primary node (caller) counts as 1 ACK, so we need writeQuorum-1 additional ACKs.
func (r *Replicator) WriteQuorum(
	ctx context.Context,
	key string,
	entry *cache.Entry,
	replicas []cluster.NodeInfo,
) error {
	needed := r.writeQuorum - 1
	if needed <= 0 {
		return nil
	}

	ctx, cancel := context.WithTimeout(ctx, r.quorumTimout)
	defer cancel()

	type result struct {
		node cluster.NodeInfo
		err  error
	}

	results := make(chan result, len(replicas))
	for _, node := range replicas {
		go func() {
			err := r.peer.Replicate(ctx, node, key, entry)
			results <- result{node: node, err: err}
		}()
	}

	acks := 0
	var ackedNodes []cluster.NodeInfo
	var failedCount int

	for range len(replicas) {
		select {
		case res := <-results:
			if res.err == nil {
				acks++
				ackedNodes = append(ackedNodes, res.node)
			} else {
				failedCount++
			}
		case <-ctx.Done():
			r.asyncRollback(key, ackedNodes)
			return errors.New("quorum timeout")
		}

		if acks >= needed {
			return nil
		}

		remaining := len(replicas) - acks - failedCount
		if acks+remaining < needed {
			r.asyncRollback(key, ackedNodes)
			return errors.New("quorum not achievable")
		}
	}

	if acks < needed {
		r.asyncRollback(key, ackedNodes)
		return errors.New("quorum not met")
	}
	return nil
}

// ReadQuorum reads from replicas and returns the entry with the highest version.
// If a version mismatch is detected, it triggers async read-repair.
func (r *Replicator) ReadQuorum(
	ctx context.Context,
	key string,
	localEntry *cache.Entry,
	replicas []cluster.NodeInfo,
	readQuorum int,
) (*cache.Entry, error) {
	best := localEntry
	entries := []*cache.Entry{localEntry}
	collected := 1

	ctx, cancel := context.WithTimeout(ctx, r.quorumTimout)
	defer cancel()

	type result struct {
		node  cluster.NodeInfo
		entry *cache.Entry
		err   error
	}

	ch := make(chan result, len(replicas))
	for _, node := range replicas {
		go func() {
			e, err := r.peer.ReadFrom(ctx, node, key)
			ch <- result{node: node, entry: e, err: err}
		}()
	}

	for range len(replicas) {
		if collected >= readQuorum {
			break
		}
		select {
		case res := <-ch:
			if res.err != nil || res.entry == nil {
				continue
			}
			entries = append(entries, res.entry)
			collected++
			if res.entry.CasToken > best.CasToken {
				best = res.entry
			}
		case <-ctx.Done():
			return best, nil
		}
	}

	r.asyncRepair(ctx, key, best, entries, replicas)
	return best, nil
}

func (r *Replicator) asyncRollback(key string, nodes []cluster.NodeInfo) {
	var wg sync.WaitGroup
	for _, node := range nodes {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), r.quorumTimout)
			defer cancel()
			_ = r.peer.DeleteFrom(ctx, node, key)
		}()
	}
}

func (r *Replicator) asyncRepair(
	ctx context.Context,
	key string,
	best *cache.Entry,
	entries []*cache.Entry,
	replicas []cluster.NodeInfo,
) {
	needsRepair := false
	for _, e := range entries {
		if e.CasToken < best.CasToken {
			needsRepair = true
			break
		}
	}
	if !needsRepair {
		return
	}

	for _, node := range replicas {
		go func() {
			repairCtx, cancel := context.WithTimeout(ctx, r.quorumTimout)
			defer cancel()
			_ = r.peer.Replicate(repairCtx, node, key, best)
		}()
	}
}
