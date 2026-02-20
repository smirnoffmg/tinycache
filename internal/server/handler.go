package server

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"strconv"
	"tinycache/internal/cache"
	"tinycache/internal/cluster"
	"tinycache/internal/protocol"
)

// KeyRing determines key ownership in the cluster.
type KeyRing interface {
	IsLocal(key string) bool
	GetNodes(key string, count int) []cluster.NodeInfo
}

// Forwarder sends raw commands to peer nodes.
type Forwarder interface {
	Forward(node cluster.NodeInfo, data []byte) ([]byte, error)
}

// WriteReplicator replicates writes to peer nodes.
type WriteReplicator interface {
	ReplicateWrite(ctx context.Context, key string, entry *cache.Entry, replicas []cluster.NodeInfo) error
}

// Handler processes Memcached text protocol commands against a cache.ReadWriter.
type Handler struct {
	store      cache.ReadWriter
	ring       KeyRing
	fwd        Forwarder
	replicator WriteReplicator
	replFactor int
}

// Option configures the Handler.
type Option func(*Handler)

// WithRing injects a key ring for distributed routing.
func WithRing(ring KeyRing) Option {
	return func(h *Handler) { h.ring = ring }
}

// WithForwarder injects a command forwarder for remote keys.
func WithForwarder(fwd Forwarder) Option {
	return func(h *Handler) { h.fwd = fwd }
}

// WithReplicator injects a write replicator and replication factor.
func WithReplicator(rep WriteReplicator, replicationFactor int) Option {
	return func(h *Handler) {
		h.replicator = rep
		h.replFactor = replicationFactor
	}
}

// NewHandler creates a Handler backed by the given cache.ReadWriter.
func NewHandler(store cache.ReadWriter, opts ...Option) *Handler {
	h := &Handler{store: store}
	for _, opt := range opts {
		opt(h)
	}
	return h
}

// HandleCommand reads one command from r, executes it, and writes the response to w.
// Returns false if the connection should be closed (quit or read error).
func (h *Handler) HandleCommand(r *bufio.Reader, w *bufio.Writer) bool {
	cmd, err := protocol.Parse(r)
	if err != nil {
		if err == io.EOF || isConnectionClosed(err) {
			return false
		}
		writeStr(w, protocol.RespError)
		return true
	}

	if h.shouldForward(cmd) {
		h.forwardCommand(w, cmd)
		return true
	}

	return h.dispatch(w, cmd)
}

func (h *Handler) shouldForward(cmd protocol.Command) bool {
	return h.ring != nil && h.fwd != nil && hasSingleKey(cmd) && !h.ring.IsLocal(cmd.Keys[0])
}

func (h *Handler) dispatch(w *bufio.Writer, cmd protocol.Command) bool {
	switch cmd.Type {
	case protocol.CmdGet:
		h.handleGet(w, cmd, false)
	case protocol.CmdGets:
		h.handleGet(w, cmd, true)
	case protocol.CmdSet:
		h.handleSet(w, cmd)
	case protocol.CmdAdd:
		h.handleAdd(w, cmd)
	case protocol.CmdReplace:
		h.handleReplace(w, cmd)
	case protocol.CmdCas:
		h.handleCas(w, cmd)
	case protocol.CmdDelete:
		h.handleDelete(w, cmd)
	case protocol.CmdFlushAll:
		h.handleFlushAll(w, cmd)
	case protocol.CmdStats:
		h.handleStats(w)
	case protocol.CmdQuit:
		return false
	}
	return true
}

func (h *Handler) handleGet(w *bufio.Writer, cmd protocol.Command, includeCas bool) {
	for _, key := range cmd.Keys {
		e, ok := h.store.Get(key)
		if !ok {
			continue
		}
		_ = protocol.WriteValue(w, key, e.Flags, e.Value, e.CasToken, includeCas)
	}
	writeStr(w, protocol.RespEnd)
}

func (h *Handler) handleSet(w *bufio.Writer, cmd protocol.Command) {
	cas := h.store.Set(cmd.Keys[0], cmd.Data, cmd.Flags, cmd.Exptime)
	h.replicateAsync(cmd.Keys[0], cmd.Data, cmd.Flags, cas)
	if !cmd.Noreply {
		writeStr(w, protocol.RespStored)
	}
}

func (h *Handler) handleAdd(w *bufio.Writer, cmd protocol.Command) {
	cas, ok := h.store.Add(cmd.Keys[0], cmd.Data, cmd.Flags, cmd.Exptime)
	if ok {
		h.replicateAsync(cmd.Keys[0], cmd.Data, cmd.Flags, cas)
	}
	if cmd.Noreply {
		return
	}
	if ok {
		writeStr(w, protocol.RespStored)
	} else {
		writeStr(w, protocol.RespNotStored)
	}
}

func (h *Handler) handleReplace(w *bufio.Writer, cmd protocol.Command) {
	cas, ok := h.store.Replace(cmd.Keys[0], cmd.Data, cmd.Flags, cmd.Exptime)
	if ok {
		h.replicateAsync(cmd.Keys[0], cmd.Data, cmd.Flags, cas)
	}
	if cmd.Noreply {
		return
	}
	if ok {
		writeStr(w, protocol.RespStored)
	} else {
		writeStr(w, protocol.RespNotStored)
	}
}

func (h *Handler) handleCas(w *bufio.Writer, cmd protocol.Command) {
	result := h.store.Cas(cmd.Keys[0], cmd.Data, cmd.Flags, cmd.Exptime, cmd.CasUnique)
	if cmd.Noreply {
		return
	}
	switch result {
	case cache.CasStored:
		writeStr(w, protocol.RespStored)
	case cache.CasExists:
		writeStr(w, protocol.RespExists)
	case cache.CasNotFound:
		writeStr(w, protocol.RespNotFound)
	}
}

func (h *Handler) handleDelete(w *bufio.Writer, cmd protocol.Command) {
	ok := h.store.Delete(cmd.Keys[0])
	if cmd.Noreply {
		return
	}
	if ok {
		writeStr(w, protocol.RespDeleted)
	} else {
		writeStr(w, protocol.RespNotFound)
	}
}

func (h *Handler) handleFlushAll(w *bufio.Writer, cmd protocol.Command) {
	h.store.FlushAll()
	if !cmd.Noreply {
		writeStr(w, protocol.RespOK)
	}
}

func (h *Handler) handleStats(w *bufio.Writer) {
	stats := h.store.Stats()
	_ = protocol.WriteStat(w, "curr_items", strconv.Itoa(stats.Items))
	_ = protocol.WriteStat(w, "bytes", fmt.Sprintf("%d", stats.MemoryBytes))
	writeStr(w, protocol.RespEnd)
}

func writeStr(w *bufio.Writer, s string) {
	_, _ = w.WriteString(s)
}

func (h *Handler) replicateAsync(key string, value []byte, flags uint32, cas uint64) {
	if h.replicator == nil || h.ring == nil {
		return
	}
	nodes := h.ring.GetNodes(key, h.replFactor)
	if len(nodes) <= 1 {
		return
	}
	entry := &cache.Entry{
		Value:    value,
		Flags:    flags,
		CasToken: cas,
	}
	go func() {
		_ = h.replicator.ReplicateWrite(context.Background(), key, entry, nodes[1:])
	}()
}

func hasSingleKey(cmd protocol.Command) bool {
	switch cmd.Type {
	case protocol.CmdSet, protocol.CmdAdd, protocol.CmdReplace, protocol.CmdCas, protocol.CmdDelete:
		return len(cmd.Keys) == 1
	case protocol.CmdGet, protocol.CmdGets:
		return len(cmd.Keys) == 1
	}
	return false
}

func (h *Handler) forwardCommand(w *bufio.Writer, cmd protocol.Command) {
	nodes := h.ring.GetNodes(cmd.Keys[0], 1)
	if len(nodes) == 0 {
		writeStr(w, protocol.RespError)
		return
	}

	raw := protocol.Marshal(cmd)
	resp, err := h.fwd.Forward(nodes[0], raw)
	if err != nil {
		_ = protocol.WriteServerError(w, "forward failed")
		return
	}
	_, _ = w.Write(resp)
}

func isConnectionClosed(err error) bool {
	return err.Error() == "use of closed network connection"
}
