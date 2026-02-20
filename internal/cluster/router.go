package cluster

import (
	"bufio"
	"fmt"
	"net"
	"strings"
	"time"
)

// Router routes keys to nodes and forwards commands to peer nodes.
type Router struct {
	ring         *Ring
	dialTimeout  time.Duration
	readTimeout  time.Duration
	writeTimeout time.Duration
}

// RouterConfig configures the Router.
type RouterConfig struct {
	DialTimeout  time.Duration
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
}

// NewRouter creates a new Router.
func NewRouter(ring *Ring, cfg RouterConfig) *Router {
	return &Router{
		ring:         ring,
		dialTimeout:  cfg.DialTimeout,
		readTimeout:  cfg.ReadTimeout,
		writeTimeout: cfg.WriteTimeout,
	}
}

// Route returns the primary node and whether the key is local.
func (rt *Router) Route(key string) (NodeInfo, bool) {
	nodes := rt.ring.GetNodes(key, 1)
	if len(nodes) == 0 {
		return NodeInfo{}, true
	}
	return nodes[0], rt.ring.IsLocal(key)
}

// Forward sends raw command bytes to a peer node and returns the full response.
// Each call opens a fresh connection to avoid stale buffer issues.
func (rt *Router) Forward(node NodeInfo, data []byte) ([]byte, error) {
	conn, err := net.DialTimeout("tcp", node.Addr, rt.dialTimeout)
	if err != nil {
		return nil, fmt.Errorf("connect to %s: %w", node.Name, err)
	}
	defer func() { _ = conn.Close() }()

	if rt.writeTimeout > 0 {
		_ = conn.SetWriteDeadline(time.Now().Add(rt.writeTimeout))
	}
	if _, err := conn.Write(data); err != nil {
		return nil, fmt.Errorf("write to %s: %w", node.Name, err)
	}

	if rt.readTimeout > 0 {
		_ = conn.SetReadDeadline(time.Now().Add(rt.readTimeout))
	}
	reader := bufio.NewReader(conn)
	return readFullResponse(reader, node.Name)
}

// Close is a no-op; kept for interface compatibility.
func (rt *Router) Close() {}

func readFullResponse(r *bufio.Reader, nodeName string) ([]byte, error) {
	var resp []byte
	for {
		line, err := r.ReadBytes('\n')
		if err != nil {
			return resp, fmt.Errorf("read from %s: %w", nodeName, err)
		}
		resp = append(resp, line...)
		if isTerminalLine(line) {
			return resp, nil
		}
	}
}

func isTerminalLine(line []byte) bool {
	s := strings.TrimRight(string(line), "\r\n")
	switch {
	case s == "STORED", s == "NOT_STORED", s == "EXISTS",
		s == "NOT_FOUND", s == "DELETED", s == "ERROR",
		s == "OK", s == "END":
		return true
	case strings.HasPrefix(s, "SERVER_ERROR"), strings.HasPrefix(s, "CLIENT_ERROR"):
		return true
	}
	return false
}
