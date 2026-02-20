package cluster

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"
	"tinycache/internal/cache"
)

// TCPPeerClient communicates with peer nodes using the Memcached text protocol
// over TCP. This reuses the same protocol the client-facing server speaks.
type TCPPeerClient struct {
	dialTimeout  time.Duration
	readTimeout  time.Duration
	writeTimeout time.Duration
}

// NewTCPPeerClient creates a new TCP-based peer client.
func NewTCPPeerClient(dialTimeout, readTimeout, writeTimeout time.Duration) *TCPPeerClient {
	return &TCPPeerClient{
		dialTimeout:  dialTimeout,
		readTimeout:  readTimeout,
		writeTimeout: writeTimeout,
	}
}

// Replicate sends a SET command to the peer to store the entry.
func (c *TCPPeerClient) Replicate(_ context.Context, node NodeInfo, key string, entry *cache.Entry) error {
	conn, err := net.DialTimeout("tcp", node.Addr, c.dialTimeout)
	if err != nil {
		return fmt.Errorf("dial %s: %w", node.Name, err)
	}
	defer func() { _ = conn.Close() }()

	if c.writeTimeout > 0 {
		_ = conn.SetWriteDeadline(time.Now().Add(c.writeTimeout))
	}

	cmd := fmt.Sprintf("set %s %d 0 %d\r\n", key, entry.Flags, len(entry.Value))
	if _, err := conn.Write([]byte(cmd)); err != nil {
		return fmt.Errorf("write cmd to %s: %w", node.Name, err)
	}
	if _, err := conn.Write(entry.Value); err != nil {
		return fmt.Errorf("write data to %s: %w", node.Name, err)
	}
	if _, err := conn.Write([]byte("\r\n")); err != nil {
		return fmt.Errorf("write terminator to %s: %w", node.Name, err)
	}

	if c.readTimeout > 0 {
		_ = conn.SetReadDeadline(time.Now().Add(c.readTimeout))
	}

	r := bufio.NewReader(conn)
	line, err := r.ReadString('\n')
	if err != nil {
		return fmt.Errorf("read response from %s: %w", node.Name, err)
	}
	if strings.TrimSpace(line) != "STORED" {
		return fmt.Errorf("replicate to %s: unexpected response %q", node.Name, line)
	}
	return nil
}

// DeleteFrom sends a DELETE command to the peer.
func (c *TCPPeerClient) DeleteFrom(_ context.Context, node NodeInfo, key string) error {
	conn, err := net.DialTimeout("tcp", node.Addr, c.dialTimeout)
	if err != nil {
		return fmt.Errorf("dial %s: %w", node.Name, err)
	}
	defer func() { _ = conn.Close() }()

	if c.writeTimeout > 0 {
		_ = conn.SetWriteDeadline(time.Now().Add(c.writeTimeout))
	}

	cmd := fmt.Sprintf("delete %s\r\n", key)
	if _, err := conn.Write([]byte(cmd)); err != nil {
		return fmt.Errorf("write to %s: %w", node.Name, err)
	}

	if c.readTimeout > 0 {
		_ = conn.SetReadDeadline(time.Now().Add(c.readTimeout))
	}

	r := bufio.NewReader(conn)
	_, err = r.ReadString('\n')
	return err
}

// ReadFrom sends a GETS command and returns the entry, or nil if not found.
func (c *TCPPeerClient) ReadFrom(_ context.Context, node NodeInfo, key string) (*cache.Entry, error) {
	conn, err := net.DialTimeout("tcp", node.Addr, c.dialTimeout)
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", node.Name, err)
	}
	defer func() { _ = conn.Close() }()

	if c.writeTimeout > 0 {
		_ = conn.SetWriteDeadline(time.Now().Add(c.writeTimeout))
	}

	cmd := fmt.Sprintf("gets %s\r\n", key)
	if _, err := conn.Write([]byte(cmd)); err != nil {
		return nil, fmt.Errorf("write to %s: %w", node.Name, err)
	}

	if c.readTimeout > 0 {
		_ = conn.SetReadDeadline(time.Now().Add(c.readTimeout))
	}

	r := bufio.NewReader(conn)
	line, err := r.ReadString('\n')
	if err != nil {
		return nil, fmt.Errorf("read from %s: %w", node.Name, err)
	}

	line = strings.TrimRight(line, "\r\n")
	if line == "END" {
		return nil, nil
	}

	// VALUE <key> <flags> <bytes> <cas>\r\n
	parts := strings.Fields(line)
	if len(parts) < 5 || parts[0] != "VALUE" {
		return nil, fmt.Errorf("unexpected response from %s: %q", node.Name, line)
	}

	flags, err := strconv.ParseUint(parts[2], 10, 32)
	if err != nil {
		return nil, fmt.Errorf("parse flags from %s: %w", node.Name, err)
	}
	byteCount, err := strconv.Atoi(parts[3])
	if err != nil {
		return nil, fmt.Errorf("parse bytes from %s: %w", node.Name, err)
	}
	casToken, err := strconv.ParseUint(parts[4], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("parse cas from %s: %w", node.Name, err)
	}

	// Read data + \r\n
	data := make([]byte, byteCount+2)
	n := 0
	for n < len(data) {
		nn, err := r.Read(data[n:])
		if err != nil {
			return nil, fmt.Errorf("read data from %s: %w", node.Name, err)
		}
		n += nn
	}

	// Read END\r\n
	_, _ = r.ReadString('\n')

	return &cache.Entry{
		Value:    data[:byteCount],
		Flags:    uint32(flags), //nolint:gosec // bounded by ParseUint 32-bit
		CasToken: casToken,
	}, nil
}
