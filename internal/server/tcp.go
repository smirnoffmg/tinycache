package server

import (
	"bufio"
	"context"
	"log"
	"net"
	"sync"
	"time"
)

// TCPServer listens for Memcached client connections and dispatches commands.
type TCPServer struct {
	handler        *Handler
	listener       net.Listener
	maxConnections int
	readTimeout    time.Duration
	writeTimeout   time.Duration
	sem            chan struct{}
	wg             sync.WaitGroup
}

// TCPConfig holds configuration for the TCP server.
type TCPConfig struct {
	Addr           string
	MaxConnections int
	ReadTimeout    time.Duration
	WriteTimeout   time.Duration
}

// NewTCPServer creates a new TCP server.
func NewTCPServer(handler *Handler, cfg TCPConfig) *TCPServer {
	return &TCPServer{
		handler:        handler,
		maxConnections: cfg.MaxConnections,
		readTimeout:    cfg.ReadTimeout,
		writeTimeout:   cfg.WriteTimeout,
		sem:            make(chan struct{}, cfg.MaxConnections),
	}
}

// ListenAndServe starts accepting TCP connections on the configured address.
// It blocks until the context is cancelled, then gracefully drains connections.
func (s *TCPServer) ListenAndServe(ctx context.Context, addr string) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	s.listener = ln

	go func() {
		<-ctx.Done()
		_ = ln.Close()
	}()

	for {
		conn, err := ln.Accept()
		if err != nil {
			select {
			case <-ctx.Done():
				s.wg.Wait()
				return nil
			default:
				log.Printf("accept error: %v", err)
				continue
			}
		}

		select {
		case s.sem <- struct{}{}:
			s.wg.Add(1)
			go s.handleConn(conn)
		default:
			_ = conn.Close()
		}
	}
}

func (s *TCPServer) handleConn(conn net.Conn) {
	defer func() {
		_ = conn.Close()
		<-s.sem
		s.wg.Done()
	}()

	r := bufio.NewReader(conn)
	w := bufio.NewWriter(conn)

	for {
		if s.readTimeout > 0 {
			_ = conn.SetReadDeadline(time.Now().Add(s.readTimeout))
		}

		keepGoing := s.handler.HandleCommand(r, w)

		if s.writeTimeout > 0 {
			_ = conn.SetWriteDeadline(time.Now().Add(s.writeTimeout))
		}
		_ = w.Flush()

		if !keepGoing {
			return
		}
	}
}
