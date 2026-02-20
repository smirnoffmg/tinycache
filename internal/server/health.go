package server

import (
	"context"
	"net/http"
	"sync/atomic"
	"time"
)

// HealthServer serves /healthz and /readyz endpoints.
type HealthServer struct {
	ready  atomic.Bool
	server *http.Server
}

// NewHealthServer creates a health server listening on the given address.
func NewHealthServer(addr string) *HealthServer {
	h := &HealthServer{}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, _ *http.Request) {
		if h.ready.Load() {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("ok"))
		} else {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte("not ready"))
		}
	})

	h.server = &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	return h
}

// SetReady marks the node as ready to serve traffic.
func (h *HealthServer) SetReady(ready bool) {
	h.ready.Store(ready)
}

// ListenAndServe starts the health HTTP server.
func (h *HealthServer) ListenAndServe() error {
	return h.server.ListenAndServe()
}

// ServeHTTP implements http.Handler, allowing direct testing without a network listener.
func (h *HealthServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.server.Handler.ServeHTTP(w, r)
}

// Shutdown gracefully shuts down the health server.
func (h *HealthServer) Shutdown(ctx context.Context) error {
	return h.server.Shutdown(ctx)
}
