package server_test

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"tinycache/internal/server"
)

func healthReq(method, path string) *http.Request {
	return httptest.NewRequest(method, path, http.NoBody)
}

func TestHealthz_ReturnsOK(t *testing.T) {
	t.Parallel()
	hs := server.NewHealthServer("127.0.0.1:0")

	rec := httptest.NewRecorder()
	hs.ServeHTTP(rec, healthReq(http.MethodGet, "/healthz"))

	if rec.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rec.Code)
	}
	if rec.Body.String() != "ok" {
		t.Errorf("expected body 'ok', got %q", rec.Body.String())
	}
}

func TestReadyz_NotReady(t *testing.T) {
	t.Parallel()
	hs := server.NewHealthServer("127.0.0.1:0")

	rec := httptest.NewRecorder()
	hs.ServeHTTP(rec, healthReq(http.MethodGet, "/readyz"))

	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("expected 503, got %d", rec.Code)
	}
	if rec.Body.String() != "not ready" {
		t.Errorf("expected body 'not ready', got %q", rec.Body.String())
	}
}

func TestReadyz_Ready(t *testing.T) {
	t.Parallel()
	hs := server.NewHealthServer("127.0.0.1:0")
	hs.SetReady(true)

	rec := httptest.NewRecorder()
	hs.ServeHTTP(rec, healthReq(http.MethodGet, "/readyz"))

	if rec.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rec.Code)
	}
	if rec.Body.String() != "ok" {
		t.Errorf("expected body 'ok', got %q", rec.Body.String())
	}
}

func TestReadyz_ToggleReadiness(t *testing.T) {
	t.Parallel()
	hs := server.NewHealthServer("127.0.0.1:0")

	hs.SetReady(true)
	rec := httptest.NewRecorder()
	hs.ServeHTTP(rec, healthReq(http.MethodGet, "/readyz"))
	if rec.Code != http.StatusOK {
		t.Errorf("expected 200 when ready, got %d", rec.Code)
	}

	hs.SetReady(false)
	rec = httptest.NewRecorder()
	hs.ServeHTTP(rec, healthReq(http.MethodGet, "/readyz"))
	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("expected 503 after unready, got %d", rec.Code)
	}
}

func TestHealthz_PostMethodNotAllowed(t *testing.T) {
	t.Parallel()
	hs := server.NewHealthServer("127.0.0.1:0")

	rec := httptest.NewRecorder()
	hs.ServeHTTP(rec, healthReq(http.MethodPost, "/healthz"))

	if rec.Code == http.StatusOK {
		t.Error("expected non-200 for POST to /healthz")
	}
}
