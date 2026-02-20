package cluster_test

import (
	"testing"
	"tinycache/internal/cluster"
)

func TestParseOrdinal_ValidPodName(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		podName string
		want    int
	}{
		{"zero", "tinycache-0", 0},
		{"one", "tinycache-1", 1},
		{"large", "tinycache-99", 99},
		{"different-prefix", "mycache-5", 5},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := cluster.ParseOrdinal(tt.podName)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.want {
				t.Errorf("got %d, want %d", got, tt.want)
			}
		})
	}
}

func TestParseOrdinal_InvalidPodName(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		podName string
	}{
		{"no-dash", "tinycache"},
		{"trailing-dash", "tinycache-"},
		{"non-numeric", "tinycache-abc"},
		{"empty", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, err := cluster.ParseOrdinal(tt.podName)
			if err == nil {
				t.Error("expected error for invalid pod name")
			}
		})
	}
}

func TestBuildPeerList_ThreeReplicas(t *testing.T) {
	t.Parallel()
	peers := cluster.BuildPeerList(3, "tinycache", "default")

	if len(peers) != 3 {
		t.Fatalf("expected 3 peers, got %d", len(peers))
	}

	wantNames := []string{"tinycache-0", "tinycache-1", "tinycache-2"}
	for i, name := range wantNames {
		if peers[i].Name != name {
			t.Errorf("peer[%d].Name = %q, want %q", i, peers[i].Name, name)
		}
	}

	wantAddr := "tinycache-0.tinycache.default.svc.cluster.local:11311"
	if peers[0].Addr != wantAddr {
		t.Errorf("peer[0].Addr = %q, want %q", peers[0].Addr, wantAddr)
	}
}

func TestBuildPeerList_CustomNamespace(t *testing.T) {
	t.Parallel()
	peers := cluster.BuildPeerList(2, "mycache", "production")

	if len(peers) != 2 {
		t.Fatalf("expected 2 peers, got %d", len(peers))
	}

	wantAddr := "mycache-1.mycache.production.svc.cluster.local:11311"
	if peers[1].Addr != wantAddr {
		t.Errorf("peer[1].Addr = %q, want %q", peers[1].Addr, wantAddr)
	}
}

func TestBuildPeerList_ZeroReplicas(t *testing.T) {
	t.Parallel()
	peers := cluster.BuildPeerList(0, "tinycache", "default")

	if len(peers) != 0 {
		t.Errorf("expected 0 peers, got %d", len(peers))
	}
}

func TestBuildPeerList_SingleReplica(t *testing.T) {
	t.Parallel()
	peers := cluster.BuildPeerList(1, "tinycache", "default")

	if len(peers) != 1 {
		t.Fatalf("expected 1 peer, got %d", len(peers))
	}
	if peers[0].Name != "tinycache-0" {
		t.Errorf("got name %q, want tinycache-0", peers[0].Name)
	}
}
