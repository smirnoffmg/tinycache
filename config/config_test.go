package config_test

import (
	"testing"
	"tinycache/config"
)

func noEnv(string) string { return "" }

func makeEnv(overrides map[string]string) func(string) string {
	return func(key string) string {
		return overrides[key]
	}
}

func assertEqual[T comparable](t *testing.T, got, want T) {
	t.Helper()
	if got != want {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestLoad_Defaults(t *testing.T) {
	t.Parallel()

	cfg := config.Load(noEnv)

	assertEqual(t, cfg.Node.Addr, "0.0.0.0:11211")
	assertEqual(t, cfg.Node.InternalAddr, "0.0.0.0:11311")

	assertEqual(t, cfg.Cluster.PodName, "")
	assertEqual(t, cfg.Cluster.Namespace, "default")
	assertEqual(t, cfg.Cluster.Replicas, 3)
	assertEqual(t, cfg.Cluster.ServiceName, "tinycache")
	assertEqual(t, cfg.Cluster.ReplicationFactor, 3)
	assertEqual(t, cfg.Cluster.WriteQuorum, 2)
	assertEqual(t, cfg.Cluster.ReadQuorum, 1)
	assertEqual(t, cfg.Cluster.QuorumTimeoutMs, 50)
	assertEqual(t, cfg.Cluster.VirtualNodes, 150)
	assertEqual(t, cfg.Cluster.RepairEnabled, true)

	assertEqual(t, cfg.Cache.MaxMemoryMB, 256)
	assertEqual(t, cfg.Cache.DefaultTTLSeconds, 0)
	assertEqual(t, cfg.Cache.EvictionIntervalMs, 500)

	assertEqual(t, cfg.Persistence.Enabled, true)
	assertEqual(t, cfg.Persistence.DataDir, "/data")
	assertEqual(t, cfg.Persistence.AOF.Enabled, true)
	assertEqual(t, cfg.Persistence.AOF.Fsync, "everysec")
	assertEqual(t, cfg.Persistence.AOF.MaxSizeMB, 512)
	assertEqual(t, cfg.Persistence.Snapshot.Enabled, true)
	assertEqual(t, cfg.Persistence.Snapshot.IntervalSeconds, 300)
	assertEqual(t, cfg.Persistence.Snapshot.MinChanges, 1000)

	assertEqual(t, cfg.Server.HealthAddr, "0.0.0.0:9090")
	assertEqual(t, cfg.Server.ShutdownTimeoutSeconds, 30)
	assertEqual(t, cfg.Server.MaxConnections, 10000)
	assertEqual(t, cfg.Server.ReadTimeoutMs, 5000)
	assertEqual(t, cfg.Server.WriteTimeoutMs, 5000)
}

func TestLoad_StringOverrides(t *testing.T) {
	t.Parallel()

	cfg := config.Load(makeEnv(map[string]string{
		"TC_ADDR":          "127.0.0.1:5555",
		"TC_INTERNAL_ADDR": "127.0.0.1:6666",
		"TC_SERVICE_NAME":  "mycache",
		"TC_DATA_DIR":      "/mnt/data",
		"TC_AOF_FSYNC":     "always",
		"TC_HEALTH_ADDR":   "0.0.0.0:8080",
	}))

	assertEqual(t, cfg.Node.Addr, "127.0.0.1:5555")
	assertEqual(t, cfg.Node.InternalAddr, "127.0.0.1:6666")
	assertEqual(t, cfg.Cluster.ServiceName, "mycache")
	assertEqual(t, cfg.Persistence.DataDir, "/mnt/data")
	assertEqual(t, cfg.Persistence.AOF.Fsync, "always")
	assertEqual(t, cfg.Server.HealthAddr, "0.0.0.0:8080")
}

func TestLoad_IntOverrides(t *testing.T) {
	t.Parallel()

	cfg := config.Load(makeEnv(map[string]string{
		"TC_CLUSTER_REPLICAS":          "5",
		"TC_REPLICATION_FACTOR":        "5",
		"TC_WRITE_QUORUM":              "3",
		"TC_READ_QUORUM":               "2",
		"TC_QUORUM_TIMEOUT_MS":         "100",
		"TC_VIRTUAL_NODES":             "200",
		"TC_MAX_MEMORY_MB":             "512",
		"TC_DEFAULT_TTL_SECONDS":       "3600",
		"TC_EVICTION_INTERVAL_MS":      "1000",
		"TC_AOF_MAX_SIZE_MB":           "1024",
		"TC_SNAPSHOT_INTERVAL_SECONDS": "600",
		"TC_SNAPSHOT_MIN_CHANGES":      "500",
		"TC_SHUTDOWN_TIMEOUT_SECONDS":  "60",
		"TC_MAX_CONNECTIONS":           "20000",
		"TC_READ_TIMEOUT_MS":           "10000",
		"TC_WRITE_TIMEOUT_MS":          "10000",
	}))

	assertEqual(t, cfg.Cluster.Replicas, 5)
	assertEqual(t, cfg.Cluster.ReplicationFactor, 5)
	assertEqual(t, cfg.Cluster.WriteQuorum, 3)
	assertEqual(t, cfg.Cluster.ReadQuorum, 2)
	assertEqual(t, cfg.Cluster.QuorumTimeoutMs, 100)
	assertEqual(t, cfg.Cluster.VirtualNodes, 200)
	assertEqual(t, cfg.Cache.MaxMemoryMB, 512)
	assertEqual(t, cfg.Cache.DefaultTTLSeconds, 3600)
	assertEqual(t, cfg.Cache.EvictionIntervalMs, 1000)
	assertEqual(t, cfg.Persistence.AOF.MaxSizeMB, 1024)
	assertEqual(t, cfg.Persistence.Snapshot.IntervalSeconds, 600)
	assertEqual(t, cfg.Persistence.Snapshot.MinChanges, 500)
	assertEqual(t, cfg.Server.ShutdownTimeoutSeconds, 60)
	assertEqual(t, cfg.Server.MaxConnections, 20000)
	assertEqual(t, cfg.Server.ReadTimeoutMs, 10000)
	assertEqual(t, cfg.Server.WriteTimeoutMs, 10000)
}

func TestLoad_BoolOverrides(t *testing.T) {
	t.Parallel()

	cfg := config.Load(makeEnv(map[string]string{
		"TC_REPAIR_ENABLED":      "false",
		"TC_PERSISTENCE_ENABLED": "false",
		"TC_AOF_ENABLED":         "false",
		"TC_SNAPSHOT_ENABLED":    "false",
	}))

	assertEqual(t, cfg.Cluster.RepairEnabled, false)
	assertEqual(t, cfg.Persistence.Enabled, false)
	assertEqual(t, cfg.Persistence.AOF.Enabled, false)
	assertEqual(t, cfg.Persistence.Snapshot.Enabled, false)
}

func TestLoad_PodEnvVars(t *testing.T) {
	t.Parallel()

	cfg := config.Load(makeEnv(map[string]string{
		"POD_NAME":      "tinycache-2",
		"POD_NAMESPACE": "production",
	}))

	assertEqual(t, cfg.Cluster.PodName, "tinycache-2")
	assertEqual(t, cfg.Cluster.Namespace, "production")
}

func TestLoad_InvalidIntKeepsDefault(t *testing.T) {
	t.Parallel()

	cfg := config.Load(makeEnv(map[string]string{
		"TC_CLUSTER_REPLICAS": "not-a-number",
		"TC_MAX_MEMORY_MB":    "abc",
	}))

	assertEqual(t, cfg.Cluster.Replicas, 3)
	assertEqual(t, cfg.Cache.MaxMemoryMB, 256)
}

func TestLoad_InvalidBoolKeepsDefault(t *testing.T) {
	t.Parallel()

	cfg := config.Load(makeEnv(map[string]string{
		"TC_REPAIR_ENABLED": "not-a-bool",
	}))

	assertEqual(t, cfg.Cluster.RepairEnabled, true)
}
