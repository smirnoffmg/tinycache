package config

import "strconv"

type Config struct {
	Node        NodeConfig
	Cluster     ClusterConfig
	Cache       CacheConfig
	Persistence PersistenceConfig
	Server      ServerConfig
}

type NodeConfig struct {
	Addr         string
	InternalAddr string
}

type ClusterConfig struct {
	PodName           string
	Namespace         string
	Replicas          int
	ServiceName       string
	ReplicationFactor int
	WriteQuorum       int
	ReadQuorum        int
	QuorumTimeoutMs   int
	VirtualNodes      int
	RepairEnabled     bool
}

type CacheConfig struct {
	MaxMemoryMB        int
	DefaultTTLSeconds  int
	EvictionIntervalMs int
}

type PersistenceConfig struct {
	Enabled  bool
	DataDir  string
	AOF      AOFConfig
	Snapshot SnapshotConfig
}

type AOFConfig struct {
	Enabled   bool
	Fsync     string
	MaxSizeMB int
}

type SnapshotConfig struct {
	Enabled         bool
	IntervalSeconds int
	MinChanges      int
}

type ServerConfig struct {
	HealthAddr             string
	ShutdownTimeoutSeconds int
	MaxConnections         int
	ReadTimeoutMs          int
	WriteTimeoutMs         int
}

// Load builds a Config from environment variables, using defaults for any unset values.
// The getenv parameter allows injecting a custom lookup function (use os.Getenv in production).
func Load(getenv func(string) string) Config {
	return Config{
		Node:        loadNode(getenv),
		Cluster:     loadCluster(getenv),
		Cache:       loadCache(getenv),
		Persistence: loadPersistence(getenv),
		Server:      loadServer(getenv),
	}
}

func loadNode(getenv func(string) string) NodeConfig {
	return NodeConfig{
		Addr:         envStr(getenv, "TC_ADDR", "0.0.0.0:11211"),
		InternalAddr: envStr(getenv, "TC_INTERNAL_ADDR", "0.0.0.0:11311"),
	}
}

func loadCluster(getenv func(string) string) ClusterConfig {
	return ClusterConfig{
		PodName:           envStr(getenv, "POD_NAME", ""),
		Namespace:         envStr(getenv, "POD_NAMESPACE", "default"),
		Replicas:          envInt(getenv, "TC_CLUSTER_REPLICAS", 3),
		ServiceName:       envStr(getenv, "TC_SERVICE_NAME", "tinycache"),
		ReplicationFactor: envInt(getenv, "TC_REPLICATION_FACTOR", 3),
		WriteQuorum:       envInt(getenv, "TC_WRITE_QUORUM", 2),
		ReadQuorum:        envInt(getenv, "TC_READ_QUORUM", 1),
		QuorumTimeoutMs:   envInt(getenv, "TC_QUORUM_TIMEOUT_MS", 50),
		VirtualNodes:      envInt(getenv, "TC_VIRTUAL_NODES", 150),
		RepairEnabled:     envBool(getenv, "TC_REPAIR_ENABLED", true),
	}
}

func loadCache(getenv func(string) string) CacheConfig {
	return CacheConfig{
		MaxMemoryMB:        envInt(getenv, "TC_MAX_MEMORY_MB", 256),
		DefaultTTLSeconds:  envInt(getenv, "TC_DEFAULT_TTL_SECONDS", 0),
		EvictionIntervalMs: envInt(getenv, "TC_EVICTION_INTERVAL_MS", 500),
	}
}

func loadPersistence(getenv func(string) string) PersistenceConfig {
	return PersistenceConfig{
		Enabled: envBool(getenv, "TC_PERSISTENCE_ENABLED", true),
		DataDir: envStr(getenv, "TC_DATA_DIR", "/data"),
		AOF: AOFConfig{
			Enabled:   envBool(getenv, "TC_AOF_ENABLED", true),
			Fsync:     envStr(getenv, "TC_AOF_FSYNC", "everysec"),
			MaxSizeMB: envInt(getenv, "TC_AOF_MAX_SIZE_MB", 512),
		},
		Snapshot: SnapshotConfig{
			Enabled:         envBool(getenv, "TC_SNAPSHOT_ENABLED", true),
			IntervalSeconds: envInt(getenv, "TC_SNAPSHOT_INTERVAL_SECONDS", 300),
			MinChanges:      envInt(getenv, "TC_SNAPSHOT_MIN_CHANGES", 1000),
		},
	}
}

func loadServer(getenv func(string) string) ServerConfig {
	return ServerConfig{
		HealthAddr:             envStr(getenv, "TC_HEALTH_ADDR", "0.0.0.0:9090"),
		ShutdownTimeoutSeconds: envInt(getenv, "TC_SHUTDOWN_TIMEOUT_SECONDS", 30),
		MaxConnections:         envInt(getenv, "TC_MAX_CONNECTIONS", 10000),
		ReadTimeoutMs:          envInt(getenv, "TC_READ_TIMEOUT_MS", 5000),
		WriteTimeoutMs:         envInt(getenv, "TC_WRITE_TIMEOUT_MS", 5000),
	}
}

func envStr(getenv func(string) string, key, fallback string) string {
	if v := getenv(key); v != "" {
		return v
	}
	return fallback
}

func envInt(getenv func(string) string, key string, fallback int) int {
	v := getenv(key)
	if v == "" {
		return fallback
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return fallback
	}
	return n
}

func envBool(getenv func(string) string, key string, fallback bool) bool {
	v := getenv(key)
	if v == "" {
		return fallback
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		return fallback
	}
	return b
}
