// Package config defines the Valkey server configuration types and defaults,
// mirroring Redis-style configuration keys and semantics.
package config

import "time"

// LogLevel represents the verbosity of server logging.
type LogLevel string

const (
	LogLevelDebug   LogLevel = "debug"
	LogLevelVerbose LogLevel = "verbose"
	LogLevelNotice  LogLevel = "notice"  // default
	LogLevelWarning LogLevel = "warning"
)

// MaxMemoryPolicy controls eviction behavior when maxmemory is reached.
type MaxMemoryPolicy string

const (
	MaxMemoryPolicyNoEviction     MaxMemoryPolicy = "noeviction"
	MaxMemoryPolicyAllKeysLRU     MaxMemoryPolicy = "allkeys-lru"
	MaxMemoryPolicyVolatileLRU    MaxMemoryPolicy = "volatile-lru"
	MaxMemoryPolicyAllKeysLFU     MaxMemoryPolicy = "allkeys-lfu"
	MaxMemoryPolicyVolatileLFU    MaxMemoryPolicy = "volatile-lfu"
	MaxMemoryPolicyAllKeysRandom  MaxMemoryPolicy = "allkeys-random"
	MaxMemoryPolicyVolatileRandom MaxMemoryPolicy = "volatile-random"
	MaxMemoryPolicyVolatileTTL    MaxMemoryPolicy = "volatile-ttl"
)

// AppendFsync controls how often fsync is called on the append-only file.
type AppendFsync string

const (
	AppendFsyncAlways   AppendFsync = "always"
	AppendFsyncEverysec AppendFsync = "everysec" // default
	AppendFsyncNo       AppendFsync = "no"
)

// SavePoint defines a condition under which an RDB snapshot is triggered:
// if Changes keys are modified within Seconds seconds, save.
type SavePoint struct {
	Seconds int
	Changes int
}

// NetworkConfig groups all TCP/socket listening options.
type NetworkConfig struct {
	// Bind is the list of IP addresses to listen on. Empty means all interfaces.
	Bind []string
	// Port is the TCP port to listen on (default 6379, 0 to disable TCP).
	Port int
	// UnixSocket is the path to a UNIX domain socket. Empty means disabled.
	UnixSocket string
	// UnixSocketPerm is the permissions for the UNIX socket file (octal).
	UnixSocketPerm uint32
	// TCPBacklog is the listen(2) backlog queue size.
	TCPBacklog int
	// Timeout closes idle client connections after this many seconds (0 = never).
	Timeout time.Duration
	// TCPKeepAlive sends TCP ACKs to clients at this interval (0 = disabled).
	TCPKeepAlive time.Duration
	// ProtectedMode rejects connections from non-loopback when no auth is set.
	ProtectedMode bool
	// TLSPort is the TLS-enabled TCP port (0 = disabled).
	TLSPort int

	// ReadBufSize is the size of the per-connection read buffer in bytes.
	// The BufReader for each connection is allocated with this capacity.
	// Reads from the network are chunked to this size.
	ReadBufSize int
	// MaxInlineSize is the maximum accepted length of an inline (non-RESP) command.
	MaxInlineSize int
}

// GeneralConfig groups process-level and logging options.
type GeneralConfig struct {
	// Daemonize runs the server as a background daemon.
	Daemonize bool
	// PidFile is the path where the PID is written when daemonized.
	PidFile string
	// LogLevel controls log verbosity.
	LogLevel LogLevel
	// LogFile is the path to the log file. Empty means stdout.
	LogFile string
	// Databases is the number of logical databases (default 16).
	Databases int
	// AlwaysShowLogo prints the ASCII-art logo on startup.
	AlwaysShowLogo bool
}

// PersistenceConfig groups RDB snapshotting and AOF options.
type PersistenceConfig struct {
	// Save holds the list of save conditions; empty disables RDB snapshots.
	Save []SavePoint
	// StopOnBGSaveError halts writes when a background save fails.
	StopOnBGSaveError bool
	// RDBCompression enables LZF compression in RDB files.
	RDBCompression bool
	// RDBChecksum appends a CRC64 checksum to RDB files.
	RDBChecksum bool
	// DBFilename is the name of the RDB dump file.
	DBFilename string
	// Dir is the working directory for data files.
	Dir string

	// AppendOnly enables the append-only file (AOF) persistence mode.
	AppendOnly bool
	// AppendFilename is the name of the AOF file.
	AppendFilename string
	// AppendFsync controls fsync frequency for AOF writes.
	AppendFsync AppendFsync
	// NoAppendFsyncOnRewrite skips fsync during BGREWRITEAOF.
	NoAppendFsyncOnRewrite bool
	// AutoAOFRewritePercentage triggers rewrite when AOF grows by this percent.
	AutoAOFRewritePercentage int
	// AutoAOFRewriteMinSize is the minimum AOF size before auto-rewrite.
	AutoAOFRewriteMinSize int64
}

// SecurityConfig groups authentication and command-renaming options.
type SecurityConfig struct {
	// RequirePass sets a password clients must authenticate with.
	RequirePass string
	// RenameCommand maps original command names to new (or empty = disabled) names.
	RenameCommand map[string]string
	// ACLFile is the path to an ACL rules file.
	ACLFile string
}

// MemoryConfig groups memory limits and eviction policy.
type MemoryConfig struct {
	// MaxMemory is the memory limit in bytes (0 = unlimited).
	MaxMemory int64
	// MaxMemoryPolicy is the eviction policy when MaxMemory is reached.
	MaxMemoryPolicy MaxMemoryPolicy
	// MaxMemorySamples is the sample size for LRU/LFU approximation.
	MaxMemorySamples int
	// LazyFreeLazyEviction runs evictions in a background thread.
	LazyFreeLazyEviction bool
}

// ReplicationConfig groups leader/follower replication settings.
type ReplicationConfig struct {
	// ReplicaOf is "<host> <port>" of the primary to replicate from.
	ReplicaOf string
	// MasterAuth is the password to authenticate with the primary.
	MasterAuth string
	// ReplicaServeStaleData allows replicas to serve (possibly stale) reads.
	ReplicaServeStaleData bool
	// ReplicaReadOnly prevents writes on replica instances.
	ReplicaReadOnly bool
	// ReplicationBacklogSize is the circular buffer size for partial resync.
	ReplicationBacklogSize int64
}

// ClusterConfig groups cluster-mode settings.
type ClusterConfig struct {
	// Enabled activates cluster mode.
	Enabled bool
	// ConfigFile is the path to the cluster node configuration file.
	ConfigFile string
	// NodeTimeout is the milliseconds before a node is considered failing.
	NodeTimeout time.Duration
	// RequireFullCoverage stops accepting writes if any hash slot is uncovered.
	RequireFullCoverage bool
}

// Config is the top-level server configuration, composed of domain-specific
// sub-structs. It is populated by the argument parser and config-file parser.
type Config struct {
	Network     NetworkConfig
	General     GeneralConfig
	Persistence PersistenceConfig
	Security    SecurityConfig
	Memory      MemoryConfig
	Replication ReplicationConfig
	Cluster     ClusterConfig
}

// DefaultConfig returns a Config pre-filled with Redis-compatible defaults.
func DefaultConfig() *Config {
	return &Config{
		Network: NetworkConfig{
			Bind:           []string{"127.0.0.1"},
			Port:           6379,
			TCPBacklog:     511,
			Timeout:        0,
			TCPKeepAlive:   300 * time.Second,
			ProtectedMode:  true,
			UnixSocketPerm: 0o700,
			ReadBufSize:    16 * 1024, // 16 KiB per connection
			MaxInlineSize:  64 * 1024, // 64 KiB inline command cap
		},
		General: GeneralConfig{
			LogLevel:       LogLevelNotice,
			Databases:      16,
			AlwaysShowLogo: true,
			PidFile:        "/var/run/valkey/valkey.pid",
		},
		Persistence: PersistenceConfig{
			Save: []SavePoint{
				{Seconds: 3600, Changes: 1},
				{Seconds: 300, Changes: 100},
				{Seconds: 60, Changes: 10000},
			},
			StopOnBGSaveError:        true,
			RDBCompression:           true,
			RDBChecksum:              true,
			DBFilename:               "dump.rdb",
			Dir:                      "./",
			AppendFsync:              AppendFsyncEverysec,
			AutoAOFRewritePercentage: 100,
			AutoAOFRewriteMinSize:    64 * 1024 * 1024, // 64 MB
			AppendFilename:           "appendonly.aof",
		},
		Security: SecurityConfig{
			RenameCommand: make(map[string]string),
		},
		Memory: MemoryConfig{
			MaxMemoryPolicy:  MaxMemoryPolicyNoEviction,
			MaxMemorySamples: 5,
		},
		Replication: ReplicationConfig{
			ReplicaServeStaleData: true,
			ReplicaReadOnly:       true,
			ReplicationBacklogSize: 1 * 1024 * 1024, // 1 MB
		},
		Cluster: ClusterConfig{
			NodeTimeout:         15 * time.Second,
			RequireFullCoverage: true,
		},
	}
}
