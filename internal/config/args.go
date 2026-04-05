// Package config provides the CLI argument parser for valkey-server.
//
// Argument syntax mirrors Redis:
//
//	valkey-server                         # all defaults
//	valkey-server valkey.conf             # load config file
//	valkey-server --port 7379             # inline overrides
//	valkey-server valkey.conf --port 7379 # config file + overrides
//	valkey-server --port 7379 --loglevel debug
//
// Boolean flags accept: yes|no|true|false|1|0
package config

import (
	"fmt"
	"strconv"
	"strings"
	"time"
	"unicode"
)

// ArgType enumerates the value types a flag can hold.
type ArgType int

const (
	ArgTypeString      ArgType = iota // plain string value
	ArgTypeBool                       // yes/no/true/false/1/0
	ArgTypeInt                        // signed integer
	ArgTypeInt64                      // signed 64-bit integer (e.g. maxmemory)
	ArgTypeDuration                   // integer seconds → time.Duration
	ArgTypeStringSlice                // space- or comma-separated list
	ArgTypeMemoryBytes                // "100mb", "1gb", bare integer (bytes)
)

// ArgDef describes a single accepted CLI flag.
type ArgDef struct {
	// Long is the canonical --long-name of the flag (without the "--" prefix).
	Long string
	// Short is the optional single-character alias (without the "-" prefix).
	Short string
	// Type controls how the raw string value is parsed.
	Type ArgType
	// Description is a human-readable help string.
	Description string
	// Required marks flags that must be provided.
	Required bool
}

// registry is the ordered list of all recognised flags.
var registry = []ArgDef{
	// ── Network ──────────────────────────────────────────────────────────────
	{Long: "port", Short: "p", Type: ArgTypeInt,
		Description: "TCP port to listen on (default 6379)"},
	{Long: "bind", Type: ArgTypeStringSlice,
		Description: "IP addresses to bind to (space-separated)"},
	{Long: "unixsocket", Type: ArgTypeString,
		Description: "Path to a UNIX domain socket"},
	{Long: "unixsocketperm", Type: ArgTypeInt,
		Description: "Permissions for the UNIX socket (octal, e.g. 700)"},
	{Long: "tcp-backlog", Type: ArgTypeInt,
		Description: "TCP listen() backlog queue size"},
	{Long: "timeout", Type: ArgTypeDuration,
		Description: "Close idle client connections after N seconds (0 = never)"},
	{Long: "tcp-keepalive", Type: ArgTypeDuration,
		Description: "TCP keepalive interval in seconds (0 = off)"},
	{Long: "protected-mode", Type: ArgTypeBool,
		Description: "Refuse external connections when no auth is configured"},
	{Long: "tls-port", Type: ArgTypeInt,
		Description: "TLS port (0 = disabled)"},
	{Long: "read-buf-size", Type: ArgTypeMemoryBytes,
		Description: "Per-connection read buffer size (e.g. 16kb, default 16kb)"},
	{Long: "max-inline-size", Type: ArgTypeMemoryBytes,
		Description: "Maximum inline (non-RESP) command length (e.g. 64kb)"},

	// ── General ──────────────────────────────────────────────────────────────
	{Long: "daemonize", Short: "d", Type: ArgTypeBool,
		Description: "Run as a background daemon"},
	{Long: "pidfile", Type: ArgTypeString,
		Description: "Path for the PID file when daemonized"},
	{Long: "loglevel", Type: ArgTypeString,
		Description: "Log verbosity: debug | verbose | notice | warning"},
	{Long: "logfile", Type: ArgTypeString,
		Description: "Path to the log file (empty = stdout)"},
	{Long: "databases", Type: ArgTypeInt,
		Description: "Number of logical databases"},
	{Long: "always-show-logo", Type: ArgTypeBool,
		Description: "Always display the ASCII-art logo on startup"},

	// ── Persistence ──────────────────────────────────────────────────────────
	{Long: "save", Type: ArgTypeString,
		Description: `Snapshot trigger: "save <seconds> <changes>" (repeat for multiple)`,
	},
	{Long: "stop-writes-on-bgsave-error", Type: ArgTypeBool,
		Description: "Halt writes when a background save fails"},
	{Long: "rdbcompression", Type: ArgTypeBool,
		Description: "Enable LZF compression in RDB files"},
	{Long: "rdbchecksum", Type: ArgTypeBool,
		Description: "Append CRC64 checksum to RDB files"},
	{Long: "dbfilename", Type: ArgTypeString,
		Description: "Filename for RDB snapshots"},
	{Long: "dir", Type: ArgTypeString,
		Description: "Working directory for data files"},
	{Long: "appendonly", Type: ArgTypeBool,
		Description: "Enable append-only file (AOF) persistence"},
	{Long: "appendfilename", Type: ArgTypeString,
		Description: "Filename for the AOF"},
	{Long: "appendfsync", Type: ArgTypeString,
		Description: "AOF fsync policy: always | everysec | no"},
	{Long: "no-appendfsync-on-rewrite", Type: ArgTypeBool,
		Description: "Skip fsync during BGREWRITEAOF"},
	{Long: "auto-aof-rewrite-percentage", Type: ArgTypeInt,
		Description: "Trigger AOF rewrite when file grows by this percent"},
	{Long: "auto-aof-rewrite-min-size", Type: ArgTypeMemoryBytes,
		Description: "Minimum AOF size before auto-rewrite (e.g. 64mb)"},

	// ── Security ─────────────────────────────────────────────────────────────
	{Long: "requirepass", Type: ArgTypeString,
		Description: "Password required for client authentication"},
	{Long: "rename-command", Type: ArgTypeString,
		Description: `Rename or disable a command: "rename-command CMD newname" (empty newname disables)`},
	{Long: "aclfile", Type: ArgTypeString,
		Description: "Path to an ACL rules file"},

	// ── Memory ───────────────────────────────────────────────────────────────
	{Long: "maxmemory", Short: "m", Type: ArgTypeMemoryBytes,
		Description: "Max memory limit (e.g. 256mb, 1gb, 0 = unlimited)"},
	{Long: "maxmemory-policy", Type: ArgTypeString,
		Description: "Eviction policy when maxmemory is reached"},
	{Long: "maxmemory-samples", Type: ArgTypeInt,
		Description: "Sample size for LRU/LFU approximation"},
	{Long: "lazyfree-lazy-eviction", Type: ArgTypeBool,
		Description: "Perform evictions asynchronously"},

	// ── Replication ──────────────────────────────────────────────────────────
	{Long: "replicaof", Type: ArgTypeString,
		Description: `Primary to replicate from: "replicaof <host> <port>"`},
	{Long: "masterauth", Type: ArgTypeString,
		Description: "Password to authenticate with the primary"},
	{Long: "replica-serve-stale-data", Type: ArgTypeBool,
		Description: "Allow replicas to serve (possibly stale) reads"},
	{Long: "replica-read-only", Type: ArgTypeBool,
		Description: "Prevent writes on replica instances"},
	{Long: "repl-backlog-size", Type: ArgTypeMemoryBytes,
		Description: "Circular buffer size for partial resync (e.g. 1mb)"},

	// ── Cluster ──────────────────────────────────────────────────────────────
	{Long: "cluster-enabled", Type: ArgTypeBool,
		Description: "Enable cluster mode"},
	{Long: "cluster-config-file", Type: ArgTypeString,
		Description: "Path to the cluster node configuration file"},
	{Long: "cluster-node-timeout", Type: ArgTypeDuration,
		Description: "Milliseconds before a node is considered failing"},
	{Long: "cluster-require-full-coverage", Type: ArgTypeBool,
		Description: "Stop accepting writes if any hash slot is uncovered"},
}

// byLong and byShort are lookup tables built once at init time.
var (
	byLong  map[string]*ArgDef
	byShort map[string]*ArgDef
)

func init() {
	byLong = make(map[string]*ArgDef, len(registry))
	byShort = make(map[string]*ArgDef)
	for i := range registry {
		d := &registry[i]
		byLong[d.Long] = d
		if d.Short != "" {
			byShort[d.Short] = d
		}
	}
}

// ParsedArgs holds the raw parse result before it is applied to a Config.
type ParsedArgs struct {
	// ConfigFile is the optional positional config-file argument (first token
	// that does not start with '-').
	ConfigFile string

	// Flags maps flag long-names to their raw string values. Multi-occurrence
	// flags (save, rename-command) accumulate as newline-joined entries.
	Flags map[string]string
}

// Parse parses os.Args[1:] (pass the slice directly) and returns a ParsedArgs
// together with any syntax error.
func Parse(args []string) (*ParsedArgs, error) {
	pa := &ParsedArgs{Flags: make(map[string]string)}

	i := 0

	// First token: optional positional config file (must not start with '-').
	if len(args) > 0 && !strings.HasPrefix(args[0], "-") {
		pa.ConfigFile = args[0]
		i = 1
	}

	for i < len(args) {
		token := args[i]

		if !strings.HasPrefix(token, "-") {
			return nil, fmt.Errorf("unexpected positional argument %q (config file must be first)", token)
		}

		// Normalise: strip leading dashes and split on '=' for --key=value style.
		key, inlineVal, hasInlineVal := strings.Cut(strings.TrimLeft(token, "-"), "=")
		key = strings.ToLower(strings.TrimSpace(key))

		def := resolve(key)
		if def == nil {
			return nil, fmt.Errorf("unknown flag: %q", token)
		}
		// Canonical name for multi-word aliases.
		key = def.Long

		// Boolean flags may stand alone (treated as "yes").
		if def.Type == ArgTypeBool {
			if hasInlineVal {
				if err := validateBoolString(inlineVal, token); err != nil {
					return nil, err
				}
				pa.Flags[key] = normaliseBool(inlineVal)
			} else if i+1 < len(args) && isBoolValue(args[i+1]) {
				i++
				pa.Flags[key] = normaliseBool(args[i])
			} else {
				// Flag present without a value → implicitly true.
				pa.Flags[key] = "yes"
			}
			i++
			continue
		}

		// All other types require a value token.
		var raw string
		if hasInlineVal {
			raw = inlineVal
		} else {
			i++
			if i >= len(args) {
				return nil, fmt.Errorf("flag --%s requires a value", key)
			}
			raw = args[i]
		}

		// For repeatable flags (save, rename-command) accumulate with newlines.
		if isRepeatable(key) {
			if existing, ok := pa.Flags[key]; ok {
				pa.Flags[key] = existing + "\n" + raw
			} else {
				pa.Flags[key] = raw
			}
		} else {
			pa.Flags[key] = raw
		}

		i++
	}

	return pa, nil
}

// Apply maps the parsed flags onto cfg, overriding whatever is already set
// (e.g. values loaded from a config file).
func (pa *ParsedArgs) Apply(cfg *Config) error {
	for key, raw := range pa.Flags {
		if err := applyFlag(cfg, key, raw); err != nil {
			return fmt.Errorf("--%s %q: %w", key, raw, err)
		}
	}
	return nil
}

// Help returns a formatted usage string listing all accepted flags.
func Help() string {
	var b strings.Builder
	b.WriteString("Usage: valkey-server [config-file] [--option value] ...\n\n")
	b.WriteString("Options:\n")

	// Group by section headers derived from the registry ordering.
	sections := []struct {
		header string
		flags  []string
	}{
		{"Network", []string{"port", "bind", "unixsocket", "unixsocketperm",
			"tcp-backlog", "timeout", "tcp-keepalive", "protected-mode", "tls-port",
			"read-buf-size", "max-inline-size"}},
		{"General", []string{"daemonize", "pidfile", "loglevel", "logfile",
			"databases", "always-show-logo"}},
		{"Persistence", []string{"save", "stop-writes-on-bgsave-error",
			"rdbcompression", "rdbchecksum", "dbfilename", "dir",
			"appendonly", "appendfilename", "appendfsync",
			"no-appendfsync-on-rewrite", "auto-aof-rewrite-percentage",
			"auto-aof-rewrite-min-size"}},
		{"Security", []string{"requirepass", "rename-command", "aclfile"}},
		{"Memory", []string{"maxmemory", "maxmemory-policy", "maxmemory-samples",
			"lazyfree-lazy-eviction"}},
		{"Replication", []string{"replicaof", "masterauth",
			"replica-serve-stale-data", "replica-read-only", "repl-backlog-size"}},
		{"Cluster", []string{"cluster-enabled", "cluster-config-file",
			"cluster-node-timeout", "cluster-require-full-coverage"}},
	}

	for _, sec := range sections {
		fmt.Fprintf(&b, "\n  %s:\n", sec.header)
		for _, long := range sec.flags {
			def, ok := byLong[long]
			if !ok {
				continue
			}
			flag := "    --" + def.Long
			if def.Short != "" {
				flag += ", -" + def.Short
			}
			// Pad to column 40.
			pad := 40 - len(flag)
			if pad < 1 {
				pad = 1
			}
			fmt.Fprintf(&b, "%s%s%s\n", flag, strings.Repeat(" ", pad), def.Description)
		}
	}
	return b.String()
}

// ─── internal helpers ────────────────────────────────────────────────────────

// resolve looks up a flag by long name or short name.
func resolve(key string) *ArgDef {
	if d, ok := byLong[key]; ok {
		return d
	}
	if d, ok := byShort[key]; ok {
		return d
	}
	return nil
}

// isRepeatable returns true for flags that may appear more than once.
func isRepeatable(long string) bool {
	return long == "save" || long == "rename-command"
}

// isBoolValue returns true if s looks like a boolean literal.
func isBoolValue(s string) bool {
	switch strings.ToLower(s) {
	case "yes", "no", "true", "false", "1", "0":
		return true
	}
	return false
}

func validateBoolString(s, flag string) error {
	if !isBoolValue(s) {
		return fmt.Errorf("flag %s: %q is not a valid boolean (use yes/no/true/false/1/0)", flag, s)
	}
	return nil
}

// normaliseBool converts any accepted boolean string to "yes" or "no".
func normaliseBool(s string) string {
	switch strings.ToLower(s) {
	case "yes", "true", "1":
		return "yes"
	default:
		return "no"
	}
}

// parseMemoryBytes parses a memory value like "256mb", "1gb", "512kb", or a
// bare integer (treated as bytes).
func parseMemoryBytes(raw string) (int64, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" || raw == "0" {
		return 0, nil
	}

	// Find where the numeric prefix ends.
	split := len(raw)
	for i, r := range raw {
		if !unicode.IsDigit(r) && r != '.' {
			split = i
			break
		}
	}

	numStr := raw[:split]
	suffix := strings.ToLower(strings.TrimSpace(raw[split:]))

	num, err := strconv.ParseFloat(numStr, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid number %q", numStr)
	}

	var multiplier float64
	switch suffix {
	case "", "b":
		multiplier = 1
	case "kb", "k":
		multiplier = 1024
	case "mb", "m":
		multiplier = 1024 * 1024
	case "gb", "g":
		multiplier = 1024 * 1024 * 1024
	case "tb", "t":
		multiplier = 1024 * 1024 * 1024 * 1024
	default:
		return 0, fmt.Errorf("unknown size suffix %q (use b/kb/mb/gb/tb)", suffix)
	}

	return int64(num * multiplier), nil
}

// parseSavePoint parses "seconds changes" from a save flag value.
func parseSavePoint(raw string) (SavePoint, error) {
	parts := strings.Fields(raw)
	if len(parts) != 2 {
		return SavePoint{}, fmt.Errorf("expected \"<seconds> <changes>\", got %q", raw)
	}
	secs, err := strconv.Atoi(parts[0])
	if err != nil {
		return SavePoint{}, fmt.Errorf("seconds %q: %w", parts[0], err)
	}
	changes, err := strconv.Atoi(parts[1])
	if err != nil {
		return SavePoint{}, fmt.Errorf("changes %q: %w", parts[1], err)
	}
	return SavePoint{Seconds: secs, Changes: changes}, nil
}

// applyFlag applies a single parsed key→raw pair to cfg.
//
//nolint:cyclop,gocyclo // this is intentionally a large dispatch table
func applyFlag(cfg *Config, key, raw string) error {
	switch key {
	// ── Network ──────────────────────────────────────────────────────────────
	case "port":
		v, err := strconv.Atoi(raw)
		if err != nil || v < 0 || v > 65535 {
			return fmt.Errorf("port must be 0-65535")
		}
		cfg.Network.Port = v

	case "bind":
		cfg.Network.Bind = strings.Fields(raw)

	case "unixsocket":
		cfg.Network.UnixSocket = raw

	case "unixsocketperm":
		v, err := strconv.ParseUint(raw, 8, 32)
		if err != nil {
			return fmt.Errorf("must be an octal integer (e.g. 700)")
		}
		cfg.Network.UnixSocketPerm = uint32(v)

	case "tcp-backlog":
		v, err := strconv.Atoi(raw)
		if err != nil || v < 0 {
			return fmt.Errorf("must be a non-negative integer")
		}
		cfg.Network.TCPBacklog = v

	case "timeout":
		secs, err := strconv.Atoi(raw)
		if err != nil || secs < 0 {
			return fmt.Errorf("must be a non-negative integer (seconds)")
		}
		cfg.Network.Timeout = time.Duration(secs) * time.Second

	case "tcp-keepalive":
		secs, err := strconv.Atoi(raw)
		if err != nil || secs < 0 {
			return fmt.Errorf("must be a non-negative integer (seconds)")
		}
		cfg.Network.TCPKeepAlive = time.Duration(secs) * time.Second

	case "protected-mode":
		cfg.Network.ProtectedMode = raw == "yes"

	case "tls-port":
		v, err := strconv.Atoi(raw)
		if err != nil || v < 0 || v > 65535 {
			return fmt.Errorf("must be 0-65535")
		}
		cfg.Network.TLSPort = v

	case "read-buf-size":
		v, err := parseMemoryBytes(raw)
		if err != nil {
			return err
		}
		if v < 1024 {
			return fmt.Errorf("read-buf-size must be at least 1kb")
		}
		cfg.Network.ReadBufSize = int(v)

	case "max-inline-size":
		v, err := parseMemoryBytes(raw)
		if err != nil {
			return err
		}
		cfg.Network.MaxInlineSize = int(v)

	// ── General ──────────────────────────────────────────────────────────────
	case "daemonize":
		cfg.General.Daemonize = raw == "yes"

	case "pidfile":
		cfg.General.PidFile = raw

	case "loglevel":
		ll := LogLevel(strings.ToLower(raw))
		switch ll {
		case LogLevelDebug, LogLevelVerbose, LogLevelNotice, LogLevelWarning:
			cfg.General.LogLevel = ll
		default:
			return fmt.Errorf("must be debug|verbose|notice|warning")
		}

	case "logfile":
		cfg.General.LogFile = raw

	case "databases":
		v, err := strconv.Atoi(raw)
		if err != nil || v < 1 {
			return fmt.Errorf("must be a positive integer")
		}
		cfg.General.Databases = v

	case "always-show-logo":
		cfg.General.AlwaysShowLogo = raw == "yes"

	// ── Persistence ──────────────────────────────────────────────────────────
	case "save":
		// May be "" to disable all saves, or multi-line due to repetition.
		if strings.TrimSpace(raw) == "" {
			cfg.Persistence.Save = nil
			break
		}
		for _, line := range strings.Split(raw, "\n") {
			sp, err := parseSavePoint(strings.TrimSpace(line))
			if err != nil {
				return err
			}
			cfg.Persistence.Save = append(cfg.Persistence.Save, sp)
		}

	case "stop-writes-on-bgsave-error":
		cfg.Persistence.StopOnBGSaveError = raw == "yes"

	case "rdbcompression":
		cfg.Persistence.RDBCompression = raw == "yes"

	case "rdbchecksum":
		cfg.Persistence.RDBChecksum = raw == "yes"

	case "dbfilename":
		cfg.Persistence.DBFilename = raw

	case "dir":
		cfg.Persistence.Dir = raw

	case "appendonly":
		cfg.Persistence.AppendOnly = raw == "yes"

	case "appendfilename":
		cfg.Persistence.AppendFilename = raw

	case "appendfsync":
		af := AppendFsync(strings.ToLower(raw))
		switch af {
		case AppendFsyncAlways, AppendFsyncEverysec, AppendFsyncNo:
			cfg.Persistence.AppendFsync = af
		default:
			return fmt.Errorf("must be always|everysec|no")
		}

	case "no-appendfsync-on-rewrite":
		cfg.Persistence.NoAppendFsyncOnRewrite = raw == "yes"

	case "auto-aof-rewrite-percentage":
		v, err := strconv.Atoi(raw)
		if err != nil || v < 0 {
			return fmt.Errorf("must be a non-negative integer")
		}
		cfg.Persistence.AutoAOFRewritePercentage = v

	case "auto-aof-rewrite-min-size":
		v, err := parseMemoryBytes(raw)
		if err != nil {
			return err
		}
		cfg.Persistence.AutoAOFRewriteMinSize = v

	// ── Security ─────────────────────────────────────────────────────────────
	case "requirepass":
		cfg.Security.RequirePass = raw

	case "rename-command":
		// Format: "COMMAND newname"  (newname empty = disable)
		parts := strings.Fields(raw)
		if len(parts) < 1 || len(parts) > 2 {
			return fmt.Errorf("expected \"CMD [newname]\"")
		}
		cmd := strings.ToUpper(parts[0])
		newName := ""
		if len(parts) == 2 {
			newName = parts[1]
		}
		if cfg.Security.RenameCommand == nil {
			cfg.Security.RenameCommand = make(map[string]string)
		}
		cfg.Security.RenameCommand[cmd] = newName

	case "aclfile":
		cfg.Security.ACLFile = raw

	// ── Memory ───────────────────────────────────────────────────────────────
	case "maxmemory":
		v, err := parseMemoryBytes(raw)
		if err != nil {
			return err
		}
		cfg.Memory.MaxMemory = v

	case "maxmemory-policy":
		p := MaxMemoryPolicy(strings.ToLower(raw))
		switch p {
		case MaxMemoryPolicyNoEviction, MaxMemoryPolicyAllKeysLRU,
			MaxMemoryPolicyVolatileLRU, MaxMemoryPolicyAllKeysLFU,
			MaxMemoryPolicyVolatileLFU, MaxMemoryPolicyAllKeysRandom,
			MaxMemoryPolicyVolatileRandom, MaxMemoryPolicyVolatileTTL:
			cfg.Memory.MaxMemoryPolicy = p
		default:
			return fmt.Errorf("unknown policy %q", raw)
		}

	case "maxmemory-samples":
		v, err := strconv.Atoi(raw)
		if err != nil || v < 1 {
			return fmt.Errorf("must be a positive integer")
		}
		cfg.Memory.MaxMemorySamples = v

	case "lazyfree-lazy-eviction":
		cfg.Memory.LazyFreeLazyEviction = raw == "yes"

	// ── Replication ──────────────────────────────────────────────────────────
	case "replicaof":
		cfg.Replication.ReplicaOf = raw

	case "masterauth":
		cfg.Replication.MasterAuth = raw

	case "replica-serve-stale-data":
		cfg.Replication.ReplicaServeStaleData = raw == "yes"

	case "replica-read-only":
		cfg.Replication.ReplicaReadOnly = raw == "yes"

	case "repl-backlog-size":
		v, err := parseMemoryBytes(raw)
		if err != nil {
			return err
		}
		cfg.Replication.ReplicationBacklogSize = v

	// ── Cluster ──────────────────────────────────────────────────────────────
	case "cluster-enabled":
		cfg.Cluster.Enabled = raw == "yes"

	case "cluster-config-file":
		cfg.Cluster.ConfigFile = raw

	case "cluster-node-timeout":
		ms, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || ms < 0 {
			return fmt.Errorf("must be a non-negative integer (milliseconds)")
		}
		cfg.Cluster.NodeTimeout = time.Duration(ms) * time.Millisecond

	case "cluster-require-full-coverage":
		cfg.Cluster.RequireFullCoverage = raw == "yes"

	default:
		return fmt.Errorf("no handler for flag (this is a bug)")
	}

	return nil
}
