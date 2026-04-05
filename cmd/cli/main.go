// valkey-cli is an interactive command-line client for the Valkey server.
//
// Usage:
//
//	valkey-cli [--host 127.0.0.1] [--port 6379]
//	valkey-cli -h 127.0.0.1 -p 6379
package main

import (
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

	"github.com/chzyer/readline"
	"github.com/valkey/valkey/internal/client"
)

const defaultHost = "127.0.0.1"
const defaultPort = 6379

func main() {
	host, port := parseFlags(os.Args[1:])
	addr := fmt.Sprintf("%s:%d", host, port)

	cl, err := client.Dial(addr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Could not connect to Valkey at %s: %v\n", addr, err)
		os.Exit(1)
	}
	defer cl.Close()

	fmt.Printf("Connected to %s\n", addr)
	runREPL(cl, addr)
}

// ─── REPL ─────────────────────────────────────────────────────────────────────

func runREPL(cl *client.Client, addr string) {
	rl, err := readline.NewEx(&readline.Config{
		Prompt:          prompt(addr),
		HistoryFile:     historyFile(),
		AutoComplete:    completer(),
		InterruptPrompt: "^C",
		EOFPrompt:       "exit",
	})
	if err != nil {
		// Fallback: if readline fails (e.g. not a TTY), skip it.
		fmt.Fprintf(os.Stderr, "readline init: %v\n", err)
		os.Exit(1)
	}
	defer rl.Close()

	for {
		line, err := rl.Readline()
		if err == readline.ErrInterrupt {
			if len(line) == 0 {
				break // Ctrl+C on empty line → exit
			}
			continue // Ctrl+C mid-input → clear and re-prompt
		}
		if err == io.EOF {
			break // Ctrl+D → exit
		}

		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		args := tokenise(line)
		if len(args) == 0 {
			continue
		}

		// Handle client-side commands before sending to server.
		switch strings.ToUpper(args[0]) {
		case "QUIT", "EXIT":
			fmt.Println("Bye!")
			return
		case "CLEAR":
			rl.Clean()
			fmt.Print("\033[H\033[2J") // ANSI clear screen
			continue
		case "HELP":
			printHelp()
			continue
		}

		val, err := cl.Do(args...)
		if err != nil {
			fmt.Fprintf(os.Stderr, "(error) %v\n", err)
			continue
		}
		fmt.Println(val.Display())
	}
}

// ─── flags ────────────────────────────────────────────────────────────────────

func parseFlags(args []string) (host string, port int) {
	host = defaultHost
	port = defaultPort

	for i := 0; i < len(args); i++ {
		arg := args[i]
		val := ""

		// Support both "--key value" and "--key=value".
		if strings.Contains(arg, "=") {
			parts := strings.SplitN(arg, "=", 2)
			arg, val = parts[0], parts[1]
		} else if i+1 < len(args) {
			val = args[i+1]
		}

		switch arg {
		case "--host", "-h":
			host = val
			if !strings.Contains(args[i], "=") {
				i++
			}
		case "--port", "-p":
			n, err := strconv.Atoi(val)
			if err != nil || n < 1 || n > 65535 {
				fmt.Fprintf(os.Stderr, "invalid port %q\n", val)
				os.Exit(1)
			}
			port = n
			if !strings.Contains(args[i], "=") {
				i++
			}
		case "--help":
			fmt.Println("Usage: valkey-cli [--host HOST] [--port PORT]")
			os.Exit(0)
		}
	}
	return host, port
}

// ─── tab completion ───────────────────────────────────────────────────────────

func completer() readline.AutoCompleter {
	cmds := []string{
		// String commands
		"SET", "GET", "DEL", "MSET", "MGET", "APPEND", "STRLEN",
		"INCR", "INCRBY", "DECR", "DECRBY", "GETSET", "SETNX", "SETEX", "PSETEX",
		// Key commands
		"EXISTS", "EXPIRE", "PEXPIRE", "TTL", "PTTL", "PERSIST",
		"RENAME", "TYPE", "KEYS", "SCAN",
		// List commands
		"LPUSH", "RPUSH", "LPOP", "RPOP", "LRANGE", "LLEN",
		// Hash commands
		"HSET", "HGET", "HDEL", "HLEN", "HGETALL", "HKEYS", "HVALS", "HEXISTS",
		// JSON commands
		"JSON.SET", "JSON.GET", "JSON.DEL", "JSON.TYPE", "JSON.NUMINCRBY",
		// Set commands
		"SADD", "SREM", "SMEMBERS", "SISMEMBER",
		// Sorted set commands
		"ZADD", "ZRANGE", "ZRANK", "ZREM", "ZSCORE",
		// Server commands
		"PING", "DBSIZE", "FLUSHDB", "FLUSHALL", "INFO", "SELECT", "AUTH",
		// Client-side
		"QUIT", "EXIT", "CLEAR", "HELP",
	}

	items := make([]readline.PrefixCompleterInterface, 0, len(cmds)*2+1)
	for _, cmd := range cmds {
		items = append(items, readline.PcItem(cmd))
		items = append(items, readline.PcItem(strings.ToLower(cmd)))
	}
	// CONFIG has subcommands.
	items = append(items,
		readline.PcItem("CONFIG",
			readline.PcItem("GET"), readline.PcItem("SET"), readline.PcItem("REWRITE"),
		),
		readline.PcItem("config",
			readline.PcItem("get"), readline.PcItem("set"), readline.PcItem("rewrite"),
		),
	)

	return readline.NewPrefixCompleter(items...)
}

// ─── helpers ──────────────────────────────────────────────────────────────────

// tokenise splits a command line into tokens, respecting quoted strings.
// Both single and double quotes are supported:
//
//	SET key "hello world"          →  ["SET", "key", "hello world"]
//	JSON.SET k $ '{"name":"Alice"}'  →  ["JSON.SET", "k", "$", "{\"name\":\"Alice\"}"]
func tokenise(line string) []string {
	var tokens []string
	var cur strings.Builder
	var quoteChar byte // 0 = not in quote, '"' or '\'' = in that quote

	for i := 0; i < len(line); i++ {
		c := line[i]
		switch {
		case quoteChar == 0 && (c == '"' || c == '\''):
			quoteChar = c
		case quoteChar != 0 && c == quoteChar:
			quoteChar = 0
		case c == ' ' && quoteChar == 0:
			if cur.Len() > 0 {
				tokens = append(tokens, cur.String())
				cur.Reset()
			}
		default:
			cur.WriteByte(c)
		}
	}
	if cur.Len() > 0 {
		tokens = append(tokens, cur.String())
	}
	return tokens
}

// prompt returns the CLI prompt string, e.g. "127.0.0.1:6379> "
func prompt(addr string) string {
	return addr + "> "
}

// historyFile returns a path for storing command history.
func historyFile() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return ""
	}
	return home + "/.valkey_cli_history"
}

func printHelp() {
	fmt.Print(`
Valkey CLI — built-in commands:
  HELP          show this message
  CLEAR         clear the screen
  QUIT / EXIT   disconnect and exit

Server commands:
  Strings:
    SET key value [EX seconds | PX milliseconds]
    GET key
    DEL key [key ...]

  TTL / Expiry:
    EXPIRE key seconds          set TTL in seconds
    PEXPIRE key milliseconds    set TTL in milliseconds
    TTL key                     get remaining TTL in seconds
    PTTL key                    get remaining TTL in milliseconds
    PERSIST key                 remove TTL

  Hashes:
    HSET key field value [field value ...]
    HGET key field
    HDEL key field [field ...]
    HGETALL key
    HLEN key
    HEXISTS key field
    HKEYS key
    HVALS key

  JSON:
    JSON.SET key path value      set JSON value at path
    JSON.GET key [path]          get JSON value (default path: $)
    JSON.DEL key [path]          delete value at path (default: whole key)
    JSON.TYPE key [path]         get type at path (object, array, string, number, boolean, null)
    JSON.NUMINCRBY key path n    increment number at path by n

  Server:
    PING

Tab-completion is available for all command names (upper and lowercase).
Use arrow keys to navigate history.

Examples:
  SET name valkey
  SET session token123 EX 3600
  GET name
  EXPIRE name 60
  TTL name
  HSET user:1 name Alice age 30
  HGET user:1 name
  HGETALL user:1
  JSON.SET user $ '{"name":"Alice","age":30}'
  JSON.GET user $.name
  JSON.SET user $.age 31
  JSON.NUMINCRBY user $.age 1
  JSON.DEL user $.age
  JSON.TYPE user $.name
  DEL name
  PING
`)
}
