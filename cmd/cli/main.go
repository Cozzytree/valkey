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
	return readline.NewPrefixCompleter(
		// String commands
		readline.PcItem("SET"),
		readline.PcItem("GET"),
		readline.PcItem("DEL"),
		readline.PcItem("MSET"),
		readline.PcItem("MGET"),
		readline.PcItem("APPEND"),
		readline.PcItem("STRLEN"),
		readline.PcItem("INCR"),
		readline.PcItem("INCRBY"),
		readline.PcItem("DECR"),
		readline.PcItem("DECRBY"),
		readline.PcItem("GETSET"),
		readline.PcItem("SETNX"),
		readline.PcItem("SETEX"),
		readline.PcItem("PSETEX"),
		// Key commands
		readline.PcItem("EXISTS"),
		readline.PcItem("EXPIRE"),
		readline.PcItem("PEXPIRE"),
		readline.PcItem("TTL"),
		readline.PcItem("PTTL"),
		readline.PcItem("PERSIST"),
		readline.PcItem("RENAME"),
		readline.PcItem("TYPE"),
		readline.PcItem("KEYS"),
		readline.PcItem("SCAN"),
		// List commands
		readline.PcItem("LPUSH"),
		readline.PcItem("RPUSH"),
		readline.PcItem("LPOP"),
		readline.PcItem("RPOP"),
		readline.PcItem("LRANGE"),
		readline.PcItem("LLEN"),
		// Hash commands
		readline.PcItem("HSET"),
		readline.PcItem("HGET"),
		readline.PcItem("HDEL"),
		readline.PcItem("HGETALL"),
		readline.PcItem("HKEYS"),
		readline.PcItem("HVALS"),
		readline.PcItem("HEXISTS"),
		// Set commands
		readline.PcItem("SADD"),
		readline.PcItem("SREM"),
		readline.PcItem("SMEMBERS"),
		readline.PcItem("SISMEMBER"),
		// Sorted set commands
		readline.PcItem("ZADD"),
		readline.PcItem("ZRANGE"),
		readline.PcItem("ZRANK"),
		readline.PcItem("ZREM"),
		readline.PcItem("ZSCORE"),
		// Server commands
		readline.PcItem("PING"),
		readline.PcItem("DBSIZE"),
		readline.PcItem("FLUSHDB"),
		readline.PcItem("FLUSHALL"),
		readline.PcItem("INFO"),
		readline.PcItem("SELECT"),
		readline.PcItem("AUTH"),
		readline.PcItem("CONFIG",
			readline.PcItem("GET"),
			readline.PcItem("SET"),
			readline.PcItem("REWRITE"),
		),
		// Client-side
		readline.PcItem("QUIT"),
		readline.PcItem("EXIT"),
		readline.PcItem("CLEAR"),
		readline.PcItem("HELP"),
	)
}

// ─── helpers ──────────────────────────────────────────────────────────────────

// tokenise splits a command line into tokens, respecting double-quoted strings.
//
//	SET key "hello world"  →  ["SET", "key", "hello world"]
func tokenise(line string) []string {
	var tokens []string
	var cur strings.Builder
	inQuote := false

	for i := 0; i < len(line); i++ {
		c := line[i]
		switch {
		case c == '"' && !inQuote:
			inQuote = true
		case c == '"' && inQuote:
			inQuote = false
		case c == ' ' && !inQuote:
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

All other input is sent directly to the server as a RESP command.
Tab-completion is available for all known command names.
Use arrow keys to navigate history.

Examples:
  SET name valkey
  GET name
  SET greeting "hello world"
  DEL name
  PING
`)
}
