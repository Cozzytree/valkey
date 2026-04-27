package server

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/valkey/valkey/internal/config"
)

// Command categories matching Redis ACL categories.
// Each category maps to a set of command names.
var commandCategories = map[string][]string{
	"read": {
		"GET", "HGET", "HGETALL", "HKEYS", "HVALS", "HLEN", "HEXISTS",
		"LINDEX", "LLEN", "LRANGE",
		"JSON.GET", "JSON.TYPE",
		"TTL", "PTTL", "EXISTS", "DBSIZE",
	},
	"write": {
		"SET", "DEL", "HSET", "HDEL",
		"LPUSH", "RPUSH", "LPOP", "RPOP", "LSET", "LINSERT", "LREM", "LTRIM",
		"JSON.SET", "JSON.DEL", "JSON.NUMINCRBY",
		"EXPIRE", "PEXPIRE", "PERSIST",
	},
	"string": {"SET", "GET"},
	"hash":   {"HSET", "HGET", "HDEL", "HGETALL", "HLEN", "HEXISTS", "HKEYS", "HVALS"},
	"list":   {"LPUSH", "RPUSH", "LPOP", "RPOP", "LLEN", "LRANGE", "LINDEX", "LSET", "LINSERT", "LREM", "LTRIM"},
	"json":   {"JSON.SET", "JSON.GET", "JSON.DEL", "JSON.TYPE", "JSON.NUMINCRBY"},
	"keyspace": {
		"DEL", "EXISTS", "EXPIRE", "PEXPIRE", "TTL", "PTTL", "PERSIST",
	},
	"server":     {"DBSIZE", "PING", "AUTH", "ACL"},
	"connection": {"AUTH", "PING", "QUIT"},
	"fast": {
		"GET", "SET", "HGET", "HSET", "LPUSH", "RPUSH", "LPOP", "RPOP",
		"LLEN", "LINDEX", "PING", "EXISTS", "DEL",
	},
	"slow": {
		"HGETALL", "LRANGE", "JSON.GET", "DBSIZE",
	},
	"dangerous": {"ACL"},
	"admin":     {"ACL", "DBSIZE"},
	"all":       {}, // special: means all commands
}

// categoryLookup maps command name → set of categories it belongs to.
var categoryLookup map[string]map[string]bool

func init() {
	categoryLookup = make(map[string]map[string]bool)
	for cat, cmds := range commandCategories {
		if cat == "all" {
			continue
		}
		for _, cmd := range cmds {
			if categoryLookup[cmd] == nil {
				categoryLookup[cmd] = make(map[string]bool)
			}
			categoryLookup[cmd][cat] = true
		}
	}
}

// ACLUser represents an ACL user with permissions.
type ACLUser struct {
	Name     string
	Enabled  bool
	NoPass   bool            // if true, any password (or none) works
	passwords map[string]bool // SHA-256 hashes of accepted passwords

	// Command permissions.
	AllCommands    bool
	AllowedCommands map[string]bool // explicitly allowed commands
	DeniedCommands  map[string]bool // explicitly denied commands

	// Key pattern permissions.
	AllKeys     bool
	KeyPatterns []string // glob patterns for allowed keys
}

func newACLUser(name string) *ACLUser {
	return &ACLUser{
		Name:            name,
		Enabled:         false,
		passwords:       make(map[string]bool),
		AllowedCommands: make(map[string]bool),
		DeniedCommands:  make(map[string]bool),
	}
}

// hashPassword returns the SHA-256 hex digest of a password.
func hashPassword(password string) string {
	h := sha256.Sum256([]byte(password))
	return hex.EncodeToString(h[:])
}

// CheckPassword returns true if the password matches one of the user's passwords,
// or if the user has nopass set.
func (u *ACLUser) CheckPassword(password string) bool {
	if u.NoPass {
		return true
	}
	return u.passwords[hashPassword(password)]
}

// CanExecute returns true if the user is allowed to execute the given command.
func (u *ACLUser) CanExecute(cmd string) bool {
	cmd = strings.ToUpper(cmd)

	// Explicitly denied always wins.
	if u.DeniedCommands[cmd] {
		return false
	}

	if u.AllCommands {
		return true
	}

	return u.AllowedCommands[cmd]
}

// CanAccessKey returns true if the user is allowed to access the given key.
func (u *ACLUser) CanAccessKey(key string) bool {
	if u.AllKeys {
		return true
	}
	for _, pattern := range u.KeyPatterns {
		if matchGlob(pattern, key) {
			return true
		}
	}
	return false
}

// ApplyRules parses and applies Redis-compatible ACL rule tokens.
// Rules: on, off, >password, <password, nopass, resetpass,
//
//	~pattern, allkeys, resetkeys,
//	+command, -command, allcommands, nocommands,
//	+@category, -@category
func (u *ACLUser) ApplyRules(rules []string) error {
	for _, rule := range rules {
		if err := u.applyRule(rule); err != nil {
			return fmt.Errorf("rule %q: %w", rule, err)
		}
	}
	return nil
}

func (u *ACLUser) applyRule(rule string) error {
	switch {
	case rule == "on":
		u.Enabled = true
	case rule == "off":
		u.Enabled = false

	case strings.HasPrefix(rule, ">"):
		password := rule[1:]
		if password == "" {
			return fmt.Errorf("empty password")
		}
		u.passwords[hashPassword(password)] = true
		u.NoPass = false

	case strings.HasPrefix(rule, "<"):
		password := rule[1:]
		delete(u.passwords, hashPassword(password))

	case rule == "nopass":
		u.NoPass = true
		u.passwords = make(map[string]bool) // clear all passwords

	case rule == "resetpass":
		u.NoPass = false
		u.passwords = make(map[string]bool)

	case strings.HasPrefix(rule, "~"):
		pattern := rule[1:]
		if pattern == "" {
			return fmt.Errorf("empty key pattern")
		}
		u.KeyPatterns = append(u.KeyPatterns, pattern)

	case rule == "allkeys":
		u.AllKeys = true
		u.KeyPatterns = nil

	case rule == "resetkeys":
		u.AllKeys = false
		u.KeyPatterns = nil

	case rule == "allcommands", rule == "+@all":
		u.AllCommands = true
		u.AllowedCommands = make(map[string]bool)
		u.DeniedCommands = make(map[string]bool)

	case rule == "nocommands", rule == "-@all":
		u.AllCommands = false
		u.AllowedCommands = make(map[string]bool)
		u.DeniedCommands = make(map[string]bool)

	case strings.HasPrefix(rule, "+@"):
		category := strings.ToLower(rule[2:])
		cmds, ok := commandCategories[category]
		if !ok {
			return fmt.Errorf("unknown category %q", category)
		}
		if category == "all" {
			u.AllCommands = true
		} else {
			for _, cmd := range cmds {
				u.AllowedCommands[cmd] = true
				delete(u.DeniedCommands, cmd)
			}
		}

	case strings.HasPrefix(rule, "-@"):
		category := strings.ToLower(rule[2:])
		cmds, ok := commandCategories[category]
		if !ok {
			return fmt.Errorf("unknown category %q", category)
		}
		if category == "all" {
			u.AllCommands = false
			u.AllowedCommands = make(map[string]bool)
		} else {
			for _, cmd := range cmds {
				u.DeniedCommands[cmd] = true
				delete(u.AllowedCommands, cmd)
			}
		}

	case strings.HasPrefix(rule, "+"):
		cmd := strings.ToUpper(rule[1:])
		if cmd == "" {
			return fmt.Errorf("empty command name")
		}
		u.AllowedCommands[cmd] = true
		delete(u.DeniedCommands, cmd)

	case strings.HasPrefix(rule, "-"):
		cmd := strings.ToUpper(rule[1:])
		if cmd == "" {
			return fmt.Errorf("empty command name")
		}
		u.DeniedCommands[cmd] = true
		delete(u.AllowedCommands, cmd)

	default:
		return fmt.Errorf("unrecognised ACL rule %q", rule)
	}
	return nil
}

// Describe returns the user definition as a Redis-compatible ACL rule string.
func (u *ACLUser) Describe() string {
	var parts []string
	parts = append(parts, "user", u.Name)

	if u.Enabled {
		parts = append(parts, "on")
	} else {
		parts = append(parts, "off")
	}

	if u.NoPass {
		parts = append(parts, "nopass")
	} else if len(u.passwords) > 0 {
		// Show hashed passwords prefixed with #
		hashes := make([]string, 0, len(u.passwords))
		for h := range u.passwords {
			hashes = append(hashes, h)
		}
		sort.Strings(hashes)
		for _, h := range hashes {
			parts = append(parts, "#"+h)
		}
	}

	if u.AllKeys {
		parts = append(parts, "~*")
	} else if len(u.KeyPatterns) > 0 {
		for _, p := range u.KeyPatterns {
			parts = append(parts, "~"+p)
		}
	}

	if u.AllCommands {
		parts = append(parts, "+@all")
	} else {
		if len(u.AllowedCommands) > 0 {
			cmds := make([]string, 0, len(u.AllowedCommands))
			for c := range u.AllowedCommands {
				cmds = append(cmds, c)
			}
			sort.Strings(cmds)
			for _, c := range cmds {
				parts = append(parts, "+"+strings.ToLower(c))
			}
		}
		if len(u.DeniedCommands) > 0 {
			cmds := make([]string, 0, len(u.DeniedCommands))
			for c := range u.DeniedCommands {
				cmds = append(cmds, c)
			}
			sort.Strings(cmds)
			for _, c := range cmds {
				parts = append(parts, "-"+strings.ToLower(c))
			}
		}
	}

	return strings.Join(parts, " ")
}

// ACL manages all ACL users and provides authentication and authorization.
type ACL struct {
	mu    sync.RWMutex
	users map[string]*ACLUser
}

// NewACL creates an ACL system initialized from the server config.
// If requirepass is set, the "default" user gets that password.
// Otherwise, the default user has nopass + allcommands + allkeys (open access).
func NewACL(cfg *config.SecurityConfig) *ACL {
	acl := &ACL{
		users: make(map[string]*ACLUser),
	}

	defaultUser := newACLUser("default")
	defaultUser.Enabled = true
	defaultUser.AllCommands = true
	defaultUser.AllKeys = true

	if cfg.RequirePass != "" {
		defaultUser.passwords[hashPassword(cfg.RequirePass)] = true
	} else {
		defaultUser.NoPass = true
	}

	acl.users["default"] = defaultUser
	return acl
}

// Required returns true if the default user requires a password.
func (a *ACL) Required() bool {
	a.mu.RLock()
	defer a.mu.RUnlock()
	defaultUser := a.users["default"]
	return defaultUser != nil && !defaultUser.NoPass
}

// Authenticate checks credentials. username="" means "default".
// Returns the user on success, nil on failure.
func (a *ACL) Authenticate(username, password string) *ACLUser {
	if username == "" {
		username = "default"
	}
	a.mu.RLock()
	defer a.mu.RUnlock()

	user, ok := a.users[username]
	if !ok || !user.Enabled {
		return nil
	}
	if !user.CheckPassword(password) {
		return nil
	}
	return user
}

// CheckCommand verifies that the user can execute cmd on the given keys.
// Returns nil if allowed, an error message otherwise.
func (a *ACL) CheckCommand(user *ACLUser, cmd string, keys []string) error {
	if user == nil {
		return fmt.Errorf("NOPERM user not authenticated")
	}

	a.mu.RLock()
	defer a.mu.RUnlock()

	if !user.CanExecute(cmd) {
		return fmt.Errorf("NOPERM this user has no permissions to run the '%s' command", strings.ToLower(cmd))
	}

	for _, key := range keys {
		if !user.CanAccessKey(key) {
			return fmt.Errorf("NOPERM this user has no permissions to access the '%s' key", key)
		}
	}

	return nil
}

// SetUser creates or updates a user with the given rules.
func (a *ACL) SetUser(name string, rules []string) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	user, ok := a.users[name]
	if !ok {
		user = newACLUser(name)
		a.users[name] = user
	}

	return user.ApplyRules(rules)
}

// GetUser returns a copy of user info (nil if not found).
func (a *ACL) GetUser(name string) *ACLUser {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.users[name]
}

// DelUser deletes users by name. Cannot delete "default".
// Returns the number of users deleted.
func (a *ACL) DelUser(names []string) int {
	a.mu.Lock()
	defer a.mu.Unlock()

	deleted := 0
	for _, name := range names {
		if name == "default" {
			continue
		}
		if _, ok := a.users[name]; ok {
			delete(a.users, name)
			deleted++
		}
	}
	return deleted
}

// List returns all users in ACL rule format.
func (a *ACL) List() []string {
	a.mu.RLock()
	defer a.mu.RUnlock()

	result := make([]string, 0, len(a.users))
	for _, user := range a.users {
		result = append(result, user.Describe())
	}
	sort.Strings(result)
	return result
}

// Users returns all usernames, sorted.
func (a *ACL) Users() []string {
	a.mu.RLock()
	defer a.mu.RUnlock()

	names := make([]string, 0, len(a.users))
	for name := range a.users {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// Categories returns all available command category names.
func (a *ACL) Categories() []string {
	cats := make([]string, 0, len(commandCategories))
	for cat := range commandCategories {
		cats = append(cats, cat)
	}
	sort.Strings(cats)
	return cats
}

// CategoryCommands returns all commands in a category.
func (a *ACL) CategoryCommands(category string) ([]string, bool) {
	cmds, ok := commandCategories[strings.ToLower(category)]
	if !ok {
		return nil, false
	}
	result := make([]string, len(cmds))
	copy(result, cmds)
	sort.Strings(result)
	return result, true
}

// matchGlob performs simple glob matching supporting * and ?.
func matchGlob(pattern, str string) bool {
	return globMatch(pattern, str, 0, 0)
}

func globMatch(pattern, str string, pi, si int) bool {
	for pi < len(pattern) {
		if si >= len(str) {
			// Only trailing *'s can match empty remainder.
			for pi < len(pattern) {
				if pattern[pi] != '*' {
					return false
				}
				pi++
			}
			return true
		}

		switch pattern[pi] {
		case '*':
			// Try matching * as zero or more characters.
			for s := si; s <= len(str); s++ {
				if globMatch(pattern, str, pi+1, s) {
					return true
				}
			}
			return false
		case '?':
			pi++
			si++
		case '[':
			// Simple character class [abc] or [a-z].
			pi++
			negate := false
			if pi < len(pattern) && pattern[pi] == '^' {
				negate = true
				pi++
			}
			matched := false
			for pi < len(pattern) && pattern[pi] != ']' {
				if pi+2 < len(pattern) && pattern[pi+1] == '-' {
					if str[si] >= pattern[pi] && str[si] <= pattern[pi+2] {
						matched = true
					}
					pi += 3
				} else {
					if str[si] == pattern[pi] {
						matched = true
					}
					pi++
				}
			}
			if pi < len(pattern) {
				pi++ // skip ']'
			}
			if matched == negate {
				return false
			}
			si++
		default:
			if pattern[pi] != str[si] {
				return false
			}
			pi++
			si++
		}
	}
	return si >= len(str)
}

// extractKeys returns the key arguments from a command for ACL key checking.
// This maps commands to their key positions.
func extractKeys(cmd string, args [][]byte) []string {
	cmd = strings.ToUpper(cmd)

	switch cmd {
	// No keys
	case "PING", "AUTH", "QUIT", "DBSIZE", "ACL":
		return nil

	// Single key at args[0]
	case "GET", "SET", "DEL", "EXISTS", "EXPIRE", "PEXPIRE", "TTL", "PTTL", "PERSIST",
		"HSET", "HGET", "HDEL", "HGETALL", "HLEN", "HEXISTS", "HKEYS", "HVALS",
		"LPUSH", "RPUSH", "LPOP", "RPOP", "LLEN", "LRANGE", "LINDEX", "LSET", "LINSERT", "LREM", "LTRIM",
		"JSON.SET", "JSON.GET", "JSON.DEL", "JSON.TYPE", "JSON.NUMINCRBY":
		if len(args) > 0 {
			return []string{string(args[0])}
		}
		return nil
	}

	// Unknown command — treat first arg as key if present (conservative).
	if len(args) > 0 {
		return []string{string(args[0])}
	}
	return nil
}
