package client

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"
)

// ErrNil is returned when the server replies with a null bulk string (key does not exist).
var ErrNil = errors.New("valkey: nil")

// errFromValue converts a RESP error Value into a Go error.
func errFromValue(v *Value) error {
	if v.IsError() {
		return errors.New(v.String())
	}
	return nil
}

// ─── strings ─────────────────────────────────────────────────────────────────

// Set stores key → value. Returns nil on success.
func (c *Client) Set(key, value string) error {
	v, err := c.Do("SET", key, value)
	if err != nil {
		return err
	}
	return errFromValue(v)
}

// SetEX stores key → value with a TTL in seconds.
func (c *Client) SetEX(key, value string, seconds int) error {
	v, err := c.Do("SET", key, value, "EX", strconv.Itoa(seconds))
	if err != nil {
		return err
	}
	return errFromValue(v)
}

// SetPX stores key → value with a TTL in milliseconds.
func (c *Client) SetPX(key, value string, milliseconds int) error {
	v, err := c.Do("SET", key, value, "PX", strconv.Itoa(milliseconds))
	if err != nil {
		return err
	}
	return errFromValue(v)
}

// Get retrieves the value for key. Returns ErrNil if the key does not exist.
func (c *Client) Get(key string) (string, error) {
	v, err := c.Do("GET", key)
	if err != nil {
		return "", err
	}
	if err := errFromValue(v); err != nil {
		return "", err
	}
	if v.IsNull() {
		return "", ErrNil
	}
	return string(v.Bytes()), nil
}

// GetBytes is like Get but returns []byte.
func (c *Client) GetBytes(key string) ([]byte, error) {
	v, err := c.Do("GET", key)
	if err != nil {
		return nil, err
	}
	if err := errFromValue(v); err != nil {
		return nil, err
	}
	if v.IsNull() {
		return nil, ErrNil
	}
	return v.Bytes(), nil
}

// Del deletes one or more keys. Returns the number of keys removed.
func (c *Client) Del(keys ...string) (int64, error) {
	args := make([]string, 0, 1+len(keys))
	args = append(args, "DEL")
	args = append(args, keys...)
	v, err := c.Do(args...)
	if err != nil {
		return 0, err
	}
	if err := errFromValue(v); err != nil {
		return 0, err
	}
	return v.Integer(), nil
}

// ─── TTL / expiry ────────────────────────────────────────────────────────────

// Expire sets a TTL on key. Returns true if the key exists.
func (c *Client) Expire(key string, seconds int) (bool, error) {
	v, err := c.Do("EXPIRE", key, strconv.Itoa(seconds))
	if err != nil {
		return false, err
	}
	if err := errFromValue(v); err != nil {
		return false, err
	}
	return v.Integer() == 1, nil
}

// PExpire sets a TTL in milliseconds. Returns true if the key exists.
func (c *Client) PExpire(key string, milliseconds int) (bool, error) {
	v, err := c.Do("PEXPIRE", key, strconv.Itoa(milliseconds))
	if err != nil {
		return false, err
	}
	if err := errFromValue(v); err != nil {
		return false, err
	}
	return v.Integer() == 1, nil
}

// TTL returns the remaining time-to-live for key.
// Returns -2 if the key does not exist, -1 if the key has no TTL.
func (c *Client) TTL(key string) (time.Duration, error) {
	v, err := c.Do("TTL", key)
	if err != nil {
		return 0, err
	}
	if err := errFromValue(v); err != nil {
		return 0, err
	}
	secs := v.Integer()
	if secs < 0 {
		return time.Duration(secs), nil // -1 or -2 as sentinel values
	}
	return time.Duration(secs) * time.Second, nil
}

// PTTL returns the remaining TTL in milliseconds.
func (c *Client) PTTL(key string) (time.Duration, error) {
	v, err := c.Do("PTTL", key)
	if err != nil {
		return 0, err
	}
	if err := errFromValue(v); err != nil {
		return 0, err
	}
	ms := v.Integer()
	if ms < 0 {
		return time.Duration(ms), nil
	}
	return time.Duration(ms) * time.Millisecond, nil
}

// Persist removes the TTL from key. Returns true if the TTL was removed.
func (c *Client) Persist(key string) (bool, error) {
	v, err := c.Do("PERSIST", key)
	if err != nil {
		return false, err
	}
	if err := errFromValue(v); err != nil {
		return false, err
	}
	return v.Integer() == 1, nil
}

// ─── hashes ──────────────────────────────────────────────────────────────────

// HSet sets field/value pairs on a hash key. Fields are alternating field, value strings.
// Returns the number of new fields added (not updated).
func (c *Client) HSet(key string, fieldValues ...string) (int64, error) {
	if len(fieldValues)%2 != 0 {
		return 0, fmt.Errorf("valkey: HSet requires even number of field/value args, got %d", len(fieldValues))
	}
	args := make([]string, 0, 1+1+len(fieldValues))
	args = append(args, "HSET", key)
	args = append(args, fieldValues...)
	v, err := c.Do(args...)
	if err != nil {
		return 0, err
	}
	if err := errFromValue(v); err != nil {
		return 0, err
	}
	return v.Integer(), nil
}

// HGet returns the value of a hash field. Returns ErrNil if the field or key doesn't exist.
func (c *Client) HGet(key, field string) (string, error) {
	v, err := c.Do("HGET", key, field)
	if err != nil {
		return "", err
	}
	if err := errFromValue(v); err != nil {
		return "", err
	}
	if v.IsNull() {
		return "", ErrNil
	}
	return string(v.Bytes()), nil
}

// HDel removes fields from a hash. Returns the number of fields deleted.
func (c *Client) HDel(key string, fields ...string) (int64, error) {
	args := make([]string, 0, 1+1+len(fields))
	args = append(args, "HDEL", key)
	args = append(args, fields...)
	v, err := c.Do(args...)
	if err != nil {
		return 0, err
	}
	if err := errFromValue(v); err != nil {
		return 0, err
	}
	return v.Integer(), nil
}

// HGetAll returns all field/value pairs as a map. Returns an empty map if the key doesn't exist.
func (c *Client) HGetAll(key string) (map[string]string, error) {
	v, err := c.Do("HGETALL", key)
	if err != nil {
		return nil, err
	}
	if err := errFromValue(v); err != nil {
		return nil, err
	}
	elems := v.Elems()
	result := make(map[string]string, len(elems)/2)
	for i := 0; i+1 < len(elems); i += 2 {
		result[string(elems[i].Bytes())] = string(elems[i+1].Bytes())
	}
	return result, nil
}

// HLen returns the number of fields in a hash.
func (c *Client) HLen(key string) (int64, error) {
	v, err := c.Do("HLEN", key)
	if err != nil {
		return 0, err
	}
	if err := errFromValue(v); err != nil {
		return 0, err
	}
	return v.Integer(), nil
}

// HExists returns whether a field exists in a hash.
func (c *Client) HExists(key, field string) (bool, error) {
	v, err := c.Do("HEXISTS", key, field)
	if err != nil {
		return false, err
	}
	if err := errFromValue(v); err != nil {
		return false, err
	}
	return v.Integer() == 1, nil
}

// HKeys returns all field names in a hash.
func (c *Client) HKeys(key string) ([]string, error) {
	v, err := c.Do("HKEYS", key)
	if err != nil {
		return nil, err
	}
	if err := errFromValue(v); err != nil {
		return nil, err
	}
	elems := v.Elems()
	result := make([]string, len(elems))
	for i, e := range elems {
		result[i] = string(e.Bytes())
	}
	return result, nil
}

// HVals returns all values in a hash.
func (c *Client) HVals(key string) ([]string, error) {
	v, err := c.Do("HVALS", key)
	if err != nil {
		return nil, err
	}
	if err := errFromValue(v); err != nil {
		return nil, err
	}
	elems := v.Elems()
	result := make([]string, len(elems))
	for i, e := range elems {
		result[i] = string(e.Bytes())
	}
	return result, nil
}

// ─── JSON ────────────────────────────────────────────────────────────────────

// JSONSet stores a JSON value at path under key.
// The value is marshalled to JSON before sending.
func (c *Client) JSONSet(key, path string, value any) error {
	data, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("valkey: JSONSet marshal: %w", err)
	}
	v, err := c.Do("JSON.SET", key, path, string(data))
	if err != nil {
		return err
	}
	return errFromValue(v)
}

// JSONGet retrieves the JSON value at path under key as a raw JSON string.
// If no path is given, defaults to "$" (root).
func (c *Client) JSONGet(key string, path ...string) (string, error) {
	p := "$"
	if len(path) > 0 {
		p = path[0]
	}
	v, err := c.Do("JSON.GET", key, p)
	if err != nil {
		return "", err
	}
	if err := errFromValue(v); err != nil {
		return "", err
	}
	if v.IsNull() {
		return "", ErrNil
	}
	return string(v.Bytes()), nil
}

// JSONDel deletes the value at path. If no path is given, deletes the whole key.
// Returns the number of paths deleted.
func (c *Client) JSONDel(key string, path ...string) (int64, error) {
	p := "$"
	if len(path) > 0 {
		p = path[0]
	}
	v, err := c.Do("JSON.DEL", key, p)
	if err != nil {
		return 0, err
	}
	if err := errFromValue(v); err != nil {
		return 0, err
	}
	return v.Integer(), nil
}

// JSONType returns the JSON type name at path.
func (c *Client) JSONType(key string, path ...string) (string, error) {
	p := "$"
	if len(path) > 0 {
		p = path[0]
	}
	v, err := c.Do("JSON.TYPE", key, p)
	if err != nil {
		return "", err
	}
	if err := errFromValue(v); err != nil {
		return "", err
	}
	if v.IsNull() {
		return "", ErrNil
	}
	return v.String(), nil
}

// JSONNumIncrBy increments the number at path by n and returns the new value.
func (c *Client) JSONNumIncrBy(key, path string, n float64) (float64, error) {
	v, err := c.Do("JSON.NUMINCRBY", key, path, strconv.FormatFloat(n, 'f', -1, 64))
	if err != nil {
		return 0, err
	}
	if err := errFromValue(v); err != nil {
		return 0, err
	}
	return strconv.ParseFloat(string(v.Bytes()), 64)
}

// ─── lists ──────────────────────────────────────────────────────────────────

// LPush prepends one or more values to a list. Returns the length of the list after the push.
func (c *Client) LPush(key string, values ...string) (int64, error) {
	args := make([]string, 0, 1+1+len(values))
	args = append(args, "LPUSH", key)
	args = append(args, values...)
	v, err := c.Do(args...)
	if err != nil {
		return 0, err
	}
	if err := errFromValue(v); err != nil {
		return 0, err
	}
	return v.Integer(), nil
}

// RPush appends one or more values to a list. Returns the length of the list after the push.
func (c *Client) RPush(key string, values ...string) (int64, error) {
	args := make([]string, 0, 1+1+len(values))
	args = append(args, "RPUSH", key)
	args = append(args, values...)
	v, err := c.Do(args...)
	if err != nil {
		return 0, err
	}
	if err := errFromValue(v); err != nil {
		return 0, err
	}
	return v.Integer(), nil
}

// LPop removes and returns the first element of a list. Returns ErrNil if the key does not exist.
func (c *Client) LPop(key string) (string, error) {
	v, err := c.Do("LPOP", key)
	if err != nil {
		return "", err
	}
	if err := errFromValue(v); err != nil {
		return "", err
	}
	if v.IsNull() {
		return "", ErrNil
	}
	return string(v.Bytes()), nil
}

// RPop removes and returns the last element of a list. Returns ErrNil if the key does not exist.
func (c *Client) RPop(key string) (string, error) {
	v, err := c.Do("RPOP", key)
	if err != nil {
		return "", err
	}
	if err := errFromValue(v); err != nil {
		return "", err
	}
	if v.IsNull() {
		return "", ErrNil
	}
	return string(v.Bytes()), nil
}

// LLen returns the number of elements in a list.
func (c *Client) LLen(key string) (int64, error) {
	v, err := c.Do("LLEN", key)
	if err != nil {
		return 0, err
	}
	if err := errFromValue(v); err != nil {
		return 0, err
	}
	return v.Integer(), nil
}

// LRange returns elements from index start to stop (inclusive). Negative indices count from the end.
func (c *Client) LRange(key string, start, stop int) ([]string, error) {
	v, err := c.Do("LRANGE", key, strconv.Itoa(start), strconv.Itoa(stop))
	if err != nil {
		return nil, err
	}
	if err := errFromValue(v); err != nil {
		return nil, err
	}
	elems := v.Elems()
	result := make([]string, len(elems))
	for i, e := range elems {
		result[i] = string(e.Bytes())
	}
	return result, nil
}

// LIndex returns the element at the given index. Negative indices count from the end.
// Returns ErrNil if the index is out of range or the key does not exist.
func (c *Client) LIndex(key string, index int) (string, error) {
	v, err := c.Do("LINDEX", key, strconv.Itoa(index))
	if err != nil {
		return "", err
	}
	if err := errFromValue(v); err != nil {
		return "", err
	}
	if v.IsNull() {
		return "", ErrNil
	}
	return string(v.Bytes()), nil
}

// LSet sets the element at the given index to value. Returns an error if the index is out of range.
func (c *Client) LSet(key string, index int, value string) error {
	v, err := c.Do("LSET", key, strconv.Itoa(index), value)
	if err != nil {
		return err
	}
	return errFromValue(v)
}

// LInsert inserts value before or after pivot in the list.
// Returns the length of the list after insert, or -1 if pivot was not found.
func (c *Client) LInsert(key, position, pivot, value string) (int64, error) {
	v, err := c.Do("LINSERT", key, position, pivot, value)
	if err != nil {
		return 0, err
	}
	if err := errFromValue(v); err != nil {
		return 0, err
	}
	return v.Integer(), nil
}

// LRem removes count occurrences of value from the list.
// count > 0: head to tail. count < 0: tail to head. count == 0: all.
// Returns the number of elements removed.
func (c *Client) LRem(key string, count int, value string) (int64, error) {
	v, err := c.Do("LREM", key, strconv.Itoa(count), value)
	if err != nil {
		return 0, err
	}
	if err := errFromValue(v); err != nil {
		return 0, err
	}
	return v.Integer(), nil
}

// LTrim trims the list to only contain elements in the range [start, stop].
func (c *Client) LTrim(key string, start, stop int) error {
	v, err := c.Do("LTRIM", key, strconv.Itoa(start), strconv.Itoa(stop))
	if err != nil {
		return err
	}
	return errFromValue(v)
}

// ─── server ──────────────────────────────────────────────────────────────────

// DBSize returns the number of keys in the database.
func (c *Client) DBSize() (int64, error) {
	v, err := c.Do("DBSIZE")
	if err != nil {
		return 0, err
	}
	if err := errFromValue(v); err != nil {
		return 0, err
	}
	return v.Integer(), nil
}

// Auth authenticates the connection with the given password (as "default" user).
func (c *Client) Auth(password string) error {
	v, err := c.Do("AUTH", password)
	if err != nil {
		return err
	}
	return errFromValue(v)
}

// AuthWithUser authenticates the connection with a username and password (ACL).
func (c *Client) AuthWithUser(username, password string) error {
	v, err := c.Do("AUTH", username, password)
	if err != nil {
		return err
	}
	return errFromValue(v)
}

// ACLWhoAmI returns the username of the current connection.
func (c *Client) ACLWhoAmI() (string, error) {
	v, err := c.Do("ACL", "WHOAMI")
	if err != nil {
		return "", err
	}
	if err := errFromValue(v); err != nil {
		return "", err
	}
	return string(v.Bytes()), nil
}

// ACLSetUser creates or modifies an ACL user with the given rules.
func (c *Client) ACLSetUser(username string, rules ...string) error {
	args := make([]string, 0, 2+len(rules))
	args = append(args, "ACL", "SETUSER", username)
	args = append(args, rules...)
	v, err := c.Do(args...)
	if err != nil {
		return err
	}
	return errFromValue(v)
}

// ACLDelUser deletes one or more ACL users. Returns the number deleted.
func (c *Client) ACLDelUser(usernames ...string) (int64, error) {
	args := make([]string, 0, 2+len(usernames))
	args = append(args, "ACL", "DELUSER")
	args = append(args, usernames...)
	v, err := c.Do(args...)
	if err != nil {
		return 0, err
	}
	if err := errFromValue(v); err != nil {
		return 0, err
	}
	return v.Integer(), nil
}

// ACLList returns all ACL users in rule format.
func (c *Client) ACLList() ([]string, error) {
	v, err := c.Do("ACL", "LIST")
	if err != nil {
		return nil, err
	}
	if err := errFromValue(v); err != nil {
		return nil, err
	}
	elems := v.Elems()
	result := make([]string, len(elems))
	for i, e := range elems {
		result[i] = string(e.Bytes())
	}
	return result, nil
}

// ACLUsers returns all ACL usernames.
func (c *Client) ACLUsers() ([]string, error) {
	v, err := c.Do("ACL", "USERS")
	if err != nil {
		return nil, err
	}
	if err := errFromValue(v); err != nil {
		return nil, err
	}
	elems := v.Elems()
	result := make([]string, len(elems))
	for i, e := range elems {
		result[i] = string(e.Bytes())
	}
	return result, nil
}

// ACLCat returns command categories, or commands in a category.
func (c *Client) ACLCat(category ...string) ([]string, error) {
	args := []string{"ACL", "CAT"}
	if len(category) > 0 {
		args = append(args, category[0])
	}
	v, err := c.Do(args...)
	if err != nil {
		return nil, err
	}
	if err := errFromValue(v); err != nil {
		return nil, err
	}
	elems := v.Elems()
	result := make([]string, len(elems))
	for i, e := range elems {
		result[i] = string(e.Bytes())
	}
	return result, nil
}

// Ping sends PING and returns the response (usually "PONG").
func (c *Client) Ping() (string, error) {
	v, err := c.Do("PING")
	if err != nil {
		return "", err
	}
	if err := errFromValue(v); err != nil {
		return "", err
	}
	return v.String(), nil
}
