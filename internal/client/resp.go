package client

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/valkey/valkey/internal/proto"
)

// ValueType identifies the RESP type of a Value.
type ValueType int

const (
	TypeSimpleString ValueType = iota
	TypeError
	TypeInteger
	TypeBulkString
	TypeNull
	TypeArray
)

// Value is a parsed RESP response value.
type Value struct {
	typ     ValueType
	str     string   // TypeSimpleString, TypeError
	integer int64    // TypeInteger
	bulk    []byte   // TypeBulkString (nil = null bulk)
	elems   []*Value // TypeArray
}

func (v *Value) Type() ValueType { return v.typ }

// IsNull returns true for null bulk strings and null arrays.
func (v *Value) IsNull() bool {
	return v.typ == TypeNull || (v.typ == TypeBulkString && v.bulk == nil)
}

// String returns the string payload for SimpleString / Error types.
func (v *Value) String() string { return v.str }

// Integer returns the integer payload.
func (v *Value) Integer() int64 { return v.integer }

// Bytes returns the raw bytes for a bulk string (nil if null).
func (v *Value) Bytes() []byte { return v.bulk }

// Elems returns the child values for an array.
func (v *Value) Elems() []*Value { return v.elems }

// IsError returns true when the server sent a RESP error reply.
func (v *Value) IsError() bool { return v.typ == TypeError }

// Display formats v the same way redis-cli does.
func (v *Value) Display() string {
	return display(v, 0)
}

func display(v *Value, depth int) string {
	indent := strings.Repeat("   ", depth)
	switch v.typ {
	case TypeSimpleString:
		return fmt.Sprintf("%q", v.str)
	case TypeError:
		return "(error) " + v.str
	case TypeInteger:
		return fmt.Sprintf("(integer) %d", v.integer)
	case TypeNull:
		return "(nil)"
	case TypeBulkString:
		if v.bulk == nil {
			return "(nil)"
		}
		return fmt.Sprintf("%q", v.bulk)
	case TypeArray:
		if v.elems == nil {
			return "(empty array)"
		}
		var b strings.Builder
		for i, e := range v.elems {
			if i > 0 {
				b.WriteString("\n")
			}
			fmt.Fprintf(&b, "%s%d) %s", indent, i+1, display(e, depth+1))
		}
		return b.String()
	}
	return ""
}

// ─── parser ───────────────────────────────────────────────────────────────────

// readValue reads one complete RESP value from r.
func readValue(r *proto.BufReader) (*Value, error) {
	prefix, err := r.ReadByte()
	if err != nil {
		return nil, err
	}

	switch prefix {
	case '+': // simple string
		line, err := r.ReadLine()
		if err != nil {
			return nil, err
		}
		return &Value{typ: TypeSimpleString, str: string(line)}, nil

	case '-': // error
		line, err := r.ReadLine()
		if err != nil {
			return nil, err
		}
		return &Value{typ: TypeError, str: string(line)}, nil

	case ':': // integer
		line, err := r.ReadLine()
		if err != nil {
			return nil, err
		}
		n, err := strconv.ParseInt(string(line), 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid integer %q", line)
		}
		return &Value{typ: TypeInteger, integer: n}, nil

	case '$': // bulk string
		line, err := r.ReadLine()
		if err != nil {
			return nil, err
		}
		length, err := strconv.Atoi(string(line))
		if err != nil {
			return nil, fmt.Errorf("invalid bulk length %q", line)
		}
		if length < 0 {
			return &Value{typ: TypeBulkString, bulk: nil}, nil
		}
		data, err := r.ReadN(length)
		if err != nil {
			return nil, err
		}
		if _, err := r.Discard(2); err != nil { // consume \r\n
			return nil, err
		}
		return &Value{typ: TypeBulkString, bulk: data}, nil

	case '*': // array
		line, err := r.ReadLine()
		if err != nil {
			return nil, err
		}
		count, err := strconv.Atoi(string(line))
		if err != nil {
			return nil, fmt.Errorf("invalid array count %q", line)
		}
		if count < 0 {
			return &Value{typ: TypeArray, elems: nil}, nil
		}
		elems := make([]*Value, count)
		for i := range elems {
			elems[i], err = readValue(r)
			if err != nil {
				return nil, fmt.Errorf("array element %d: %w", i, err)
			}
		}
		return &Value{typ: TypeArray, elems: elems}, nil

	default:
		return nil, fmt.Errorf("unknown RESP prefix %q", prefix)
	}
}
