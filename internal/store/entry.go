package store

import (
	"errors"
	"time"
)

// ErrWrongType is returned when a command is used against a key holding
// a value of the wrong type (e.g. GET on a hash key).
var ErrWrongType = errors.New("WRONGTYPE Operation against a key holding the wrong kind of value")

// EntryType tags the data type stored under a key.
type EntryType uint8

const (
	TypeString EntryType = iota
	TypeHash
)

// Entry is a typed value in the store. Every key maps to exactly one Entry.
type Entry struct {
	Type     EntryType
	Str      []byte            // populated when Type == TypeString
	Hash     map[string][]byte // populated when Type == TypeHash
	ExpireAt time.Time         // zero value = no expiry
}
