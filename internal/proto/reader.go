// Package proto implements I/O primitives for the RESP (REdis Serialization
// Protocol) wire format used by Valkey clients.
//
// The central abstraction is Reader, a byte-oriented interface that lets the
// RESP parser request exactly the number of bytes it needs at each step.
// BufReader is the production implementation backed by a bufio.Reader; it is
// designed so that the RESP parser never has to deal with partial reads.
package proto

import (
	"bufio"
	"errors"
	"fmt"
	"io"
)

// ─── Errors ──────────────────────────────────────────────────────────────────

var (
	// ErrUnexpectedEOF is returned when the underlying stream closes before the
	// requested number of bytes is available.
	ErrUnexpectedEOF = errors.New("proto: unexpected EOF")

	// ErrBufferTooSmall is returned when the requested read size exceeds the
	// reader's internal buffer capacity.
	ErrBufferTooSmall = errors.New("proto: read size exceeds buffer capacity")

	// ErrLineTooLong is returned when ReadLine encounters a line that exceeds
	// the reader's maximum line length.
	ErrLineTooLong = errors.New("proto: line exceeds maximum length")
)

// ─── Reader interface ────────────────────────────────────────────────────────

// Reader is the core I/O interface consumed by the RESP parser.
// All methods must return exactly the data requested or an error; partial
// results are not permitted.
type Reader interface {
	// ReadN reads exactly n bytes and returns them as a fresh slice.
	// It blocks until n bytes are available or an error occurs.
	// Returns ErrUnexpectedEOF if the stream closes before n bytes arrive.
	ReadN(n int) ([]byte, error)

	// ReadLine reads bytes up to and including the next CRLF ("\r\n") and
	// returns the line content without the trailing CRLF.
	// Returns ErrLineTooLong if the line exceeds the implementation's limit.
	ReadLine() ([]byte, error)

	// ReadByte reads and returns a single byte.
	ReadByte() (byte, error)

	// PeekByte returns the next byte without consuming it.
	PeekByte() (byte, error)

	// Discard skips the next n bytes, returning the number actually discarded.
	Discard(n int) (int, error)

	// Buffered returns the number of bytes that can be read without blocking.
	Buffered() int
}

// ─── BufReader ───────────────────────────────────────────────────────────────

const (
	// DefaultBufSize is the default internal buffer size (32 KiB).
	DefaultBufSize = 32 * 1024
	// DefaultMaxLineLen caps the length of a single RESP simple string / error.
	DefaultMaxLineLen = 64 * 1024
)

// BufReaderOption configures a BufReader.
type BufReaderOption func(*BufReader)

// WithBufSize sets the internal bufio.Reader buffer size.
// This also becomes the default chunk size used by Read().
func WithBufSize(n int) BufReaderOption {
	return func(r *BufReader) {
		r.bufSize = n
		r.chunkSize = n // keep in sync unless overridden explicitly
	}
}

// WithChunkSize sets the maximum number of bytes consumed by a single Read()
// call when BufReader is used as an io.Reader.  Defaults to bufSize.
// A chunk size smaller than bufSize causes Read() to return data in smaller
// increments, which is useful for rate-limiting or testing.
func WithChunkSize(n int) BufReaderOption {
	return func(r *BufReader) { r.chunkSize = n }
}

// WithMaxLineLen sets the maximum accepted line length for ReadLine().
func WithMaxLineLen(n int) BufReaderOption {
	return func(r *BufReader) { r.maxLineLen = n }
}

// BufReader is a Reader backed by a bufio.Reader.
//
// It implements both the proto.Reader interface (exact-length reads for the
// RESP parser) and the standard io.Reader interface (chunked reads of at most
// chunkSize bytes, suitable for generic io pipelines).
type BufReader struct {
	rd         *bufio.Reader
	bufSize    int // internal bufio buffer capacity
	chunkSize  int // maximum bytes per Read() call (io.Reader)
	maxLineLen int // maximum bytes per ReadLine() call
}

// NewBufReader wraps r in a BufReader with the supplied options.
func NewBufReader(r io.Reader, opts ...BufReaderOption) *BufReader {
	br := &BufReader{
		bufSize:    DefaultBufSize,
		chunkSize:  DefaultBufSize,
		maxLineLen: DefaultMaxLineLen,
	}
	for _, o := range opts {
		o(br)
	}
	br.rd = bufio.NewReaderSize(r, br.bufSize)
	return br
}

// Read implements io.Reader.
//
// It reads up to min(len(p), chunkSize) bytes from the underlying stream,
// capping each call to the configured chunk size so that callers processing
// the data byte-by-byte (e.g. a streaming decoder) receive predictable
// increments that match the network read-buffer size from config.
//
// Unlike ReadN, Read may return fewer bytes than len(p) without an error —
// this satisfies the io.Reader contract.
func (r *BufReader) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	limit := len(p)
	if r.chunkSize > 0 && limit > r.chunkSize {
		limit = r.chunkSize
	}
	n, err := r.rd.Read(p[:limit])
	if errors.Is(err, io.EOF) && n == 0 {
		return 0, ErrUnexpectedEOF
	}
	return n, err
}

// ReadN reads exactly n bytes, blocking until all are available or an error
// occurs. Unlike io.Read, it never returns fewer bytes than requested unless
// an error accompanies the short read.
func (r *BufReader) ReadN(n int) ([]byte, error) {
	if n == 0 {
		return []byte{}, nil
	}
	if n > r.bufSize {
		return nil, fmt.Errorf("%w: requested %d, capacity %d", ErrBufferTooSmall, n, r.bufSize)
	}

	buf := make([]byte, n)
	_, err := io.ReadFull(r.rd, buf)
	if err != nil {
		if errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF) {
			return buf, ErrUnexpectedEOF
		}
		return nil, err
	}
	return buf, nil
}

// ReadLine reads bytes until "\r\n" and returns the line without the CRLF.
// It also handles bare "\n" for lenient clients.
func (r *BufReader) ReadLine() ([]byte, error) {
	line, err := r.rd.ReadSlice('\n')
	if err != nil {
		if errors.Is(err, io.EOF) {
			return nil, ErrUnexpectedEOF
		}
		// bufio.ErrBufferFull means the line is longer than the buffer.
		if errors.Is(err, bufio.ErrBufferFull) {
			return nil, ErrLineTooLong
		}
		return nil, err
	}

	if len(line) > r.maxLineLen {
		return nil, ErrLineTooLong
	}

	// Strip trailing CRLF or bare LF.
	line = trimCRLF(line)
	// Return a copy so the caller owns the slice (bufio may reuse its buffer).
	out := make([]byte, len(line))
	copy(out, line)
	return out, nil
}

// ReadByte reads and returns a single byte.
func (r *BufReader) ReadByte() (byte, error) {
	b, err := r.rd.ReadByte()
	if errors.Is(err, io.EOF) {
		return 0, ErrUnexpectedEOF
	}
	return b, err
}

// PeekByte returns the next byte without consuming it.
func (r *BufReader) PeekByte() (byte, error) {
	b, err := r.rd.Peek(1)
	if err != nil {
		if errors.Is(err, io.EOF) {
			return 0, ErrUnexpectedEOF
		}
		return 0, err
	}
	return b[0], nil
}

// Discard skips the next n bytes.
func (r *BufReader) Discard(n int) (int, error) {
	return r.rd.Discard(n)
}

// Buffered returns the number of bytes currently in the read buffer.
func (r *BufReader) Buffered() int {
	return r.rd.Buffered()
}

// Reset replaces the underlying reader, resetting the internal buffer state.
// Useful when reusing a BufReader across client connections.
func (r *BufReader) Reset(rd io.Reader) {
	r.rd.Reset(rd)
}

// ─── helpers ─────────────────────────────────────────────────────────────────

// trimCRLF removes a trailing "\r\n" or bare "\n".
func trimCRLF(b []byte) []byte {
	if len(b) >= 2 && b[len(b)-2] == '\r' && b[len(b)-1] == '\n' {
		return b[:len(b)-2]
	}
	if len(b) >= 1 && b[len(b)-1] == '\n' {
		return b[:len(b)-1]
	}
	return b
}
