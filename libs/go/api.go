package kps

import (
	"context"
	"fmt"
	"io"
)

// DatagramTooLargeError is returned by SendDatagram when the payload exceeds the
// connection's current datagram size limit. The limit is transport- and
// path-dependent (so KPS does not expose it as a fixed property); this error
// reports it, mirroring QUIC. As a rule of thumb, payloads up to ~1100 bytes are
// safe on every connection; larger payloads may or may not fit.
type DatagramTooLargeError struct {
	MaxDatagramPayloadSize int
}

func (e *DatagramTooLargeError) Error() string {
	return fmt.Sprintf("kps: datagram exceeds limit (max %d bytes)", e.MaxDatagramPayloadSize)
}

// Conn is an authenticated, secure, multiplexed kps session (SPEC §4),
// carrying any number of independent byte Streams. It is implemented by both
// transports (WebRTC and QUIC); callers cannot tell which backs a connection.
type Conn interface {
	// OpenStream opens a new bidirectional byte stream, blocking until ready.
	OpenStream(ctx context.Context) (Stream, error)
	// AcceptStream returns the next stream opened by the peer.
	AcceptStream(ctx context.Context) (Stream, error)
	// Close tears down the connection and invalidates all its streams.
	Close() error
	// Closed is closed when the connection ends.
	Closed() <-chan struct{}

	// Datagrams are unreliable, unordered, size-limited messages available on
	// every connection (SPEC §7). There is a per-connection size limit; an
	// oversized SendDatagram returns a *DatagramTooLargeError reporting it.
	// Delivery is best-effort: a sent datagram may never arrive.
	SendDatagram(p []byte) error
	ReceiveDatagram(ctx context.Context) ([]byte, error)
}

// Stream is an unnamed, bidirectional, reliable, ordered byte stream (SPEC §6)
// with no message boundaries. It is an io.Reader and io.Writer with QUIC-like
// lifecycle controls.
type Stream interface {
	io.Reader
	io.Writer
	// CloseWrite gracefully finishes the local write half; the peer observes
	// EOF after all previously written bytes.
	CloseWrite() error
	// CancelRead stops inbound bytes (cancellation, not EOF); where supported
	// the peer is told to stop sending.
	CancelRead(code ErrorCode) error
	// ResetWrite aborts the local write half; the peer observes a stream error.
	ResetWrite(code ErrorCode) error
	// Close tears down both halves of the stream.
	Close() error
}
