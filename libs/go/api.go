package kps

import (
	"context"
	"io"
)

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

	// SupportsDatagrams reports whether connection-level datagrams are
	// available (SPEC §7).
	SupportsDatagrams() bool
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
