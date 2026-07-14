package kps

import (
	"context"
	"fmt"
	"io"
	"net"
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
	// CloseWithError tears down the connection, conveying an error code to the
	// peer where the transport supports it (QUIC CONNECTION_CLOSE; a no-op code
	// on WebRTC). Mirrors the JS client's close(reason).
	CloseWithError(code ErrorCode) error
	// Closed is closed when the connection ends.
	Closed() <-chan struct{}
	// Err reports why the connection closed: nil while open or after a clean
	// close, non-nil otherwise. Best-effort — a reason is only available where
	// the transport carries one (QUIC), so WebRTC failures surface generically.
	Err() error
	// RemoteAddr returns the peer's UDP endpoint (e.g. for per-IP policy such
	// as rate limiting). It reflects the endpoint observed at connection
	// establishment and MAY change over the connection's life (QUIC path
	// migration, ICE renomination); on the dial side it is the dialed endpoint.
	RemoteAddr() net.Addr

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
	// CloseWithError tears down both halves, conveying an error code to the peer
	// (reset write + stop-sending read). Mirrors the JS stream's close(reason).
	CloseWithError(code ErrorCode) error
	// Closed is closed when the stream ends (either half torn down or peer gone).
	Closed() <-chan struct{}
	// Err reports why the stream closed: nil while open or after a clean close,
	// non-nil otherwise (best-effort, transport-dependent).
	Err() error
}
