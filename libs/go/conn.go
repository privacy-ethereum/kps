package kps

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/pion/webrtc/v4"
)

// ErrDatagramsUnsupported is returned by the datagram API while the capability
// is not implemented (SPEC §7). The abstraction exists so datagrams can be
// added later without an API break.
var ErrDatagramsUnsupported = errors.New("kps: datagrams not supported")

var errConnClosed = errors.New("kps: connection closed")

// Conn is an authenticated, secure, multiplexed kps session (SPEC §4). It
// carries any number of independent byte Streams. For v0 this is the WebRTC
// transport; the QUIC transport (M3) will implement the same surface.
type Conn struct {
	pc *webrtc.PeerConnection

	streamCh  chan *Stream
	streamSeq uint64

	closeOnce sync.Once
	closedCh  chan struct{}
	closeErr  error
}

func newConn(pc *webrtc.PeerConnection) *Conn {
	c := &Conn{
		pc:       pc,
		streamCh: make(chan *Stream, 16),
		closedCh: make(chan struct{}),
	}
	pc.OnDataChannel(func(dc *webrtc.DataChannel) {
		s := newStream(dc)
		select {
		case c.streamCh <- s:
		case <-c.closedCh:
			_ = dc.Close()
		}
	})
	return c
}

// OpenStream opens a new bidirectional byte stream and blocks until it is ready
// or ctx is done. The data-channel label is generated and non-semantic.
func (c *Conn) OpenStream(ctx context.Context) (*Stream, error) {
	select {
	case <-c.closedCh:
		return nil, errConnClosed
	default:
	}
	label := fmt.Sprintf("kps-%d", atomic.AddUint64(&c.streamSeq, 1))
	dc, err := c.pc.CreateDataChannel(label, nil)
	if err != nil {
		return nil, err
	}
	s := newStream(dc)
	select {
	case <-s.openCh:
		if dc.ReadyState() != webrtc.DataChannelStateOpen {
			return nil, errStreamClosed
		}
		return s, nil
	case <-ctx.Done():
		_ = dc.Close()
		return nil, ctx.Err()
	case <-c.closedCh:
		_ = dc.Close()
		return nil, errConnClosed
	}
}

// AcceptStream returns the next stream opened by the peer, blocking until one
// arrives, ctx is done, or the connection closes.
func (c *Conn) AcceptStream(ctx context.Context) (*Stream, error) {
	select {
	case s := <-c.streamCh:
		return s, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-c.closedCh:
		if c.closeErr != nil {
			return nil, c.closeErr
		}
		return nil, errConnClosed
	}
}

// Close tears down the connection and invalidates all its streams.
func (c *Conn) Close() error {
	c.markClosed(nil)
	return c.pc.Close()
}

// Closed returns a channel closed when the connection ends.
func (c *Conn) Closed() <-chan struct{} { return c.closedCh }

func (c *Conn) markClosed(err error) {
	c.closeOnce.Do(func() {
		c.closeErr = err
		close(c.closedCh)
	})
}

// SupportsDatagrams reports whether connection-level datagrams are available
// (SPEC §7). v0 over WebRTC: not yet.
func (c *Conn) SupportsDatagrams() bool { return false }

// SendDatagram sends one unreliable, unordered datagram.
func (c *Conn) SendDatagram([]byte) error { return ErrDatagramsUnsupported }

// ReceiveDatagram receives one inbound datagram.
func (c *Conn) ReceiveDatagram(context.Context) ([]byte, error) {
	return nil, ErrDatagramsUnsupported
}
