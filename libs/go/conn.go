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

// webrtcConn is the WebRTC implementation of Conn (SPEC §4). It carries any
// number of independent byte Streams over one PeerConnection.
type webrtcConn struct {
	pc *webrtc.PeerConnection

	streamCh  chan *webrtcStream
	streamSeq uint64

	closeOnce sync.Once
	closedCh  chan struct{}
	closeErr  error
}

func newConn(pc *webrtc.PeerConnection) *webrtcConn {
	c := &webrtcConn{
		pc:       pc,
		streamCh: make(chan *webrtcStream, 16),
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

func (c *webrtcConn) OpenStream(ctx context.Context) (Stream, error) {
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

func (c *webrtcConn) AcceptStream(ctx context.Context) (Stream, error) {
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

func (c *webrtcConn) Close() error {
	c.markClosed(nil)
	return c.pc.Close()
}

func (c *webrtcConn) Closed() <-chan struct{} { return c.closedCh }

func (c *webrtcConn) markClosed(err error) {
	c.closeOnce.Do(func() {
		c.closeErr = err
		close(c.closedCh)
	})
}

func (c *webrtcConn) SupportsDatagrams() bool   { return false }
func (c *webrtcConn) SendDatagram([]byte) error { return ErrDatagramsUnsupported }
func (c *webrtcConn) ReceiveDatagram(context.Context) ([]byte, error) {
	return nil, ErrDatagramsUnsupported
}
