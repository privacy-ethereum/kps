package kps

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pion/webrtc/v4"
)

var errConnClosed = errors.New("kps: connection closed")

// webrtcConn is the WebRTC implementation of Conn (SPEC §4). It carries any
// number of independent byte Streams over one PeerConnection.
type webrtcConn struct {
	pc *webrtc.PeerConnection

	streamCh  chan *webrtcStream
	streamSeq uint64

	dgChan  *webrtc.DataChannel
	dgInbox chan []byte

	control *webrtc.DataChannel // reserved reliable channel (ID 2): CONNECTION_CLOSE

	closeOnce sync.Once
	closedCh  chan struct{}
	closeErr  error
}

// newConn wraps a connected PeerConnection. `control` is the reserved reliable
// channel (ID 0): the client passes the one it created pre-offer (to force the
// SCTP m-line); the server passes nil and newConn creates its side. It carries
// CONNECTION_CLOSE (SPEC §8).
func newConn(pc *webrtc.PeerConnection, control *webrtc.DataChannel) *webrtcConn {
	c := &webrtcConn{
		pc:       pc,
		streamCh: make(chan *webrtcStream, 16),
		dgInbox:  make(chan []byte, 256),
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
	c.openDatagramChannel()
	c.setupControlChannel(control)
	return c
}

// openDatagramChannel reserves the unreliable, unordered datagram channel
// (SPEC §7/§8): negotiated on both sides at fixed ID 1, so it carries datagrams
// without DCEP and never surfaces as an application stream.
func (c *webrtcConn) openDatagramChannel() {
	negotiated := true
	var id uint16 = 1
	var maxRetransmits uint16 = 0
	ordered := false
	dc, err := c.pc.CreateDataChannel("_kps_datagrams", &webrtc.DataChannelInit{
		Negotiated:     &negotiated,
		ID:             &id,
		Ordered:        &ordered,
		MaxRetransmits: &maxRetransmits,
	})
	if err != nil {
		return
	}
	c.dgChan = dc
	dc.OnMessage(func(msg webrtc.DataChannelMessage) {
		data := append([]byte(nil), msg.Data...)
		select {
		case c.dgInbox <- data:
		default:
			// bounded buffer: drop when full (datagrams are best-effort)
		}
	})
}

// setupControlChannel wires the reserved reliable channel (SPEC §8, negotiated,
// fixed ID 0). The client already created it before the offer (to force the SCTP
// m-line) and passes it in; the server passes nil and we create our side here.
// Its only message is CONNECTION_CLOSE (a big-endian uint32 code) — the WebRTC
// analogue of QUIC CONNECTION_CLOSE.
func (c *webrtcConn) setupControlChannel(control *webrtc.DataChannel) {
	if control == nil {
		negotiated := true
		var id uint16 = 0
		dc, err := c.pc.CreateDataChannel("_kps_control", &webrtc.DataChannelInit{
			Negotiated: &negotiated,
			ID:         &id,
		})
		if err != nil {
			return
		}
		control = dc
	}
	c.control = control
	control.OnMessage(func(msg webrtc.DataChannelMessage) {
		code := decodeCode(msg.Data)
		if code != CodeNone {
			c.markClosed(&StreamError{Code: code, Remote: true})
		} else {
			c.markClosed(nil)
		}
		_ = c.pc.Close()
	})
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

// CloseWithError closes the connection, conveying an application error code to
// the peer as a best-effort CONNECTION_CLOSE on the control channel (SPEC §8)
// before teardown — the WebRTC analogue of QUIC CONNECTION_CLOSE. Delivery isn't
// guaranteed (teardown may race), matching QUIC's single-packet close.
func (c *webrtcConn) CloseWithError(code ErrorCode) error {
	if c.control != nil {
		// The control channel opens asynchronously after the PC connects; a close
		// right after dial can beat it. Wait briefly for it to open, send, then
		// let the reliable message flush before tearing down SCTP (pc.Close()
		// aborts in-flight data). All bounded so close stays prompt.
		openBy := time.Now().Add(250 * time.Millisecond)
		for c.control.ReadyState() == webrtc.DataChannelStateConnecting && time.Now().Before(openBy) {
			time.Sleep(5 * time.Millisecond)
		}
		if c.control.ReadyState() == webrtc.DataChannelStateOpen {
			_ = c.control.Send(encodeConnClose(code))
			flushBy := time.Now().Add(250 * time.Millisecond)
			for c.control.BufferedAmount() > 0 && time.Now().Before(flushBy) {
				time.Sleep(5 * time.Millisecond)
			}
		}
	}
	if code != CodeNone {
		c.markClosed(&StreamError{Code: code})
	} else {
		c.markClosed(nil)
	}
	return c.pc.Close()
}

func (c *webrtcConn) Closed() <-chan struct{} { return c.closedCh }

func (c *webrtcConn) Err() error {
	select {
	case <-c.closedCh:
		return c.closeErr
	default:
		return nil // still open
	}
}

func (c *webrtcConn) markClosed(err error) {
	c.closeOnce.Do(func() {
		c.closeErr = err
		close(c.closedCh)
	})
}

// webrtcMaxDatagram caps the WebRTC datagram payload to a sub-MTU size so a
// datagram travels as a single unreliable SCTP message (fragmenting an
// unreliable message multiplies its loss). Oversized sends report this limit.
const webrtcMaxDatagram = 1200

func (c *webrtcConn) SendDatagram(p []byte) error {
	if len(p) > webrtcMaxDatagram {
		return &DatagramTooLargeError{MaxDatagramPayloadSize: webrtcMaxDatagram}
	}
	if c.dgChan == nil || c.dgChan.ReadyState() != webrtc.DataChannelStateOpen {
		return errors.New("kps: datagram channel not open")
	}
	return c.dgChan.Send(p)
}

func (c *webrtcConn) ReceiveDatagram(ctx context.Context) ([]byte, error) {
	select {
	case d := <-c.dgInbox:
		return d, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-c.closedCh:
		return nil, errConnClosed
	}
}
