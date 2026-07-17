package kps

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pion/webrtc/v4"
)

var errConnClosed = errors.New("kps: connection closed")

// The pre-HELLO state must be bounded (SPEC §8). Counted from transport
// establishment on the accept side; dialers usually fail faster via ctx.
const helloTimeout = 15 * time.Second

// webrtcConn is the WebRTC implementation of Conn (SPEC §4). It carries any
// number of independent byte Streams over one PeerConnection, gated on the §8
// HELLO exchange, with §6.5 end-to-end flow control.
type webrtcConn struct {
	pc *webrtc.PeerConnection

	flow *connFlow

	streamSeq uint64

	dgChan  *webrtc.DataChannel
	dgInbox chan []byte

	control *webrtc.DataChannel // reserved reliable channel (ID 0): §8 typed messages

	// remote is the peer's UDP endpoint: the first STUN source on the accept
	// side, the dialed endpoint on the dial side.
	remote net.Addr

	mu            sync.Mutex
	helloSent     bool
	peerHello     bool
	staged        []*webrtcStream // peer streams observed before mutual HELLO
	incoming      []*webrtcStream // accepted-stream queue: bounded by MAX_STREAMS credit, never blocks the transport callbacks
	acceptWake    chan struct{}   // closed+replaced when incoming grows
	streams       map[*webrtcStream]struct{}
	establishedCh chan struct{}
	estOnce       sync.Once

	tearingDown atomic.Bool

	closeOnce sync.Once
	closedCh  chan struct{}
	closeErr  error
}

// newConn wraps a connected PeerConnection. `control` is the reserved reliable
// channel (ID 0): the client passes the one it created pre-offer (to force the
// SCTP m-line); the server passes nil and newConn creates its side. It carries
// the §8 typed control messages (HELLO, CONNECTION_CLOSE, credit). `remote` is
// the peer's UDP endpoint (RemoteAddr).
func newConn(pc *webrtc.PeerConnection, control *webrtc.DataChannel, remote net.Addr) *webrtcConn {
	c := &webrtcConn{
		pc:            pc,
		flow:          newConnFlow(defaultFlowLimits),
		dgInbox:       make(chan []byte, 256),
		remote:        remote,
		acceptWake:    make(chan struct{}),
		streams:       make(map[*webrtcStream]struct{}),
		establishedCh: make(chan struct{}),
		closedCh:      make(chan struct{}),
	}
	pc.OnDataChannel(func(dc *webrtc.DataChannel) { c.handleIncoming(dc) })
	c.openDatagramChannel()
	c.setupControlChannel(control)
	// Connection-level credit advertisements travel on the control channel.
	c.flow.sendMaxData = func(v uint64) { c.trySendControl(encodeMaxData(v)) }
	c.flow.sendMaxStreams = func(v uint64) { c.trySendControl(encodeMaxStreams(v)) }
	return c
}

func (c *webrtcConn) hooksFor() streamHooks {
	return streamHooks{
		fatal:      c.fatal,
		isTeardown: func() bool { return c.tearingDown.Load() },
	}
}

// handleIncoming wires a peer-opened (DCEP) channel: it consumes a stream slot
// immediately (even unaccepted or pre-HELLO), and is staged until mutual HELLO
// (SPEC §8 cross-channel ordering).
func (c *webrtcConn) handleIncoming(dc *webrtc.DataChannel) {
	if err := c.flow.peerStreamOpened(); err != nil {
		c.fatal(CodeProtocolError, err.Error())
		return
	}
	hooks := c.hooksFor()
	// Peer-initiated streams return MAX_STREAMS credit when they retire.
	var s *webrtcStream
	hooks.retired = func() {
		c.dropStream(s)
		c.flow.peerStreamRetired()
	}
	s = newStream(dc, c.flow, hooks)
	c.mu.Lock()
	c.streams[s] = struct{}{}
	established := c.isEstablishedLocked()
	if !established {
		c.staged = append(c.staged, s)
		c.mu.Unlock()
		return
	}
	c.mu.Unlock()
	c.surfaceStream(s)
}

// surfaceStream enqueues an established peer stream for AcceptStream. It MUST
// NOT block: it runs on the transport's callback goroutine, and stalling it
// stalls every later data-channel event. The queue is bounded by MAX_STREAMS
// credit (peerStreamOpened), not by queue capacity.
func (c *webrtcConn) surfaceStream(s *webrtcStream) {
	select {
	case <-c.closedCh:
		s.destroy()
		return
	default:
	}
	c.mu.Lock()
	c.incoming = append(c.incoming, s)
	close(c.acceptWake)
	c.acceptWake = make(chan struct{})
	c.mu.Unlock()
}

func (c *webrtcConn) dropStream(s *webrtcStream) {
	c.mu.Lock()
	delete(c.streams, s)
	c.mu.Unlock()
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
	// Loss of a reserved channel while the connection is healthy is fatal (§8).
	dc.OnClose(func() { c.reservedChannelLost("datagram") })
}

// setupControlChannel wires the reserved reliable channel (SPEC §8, negotiated,
// fixed ID 0). The client already created it before the offer (to force the
// SCTP m-line) and passes it in; the server passes nil and we create our side
// here. It carries the typed §8 messages; HELLO is sent the moment it opens.
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
	control.OnOpen(func() { c.sendHello() })
	if control.ReadyState() == webrtc.DataChannelStateOpen {
		c.sendHello()
	}
	control.OnMessage(func(msg webrtc.DataChannelMessage) { c.onControl(msg.Data) })
	control.OnClose(func() { c.reservedChannelLost("control") })
}

func (c *webrtcConn) sendHello() {
	c.mu.Lock()
	if c.helloSent {
		c.mu.Unlock()
		return
	}
	c.helloSent = true
	c.mu.Unlock()
	c.trySendControl(encodeHello(c.flow.local))
	c.checkEstablished()
}

func (c *webrtcConn) onControl(data []byte) {
	m, err := decodeControl(data)
	if err != nil {
		c.fatal(CodeProtocolError, err.Error())
		return
	}
	switch m.typ {
	case ctrlHello:
		c.mu.Lock()
		dup := c.peerHello
		c.mu.Unlock()
		if dup {
			c.fatal(CodeProtocolError, "duplicate HELLO")
			return
		}
		if m.version != wireVersion {
			c.trySendControl(encodeConnClose(CodeUnsupported))
			c.teardown(&StreamError{Code: CodeUnsupported, Remote: false},
				fmt.Sprintf("peer wire version %d (want %d)", m.version, wireVersion))
			return
		}
		c.flow.onPeerHello(m.limits)
		c.mu.Lock()
		c.peerHello = true
		c.mu.Unlock()
		c.checkEstablished()
	case ctrlConnClose:
		// Valid at any time — before HELLO it is a handshake rejection (§8).
		if m.code != CodeNone {
			c.teardown(&StreamError{Code: m.code, Remote: true}, "")
		} else {
			c.teardown(nil, "")
		}
	case ctrlMaxData:
		if !c.isPeerHelloDone() {
			c.fatal(CodeProtocolError, "control message before HELLO")
			return
		}
		c.flow.onPeerMaxData(m.value)
	case ctrlMaxStreams:
		if !c.isPeerHelloDone() {
			c.fatal(CodeProtocolError, "control message before HELLO")
			return
		}
		c.flow.onPeerMaxStreams(m.value)
	}
}

func (c *webrtcConn) isPeerHelloDone() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.peerHello
}

func (c *webrtcConn) isEstablishedLocked() bool {
	select {
	case <-c.establishedCh:
		return true
	default:
		return false
	}
}

func (c *webrtcConn) checkEstablished() {
	c.mu.Lock()
	ready := c.helloSent && c.peerHello
	var staged []*webrtcStream
	if ready {
		staged = c.staged
		c.staged = nil
	}
	c.mu.Unlock()
	if !ready {
		return
	}
	c.estOnce.Do(func() { close(c.establishedCh) })
	for _, s := range staged {
		c.surfaceStream(s)
	}
}

// waitEstablished blocks until the mutual HELLO exchange completes (SPEC §8:
// dial/accept MUST NOT complete before it), bounded by ctx and helloTimeout.
func (c *webrtcConn) waitEstablished(ctx context.Context) error {
	timer := time.NewTimer(helloTimeout)
	defer timer.Stop()
	select {
	case <-c.establishedCh:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		c.mu.Lock()
		sent, received := c.helloSent, c.peerHello
		c.mu.Unlock()
		controlState := "nil"
		if c.control != nil {
			controlState = c.control.ReadyState().String()
		}
		c.fatal(CodeTimeout, "HELLO timeout")
		return fmt.Errorf("kps: HELLO timeout (helloSent=%v peerHello=%v control=%s pc=%s)",
			sent, received, controlState, c.pc.ConnectionState())
	case <-c.closedCh:
		if c.closeErr != nil {
			return c.closeErr
		}
		return errConnClosed
	}
}

func (c *webrtcConn) trySendControl(msg []byte) {
	if c.control != nil && c.control.ReadyState() == webrtc.DataChannelStateOpen {
		_ = c.control.Send(msg)
	}
}

func (c *webrtcConn) reservedChannelLost(which string) {
	if c.tearingDown.Load() {
		return
	}
	c.fatal(CodeProtocolError, "reserved "+which+" channel lost")
}

// fatal handles a peer wire violation (or local fatal condition): convey the
// code best-effort, then tear the connection down.
func (c *webrtcConn) fatal(code ErrorCode, msg string) {
	if c.tearingDown.Swap(true) {
		return
	}
	c.trySendControl(encodeConnClose(code))
	c.teardown(&StreamError{Code: code}, msg)
}

// teardown finalizes the connection: fail credit waiters, destroy streams,
// close the transport.
func (c *webrtcConn) teardown(err *StreamError, _ string) {
	c.tearingDown.Store(true)
	c.closeOnce.Do(func() {
		if err != nil {
			c.closeErr = err
		}
		close(c.closedCh)
	})
	if err != nil {
		c.flow.fail(err)
	} else {
		c.flow.fail(errConnClosed)
	}
	c.mu.Lock()
	streams := make([]*webrtcStream, 0, len(c.streams))
	for s := range c.streams {
		streams = append(streams, s)
	}
	c.streams = make(map[*webrtcStream]struct{})
	c.staged = nil
	c.mu.Unlock()
	for _, s := range streams {
		s.destroy()
	}
	_ = c.pc.Close()
}

func (c *webrtcConn) OpenStream(ctx context.Context) (Stream, error) {
	select {
	case <-c.closedCh:
		return nil, errConnClosed
	default:
	}
	// An endpoint MUST NOT open application streams before mutual HELLO (§8);
	// dial/accept already gate on it, so this only guards early callers.
	if err := c.waitEstablished(ctx); err != nil {
		return nil, err
	}
	// Stream-count credit: reserve a slot (waits at the peer's limit), commit
	// on successful channel creation.
	if err := c.flow.reserveStreamSlot(ctx); err != nil {
		return nil, err
	}
	label := fmt.Sprintf("kps-%d", atomic.AddUint64(&c.streamSeq, 1))
	dc, err := c.pc.CreateDataChannel(label, nil)
	if err != nil {
		c.flow.releaseStreamSlot()
		return nil, err
	}
	c.flow.commitStreamSlot()
	hooks := c.hooksFor()
	var s *webrtcStream
	hooks.retired = func() { c.dropStream(s) } // self-initiated: the peer grants credit
	s = newStream(dc, c.flow, hooks)
	c.mu.Lock()
	c.streams[s] = struct{}{}
	c.mu.Unlock()
	select {
	case <-s.openCh:
		if dc.ReadyState() != webrtc.DataChannelStateOpen {
			return nil, errStreamClosed
		}
		return s, nil
	case <-ctx.Done():
		// Abandon via the stream lifecycle (RESET + STOP_SENDING once open,
		// then retirement closes the channel) — closing a non-wire-complete
		// channel directly would be a §6.5 protocol violation against the peer.
		go func() {
			if s.WaitOpen() == nil {
				_ = s.CloseWithError(CodeCancelled)
			}
		}()
		return nil, ctx.Err()
	case <-c.closedCh:
		return nil, errConnClosed
	}
}

func (c *webrtcConn) AcceptStream(ctx context.Context) (Stream, error) {
	for {
		c.mu.Lock()
		if len(c.incoming) > 0 {
			s := c.incoming[0]
			c.incoming = c.incoming[1:]
			c.mu.Unlock()
			return s, nil
		}
		wake := c.acceptWake
		c.mu.Unlock()
		select {
		case <-wake:
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-c.closedCh:
			if c.closeErr != nil {
				return nil, c.closeErr
			}
			return nil, errConnClosed
		}
	}
}

func (c *webrtcConn) Close() error {
	c.tearingDown.Store(true)
	c.sendCloseAndFlush(CodeNone)
	c.teardown(nil, "")
	return nil
}

// CloseWithError closes the connection, conveying an application error code to
// the peer as a best-effort CONNECTION_CLOSE on the control channel (SPEC §8)
// before teardown — the WebRTC analogue of QUIC CONNECTION_CLOSE. Delivery
// isn't guaranteed (teardown may race), matching QUIC's single-packet close.
func (c *webrtcConn) CloseWithError(code ErrorCode) error {
	c.tearingDown.Store(true)
	c.sendCloseAndFlush(code)
	if code != CodeNone {
		c.teardown(&StreamError{Code: code}, "")
	} else {
		c.teardown(nil, "")
	}
	return nil
}

func (c *webrtcConn) sendCloseAndFlush(code ErrorCode) {
	if c.control == nil {
		return
	}
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

func (c *webrtcConn) Closed() <-chan struct{} { return c.closedCh }

func (c *webrtcConn) RemoteAddr() net.Addr { return c.remote }

func (c *webrtcConn) Err() error {
	select {
	case <-c.closedCh:
		return c.closeErr
	default:
		return nil // still open
	}
}

// markClosed preserves the pre-flow-control entry point used by transport
// state callbacks (dial/listener): the connection failed or closed at the
// transport layer.
func (c *webrtcConn) markClosed(err error) {
	c.tearingDown.Store(true)
	var se *StreamError
	if err != nil {
		if s, ok := err.(*StreamError); ok {
			se = s
		} else {
			se = &StreamError{Code: CodeNetworkError}
		}
	}
	c.teardown(se, "")
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
