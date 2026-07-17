package kps

import (
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/pion/webrtc/v4"
)

// writeBufferLowThreshold is the SCTP send-buffer level at which a blocked
// Write resumes. This is a LOCAL queue bound only — flow control is the §6.5
// credit reservation.
const writeBufferLowThreshold = 1 << 20 // 1 MiB

var (
	errWriteClosed  = errors.New("kps: write half closed")
	errStreamClosed = errors.New("kps: stream closed")
)

// StreamError is the error surfaced to the read side when the peer aborts its
// write half (RESET), or to the write side when the peer cancels its read
// (STOP_SENDING). Callers can inspect Code (SPEC §9.1).
type StreamError struct {
	Code ErrorCode
	// Remote is true when the code originated from the peer.
	Remote bool
}

func (e *StreamError) Error() string {
	return fmt.Sprintf("kps: stream reset (code %d)", e.Code)
}

// streamHooks connect a stream to its owning connection.
type streamHooks struct {
	// fatal reports a wire violation by the peer: the whole connection fails.
	fatal func(code ErrorCode, msg string)
	// retired fires once when the stream fully retires (wire-complete +
	// channel closed + drained).
	retired func()
	// isTeardown reports whether the connection is closing/failed (suppresses
	// unexpected-close policing).
	isTeardown func() bool
}

type terminalKind byte

const (
	terminalNone terminalKind = iota
	terminalFin
	terminalReset
)

// webrtcStream is the WebRTC implementation of Stream (SPEC §6.2 framing +
// §6.5 flow control): a byte stream over one SCTP data channel. The
// data-channel label is a non-semantic implementation detail.
type webrtcStream struct {
	dc    *webrtc.DataChannel
	sf    *streamFlow
	hooks streamHooks

	mu    sync.Mutex
	rcond *sync.Cond // read-side state changes
	wcond *sync.Cond // local send-buffer drainage

	inbuf         []byte
	peerFin       bool         // peer FIN observed
	peerReset     *StreamError // peer RESET observed
	peerStop      *StreamError // peer STOP_SENDING observed
	localTerminal terminalKind // FIN/RESET handed to the transport
	readCancel    bool         // local CancelRead
	dcClosed      bool
	retiredFired  bool

	openCh   chan struct{}
	openOnce sync.Once

	closedCh  chan struct{}
	closeOnce sync.Once
}

func newStream(dc *webrtc.DataChannel, fc *connFlow, hooks streamHooks) *webrtcStream {
	s := &webrtcStream{dc: dc, hooks: hooks, openCh: make(chan struct{}), closedCh: make(chan struct{})}
	s.rcond = sync.NewCond(&s.mu)
	s.wcond = sync.NewCond(&s.mu)
	// This stream's credit advertisements travel on this stream's channel.
	s.sf = fc.newStream(func(v uint64) {
		if dc.ReadyState() == webrtc.DataChannelStateOpen {
			_ = dc.Send(encodeMaxStreamData(v))
		}
	})

	dc.SetBufferedAmountLowThreshold(writeBufferLowThreshold)
	dc.OnBufferedAmountLow(func() {
		s.mu.Lock()
		s.wcond.Broadcast()
		s.mu.Unlock()
	})
	if dc.ReadyState() == webrtc.DataChannelStateOpen {
		s.openOnce.Do(func() { close(s.openCh) })
	}
	dc.OnOpen(func() { s.openOnce.Do(func() { close(s.openCh) }) })
	dc.OnMessage(func(msg webrtc.DataChannelMessage) { s.onFrame(msg.Data) })
	dc.OnClose(func() { s.onChannelClose() })
	return s
}

func (s *webrtcStream) onFrame(data []byte) {
	f, err := parseFrame(data)
	if err != nil {
		s.hooks.fatal(CodeProtocolError, err.Error())
		return
	}
	switch f.typ {
	case frameData:
		s.mu.Lock()
		if s.peerFin || s.peerReset != nil {
			s.mu.Unlock()
			s.hooks.fatal(CodeProtocolError, "DATA after terminal frame")
			return
		}
		cancelled := s.readCancel
		s.mu.Unlock()
		if err := s.sf.onDataReceived(uint64(len(f.payload))); err != nil {
			s.hooks.fatal(CodeProtocolError, err.Error())
			return
		}
		if cancelled {
			// In-flight DATA racing our STOP_SENDING: discard = consumed.
			s.sf.onConsumed(uint64(len(f.payload)))
			return
		}
		s.mu.Lock()
		s.inbuf = append(s.inbuf, f.payload...)
		s.rcond.Broadcast()
		s.mu.Unlock()
	case frameFin:
		s.mu.Lock()
		if s.peerFin || s.peerReset != nil {
			s.mu.Unlock()
			s.hooks.fatal(CodeProtocolError, "second terminal frame")
			return
		}
		s.peerFin = true
		s.rcond.Broadcast()
		s.mu.Unlock()
		s.maybeRetire()
	case frameReset:
		s.mu.Lock()
		if s.peerFin || s.peerReset != nil {
			s.mu.Unlock()
			s.hooks.fatal(CodeProtocolError, "second terminal frame")
			return
		}
		s.peerReset = &StreamError{Code: f.code, Remote: true}
		// QUIC-like reset: discard buffered-but-unread bytes (counts as
		// consumed, releasing connection credit) and surface the error.
		discarded := uint64(len(s.inbuf))
		s.inbuf = nil
		s.rcond.Broadcast()
		s.mu.Unlock()
		if discarded > 0 {
			s.sf.onConsumed(discarded)
		}
		s.maybeRetire()
	case frameStopSending:
		s.mu.Lock()
		if s.peerStop != nil {
			s.mu.Unlock()
			return // duplicate: ignore
		}
		s.peerStop = &StreamError{Code: f.code, Remote: true}
		autoReset := s.localTerminal == terminalNone
		if autoReset {
			s.localTerminal = terminalReset
		}
		s.wcond.Broadcast()
		s.mu.Unlock()
		s.sf.failSend(&StreamError{Code: f.code, Remote: true})
		if autoReset {
			// No terminal handed to the transport yet: reply with RESET (§6.2).
			if s.dc.ReadyState() == webrtc.DataChannelStateOpen {
				_ = s.dc.Send(encodeCode(frameReset, f.code))
			}
			s.maybeRetire()
		}
	case frameMaxStreamData:
		s.sf.onPeerMaxStreamData(f.credit)
	}
}

func (s *webrtcStream) onChannelClose() {
	s.mu.Lock()
	s.dcClosed = true
	wireComplete := s.localTerminal != terminalNone && (s.peerFin || s.peerReset != nil)
	s.rcond.Broadcast()
	s.wcond.Broadcast()
	s.mu.Unlock()
	if !wireComplete && !s.hooks.isTeardown() {
		// §6.5 teardown accounting: a channel disappearing mid-stream leaves
		// connection credit ambiguous — connection-fatal.
		s.hooks.fatal(CodeProtocolError, "data channel closed mid-stream")
	}
	s.sf.failSend(errStreamClosed)
	s.openOnce.Do(func() { close(s.openCh) })
	s.closeOnce.Do(func() { close(s.closedCh) })
	s.maybeRetire()
}

// maybeRetire drives the §6.5 retirement ladder: once wire-complete and
// locally drained we MUST initiate the channel close; once the channel has
// also closed, the stream retires (returning MAX_STREAMS credit for
// peer-initiated streams via hooks.retired).
func (s *webrtcStream) maybeRetire() {
	s.mu.Lock()
	wireComplete := s.localTerminal != terminalNone && (s.peerFin || s.peerReset != nil)
	drained := len(s.inbuf) == 0
	dcClosed := s.dcClosed
	fire := false
	if wireComplete && drained && dcClosed && !s.retiredFired {
		s.retiredFired = true
		fire = true
	}
	s.mu.Unlock()
	if !wireComplete || !drained {
		return
	}
	if !dcClosed {
		// Let the terminal frame flush out of the SCTP send buffer before
		// resetting the stream (bounded so close stays prompt; runs off the
		// caller's goroutine — maybeRetire is called from read/frame paths).
		go func() {
			deadline := time.Now().Add(250 * time.Millisecond)
			for s.dc.BufferedAmount() > 0 && time.Now().Before(deadline) {
				time.Sleep(5 * time.Millisecond)
			}
			_ = s.dc.Close()
		}()
		return
	}
	if fire {
		s.hooks.retired()
	}
}

// WaitOpen blocks until the data channel is open or the stream is closed.
func (s *webrtcStream) WaitOpen() error {
	<-s.openCh
	if s.dc.ReadyState() != webrtc.DataChannelStateOpen {
		return errStreamClosed
	}
	return nil
}

// Read fills p with inbound bytes, blocking until some are available. It
// returns io.EOF after the peer's CloseWrite and all bytes are consumed, or a
// *StreamError if the peer reset its write half. Bytes returned here are
// consumption (§6.5): they advance the receive counters and eventually
// re-advertise credit to the peer.
func (s *webrtcStream) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	s.mu.Lock()
	for {
		if len(s.inbuf) > 0 {
			n := copy(p, s.inbuf)
			s.inbuf = s.inbuf[n:]
			s.mu.Unlock()
			s.sf.onConsumed(uint64(n))
			s.maybeRetire()
			return n, nil
		}
		if s.readCancel {
			s.mu.Unlock()
			return 0, errStreamClosed
		}
		if s.peerReset != nil {
			err := s.peerReset
			s.mu.Unlock()
			return 0, err
		}
		if s.peerFin {
			s.mu.Unlock()
			return 0, io.EOF
		}
		if s.dcClosed {
			s.mu.Unlock()
			return 0, errStreamClosed
		}
		s.rcond.Wait()
	}
}

// Write sends p as stream bytes. Frames split at MAX_FRAME_PAYLOAD and at the
// peer's credit boundary; a write blocks while the peer's advertised windows
// (§6.5) are exhausted — i.e. until the remote application actually reads. It
// returns a *StreamError if the peer has cancelled its read half
// (STOP_SENDING).
func (s *webrtcStream) Write(p []byte) (int, error) {
	if err := s.WaitOpen(); err != nil {
		return 0, err
	}
	written := 0
	for len(p) > 0 {
		want := uint64(min(len(p), maxFramePayload))
		granted, err := s.sf.reserve(want) // credit BEFORE the frame may exist
		if err != nil {
			return written, err
		}
		chunk := p[:granted]
		if err := s.sendReserved(encodeData(chunk), granted); err != nil {
			return written, err
		}
		written += int(granted)
		p = p[granted:]
	}
	return written, nil
}

// sendReserved applies the local send-buffer bound, then hands one frame with
// n reserved payload bytes to the transport (committing or releasing the
// reservation).
func (s *webrtcStream) sendReserved(frame []byte, n uint64) error {
	s.mu.Lock()
	for {
		if s.peerStop != nil {
			err := s.peerStop
			s.mu.Unlock()
			s.sf.release(n)
			return err
		}
		if s.localTerminal != terminalNone {
			s.mu.Unlock()
			s.sf.release(n)
			return errWriteClosed
		}
		if s.dcClosed {
			s.mu.Unlock()
			s.sf.release(n)
			return errStreamClosed
		}
		if s.dc.BufferedAmount() < writeBufferLowThreshold {
			break
		}
		// OnBufferedAmountLow is edge-triggered; guard the wait with a timed
		// re-check so a missed/never-fired crossing can't wedge the writer
		// (mirrors the Rust writer task's 100ms timeout on buf_low waits).
		wake := time.AfterFunc(100*time.Millisecond, func() {
			s.mu.Lock()
			s.wcond.Broadcast()
			s.mu.Unlock()
		})
		s.wcond.Wait()
		wake.Stop()
	}
	s.mu.Unlock()
	if err := s.dc.Send(frame); err != nil {
		s.sf.release(n)
		return err
	}
	s.sf.commit(n)
	return nil
}

// CloseWrite gracefully finishes the local write half; the peer observes EOF
// after all previously written bytes (SPEC §6.1).
func (s *webrtcStream) CloseWrite() error {
	s.mu.Lock()
	if s.localTerminal != terminalNone {
		s.mu.Unlock()
		return nil
	}
	s.localTerminal = terminalFin
	s.wcond.Broadcast()
	s.mu.Unlock()
	s.sf.failSend(errWriteClosed)
	if err := s.WaitOpen(); err != nil {
		return err
	}
	err := s.dc.Send(encodeFin())
	s.maybeRetire()
	return err
}

// CancelRead tells the peer we no longer want inbound bytes (STOP_SENDING). It
// is cancellation, not graceful EOF. Buffered bytes are discarded (which still
// releases connection-level credit); no further stream credit is granted.
func (s *webrtcStream) CancelRead(code ErrorCode) error {
	s.mu.Lock()
	if s.readCancel {
		s.mu.Unlock()
		return nil
	}
	s.readCancel = true
	discarded := uint64(len(s.inbuf))
	s.inbuf = nil
	peerTerminal := s.peerFin || s.peerReset != nil
	s.rcond.Broadcast()
	s.mu.Unlock()
	s.sf.markCancelled()
	if discarded > 0 {
		s.sf.onConsumed(discarded)
	}
	defer s.maybeRetire()
	if peerTerminal {
		return nil // the peer's write half already ended; STOP_SENDING is moot
	}
	if err := s.WaitOpen(); err != nil {
		return err
	}
	return s.dc.Send(encodeCode(frameStopSending, code))
}

// ResetWrite aborts the local write half; the peer observes a stream error
// rather than EOF.
func (s *webrtcStream) ResetWrite(code ErrorCode) error {
	s.mu.Lock()
	if s.localTerminal != terminalNone {
		s.mu.Unlock()
		return nil
	}
	s.localTerminal = terminalReset
	s.wcond.Broadcast()
	s.mu.Unlock()
	s.sf.failSend(&StreamError{Code: code})
	if err := s.WaitOpen(); err != nil {
		return err
	}
	err := s.dc.Send(encodeCode(frameReset, code))
	s.maybeRetire()
	return err
}

// Close tears down both halves: it finishes the write half (if still open) and
// cancels the read half. The channel itself closes at retirement — once the
// peer's terminal frame (a conforming peer answers STOP_SENDING with RESET)
// has arrived — because closing it earlier is a §6.5 protocol violation.
func (s *webrtcStream) Close() error {
	_ = s.CloseWrite()
	_ = s.CancelRead(CodeClosed)
	return nil
}

// CloseWithError tears down both halves conveying a code: the peer observes a
// stream error (RESET) rather than EOF, and is told to stop sending.
func (s *webrtcStream) CloseWithError(code ErrorCode) error {
	_ = s.ResetWrite(code)
	_ = s.CancelRead(code)
	return nil
}

// destroy is connection teardown: discard state and fail waiters, no wire
// activity, no close policing.
func (s *webrtcStream) destroy() {
	s.mu.Lock()
	discarded := uint64(len(s.inbuf))
	s.inbuf = nil
	s.readCancel = true
	s.rcond.Broadcast()
	s.wcond.Broadcast()
	s.mu.Unlock()
	if discarded > 0 {
		s.sf.onConsumed(discarded) // buffered-but-never-delivered counts consumed
	}
	s.sf.failSend(errConnClosed)
	s.openOnce.Do(func() { close(s.openCh) })
	s.closeOnce.Do(func() { close(s.closedCh) })
}

func (s *webrtcStream) Closed() <-chan struct{} { return s.closedCh }

// Err reports the stream's close reason (best-effort): a peer RESET surfaces as
// a *StreamError; a clean close or local teardown is nil.
func (s *webrtcStream) Err() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.peerReset != nil {
		return s.peerReset
	}
	return nil
}
