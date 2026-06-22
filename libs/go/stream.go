package kps

import (
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/pion/webrtc/v4"
)

// writeBufferLowThreshold is the SCTP send-buffer level at which a blocked
// Write resumes; Write applies backpressure above it.
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

// webrtcStream is the WebRTC implementation of Stream (SPEC §6.2): a byte
// stream over one SCTP data channel, framed with DATA/FIN/RESET/STOP_SENDING.
// The data-channel label is a non-semantic implementation detail.
type webrtcStream struct {
	dc *webrtc.DataChannel

	mu    sync.Mutex
	rcond *sync.Cond // read-side state changes
	wcond *sync.Cond // write-side backpressure relief

	inbuf       []byte
	readEOF     bool  // peer FIN observed
	readErr     error // peer RESET observed (*StreamError)
	readCancel  bool  // local CancelRead
	writeClosed bool  // local CloseWrite/ResetWrite/Close
	peerStop    error // peer STOP_SENDING observed (*StreamError)
	dcClosed    bool

	openCh   chan struct{}
	openOnce sync.Once
}

func newStream(dc *webrtc.DataChannel) *webrtcStream {
	s := &webrtcStream{dc: dc, openCh: make(chan struct{})}
	s.rcond = sync.NewCond(&s.mu)
	s.wcond = sync.NewCond(&s.mu)

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
	dc.OnClose(func() {
		s.mu.Lock()
		s.dcClosed = true
		if !s.readEOF && s.readErr == nil {
			s.readEOF = true // unexpected close reads as EOF
		}
		s.openOnce.Do(func() { close(s.openCh) })
		s.rcond.Broadcast()
		s.wcond.Broadcast()
		s.mu.Unlock()
	})
	return s
}

func (s *webrtcStream) onFrame(data []byte) {
	if len(data) == 0 {
		return
	}
	t := frameType(data[0])
	payload := data[1:]
	s.mu.Lock()
	defer s.mu.Unlock()
	switch t {
	case frameData:
		if s.readCancel || s.readEOF || s.readErr != nil {
			return // dropping inbound after cancel/EOF/reset
		}
		s.inbuf = append(s.inbuf, payload...)
		s.rcond.Broadcast()
	case frameFin:
		s.readEOF = true
		s.rcond.Broadcast()
	case frameReset:
		if s.readErr == nil {
			s.readErr = &StreamError{Code: decodeCode(payload), Remote: true}
		}
		s.rcond.Broadcast()
	case frameStopSending:
		if s.peerStop == nil {
			s.peerStop = &StreamError{Code: decodeCode(payload), Remote: true}
		}
		s.writeClosed = true
		s.wcond.Broadcast()
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
// *StreamError if the peer reset its write half.
func (s *webrtcStream) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for {
		if len(s.inbuf) > 0 {
			n := copy(p, s.inbuf)
			s.inbuf = s.inbuf[n:]
			return n, nil
		}
		if s.readCancel {
			return 0, errStreamClosed
		}
		if s.readErr != nil {
			return 0, s.readErr
		}
		if s.readEOF {
			return 0, io.EOF
		}
		s.rcond.Wait()
	}
}

// Write sends p as stream bytes, splitting into frames and applying backpressure
// from the SCTP send buffer. It returns a *StreamError if the peer has cancelled
// its read half (STOP_SENDING).
func (s *webrtcStream) Write(p []byte) (int, error) {
	if err := s.WaitOpen(); err != nil {
		return 0, err
	}
	written := 0
	for len(p) > 0 {
		chunk := p
		if len(chunk) > maxFramePayload {
			chunk = chunk[:maxFramePayload]
		}
		if err := s.writeFrame(encodeData(chunk)); err != nil {
			return written, err
		}
		written += len(chunk)
		p = p[len(chunk):]
	}
	return written, nil
}

// writeFrame blocks for backpressure, then sends one frame.
func (s *webrtcStream) writeFrame(frame []byte) error {
	s.mu.Lock()
	for {
		if s.peerStop != nil {
			err := s.peerStop
			s.mu.Unlock()
			return err
		}
		if s.writeClosed {
			s.mu.Unlock()
			return errWriteClosed
		}
		if s.dcClosed {
			s.mu.Unlock()
			return errStreamClosed
		}
		if s.dc.BufferedAmount() < writeBufferLowThreshold {
			break
		}
		s.wcond.Wait()
	}
	s.mu.Unlock()
	return s.dc.Send(frame)
}

// CloseWrite gracefully finishes the local write half; the peer observes EOF
// after all previously written bytes (SPEC §6.1).
func (s *webrtcStream) CloseWrite() error {
	s.mu.Lock()
	if s.writeClosed {
		s.mu.Unlock()
		return nil
	}
	s.writeClosed = true
	s.wcond.Broadcast()
	s.mu.Unlock()
	if err := s.WaitOpen(); err != nil {
		return err
	}
	return s.dc.Send(encodeFin())
}

// CancelRead tells the peer we no longer want inbound bytes (STOP_SENDING). It
// is cancellation, not graceful EOF.
func (s *webrtcStream) CancelRead(code ErrorCode) error {
	s.mu.Lock()
	if s.readCancel {
		s.mu.Unlock()
		return nil
	}
	s.readCancel = true
	s.inbuf = nil
	s.rcond.Broadcast()
	s.mu.Unlock()
	if err := s.WaitOpen(); err != nil {
		return err
	}
	return s.dc.Send(encodeCode(frameStopSending, code))
}

// ResetWrite aborts the local write half; the peer observes a stream error
// rather than EOF.
func (s *webrtcStream) ResetWrite(code ErrorCode) error {
	s.mu.Lock()
	if s.writeClosed {
		s.mu.Unlock()
		return nil
	}
	s.writeClosed = true
	s.wcond.Broadcast()
	s.mu.Unlock()
	if err := s.WaitOpen(); err != nil {
		return err
	}
	return s.dc.Send(encodeCode(frameReset, code))
}

// Close tears down both halves: it finishes the write half (if still open),
// cancels the read half, and closes the underlying channel.
func (s *webrtcStream) Close() error {
	_ = s.CloseWrite()
	_ = s.CancelRead(CodeClosed)
	return s.dc.Close()
}
