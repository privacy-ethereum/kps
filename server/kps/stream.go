package kps

import (
	"errors"
	"io"
	"sync"

	"github.com/pion/webrtc/v4"
)

// Stream is a single message-oriented logical stream over a kps
// connection. Each Send produces one message on the peer.
type Stream struct {
	dc *webrtc.DataChannel

	openCh chan struct{}
	openOnce sync.Once

	mu      sync.Mutex
	queue   [][]byte
	waiters []chan []byte
	closed  bool
}

func newStream(dc *webrtc.DataChannel) *Stream {
	s := &Stream{
		dc:     dc,
		openCh: make(chan struct{}),
	}
	if dc.ReadyState() == webrtc.DataChannelStateOpen {
		s.openOnce.Do(func() { close(s.openCh) })
	}
	dc.OnOpen(func() {
		s.openOnce.Do(func() { close(s.openCh) })
	})
	dc.OnMessage(func(msg webrtc.DataChannelMessage) {
		buf := append([]byte(nil), msg.Data...)
		s.mu.Lock()
		if len(s.waiters) > 0 {
			w := s.waiters[0]
			s.waiters = s.waiters[1:]
			s.mu.Unlock()
			w <- buf
			return
		}
		s.queue = append(s.queue, buf)
		s.mu.Unlock()
	})
	dc.OnClose(func() {
		s.mu.Lock()
		s.closed = true
		waiters := s.waiters
		s.waiters = nil
		s.mu.Unlock()
		for _, w := range waiters {
			close(w)
		}
	})
	return s
}

// Name returns the stream's protocol name (the data channel label).
func (s *Stream) Name() string { return s.dc.Label() }

// WaitOpen blocks until the data channel is open or the stream is closed.
func (s *Stream) WaitOpen() error {
	<-s.openCh
	if s.dc.ReadyState() != webrtc.DataChannelStateOpen {
		return errors.New("kps: stream closed before open")
	}
	return nil
}

// Send writes one message. Each call delivers exactly one message on the peer.
func (s *Stream) Send(p []byte) error {
	if s.dc.ReadyState() != webrtc.DataChannelStateOpen {
		return errors.New("kps: stream not open")
	}
	return s.dc.Send(p)
}

// SendString writes one string message.
func (s *Stream) SendString(p string) error {
	if s.dc.ReadyState() != webrtc.DataChannelStateOpen {
		return errors.New("kps: stream not open")
	}
	return s.dc.SendText(p)
}

// Recv blocks until the next inbound message. Returns io.EOF when the
// stream closes with no more buffered messages.
func (s *Stream) Recv() ([]byte, error) {
	s.mu.Lock()
	if len(s.queue) > 0 {
		buf := s.queue[0]
		s.queue = s.queue[1:]
		s.mu.Unlock()
		return buf, nil
	}
	if s.closed {
		s.mu.Unlock()
		return nil, io.EOF
	}
	w := make(chan []byte, 1)
	s.waiters = append(s.waiters, w)
	s.mu.Unlock()

	buf, ok := <-w
	if !ok {
		return nil, io.EOF
	}
	return buf, nil
}

// Close closes the stream.
func (s *Stream) Close() error {
	return s.dc.Close()
}
