package kps

import (
	"context"
	"io"
	"sync"
	"testing"
	"time"
)

// WebRTC-transport interop, driven by the Go WebRTC client (DialWebRTC). These
// exercise the §6.2 framing state machine (webrtcStream) end-to-end — the
// counterpart to the QUIC matrix in interop_test.go.

func TestWebRTC_Echo(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	ln := listenWith(t, ctx, echoHandler)
	defer ln.Close()

	conn, err := DialWebRTC(ctx, ln.Address("127.0.0.1"))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	s, err := conn.OpenStream(ctx)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	msg := []byte("hello-webrtc-byte-stream")
	if _, err := s.Write(msg); err != nil {
		t.Fatal(err)
	}
	_ = s.CloseWrite()
	got, err := io.ReadAll(s)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if string(got) != string(msg) {
		t.Fatalf("echo mismatch: got %q want %q", got, msg)
	}
}

func TestWebRTC_ResetWrite_PeerSeesError(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	readErr := make(chan error, 1)
	ln := listenWith(t, ctx, func(s Stream) {
		buf := make([]byte, 4096)
		var err error
		for err == nil {
			_, err = s.Read(buf)
		}
		readErr <- err
	})
	defer ln.Close()

	conn, err := DialWebRTC(ctx, ln.Address("127.0.0.1"))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	s, err := conn.OpenStream(ctx)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	_, _ = s.Write([]byte("partial"))
	time.Sleep(100 * time.Millisecond)
	if err := s.ResetWrite(CodeReset); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-readErr:
		if err == nil || err == io.EOF {
			t.Fatalf("peer read ended with %v, want a stream error (not EOF)", err)
		}
		if se, ok := err.(*StreamError); !ok {
			t.Fatalf("want *StreamError, got %T (%v)", err, err)
		} else if se.Code != CodeReset {
			t.Fatalf("StreamError code = %d, want %d", se.Code, CodeReset)
		}
	case <-time.After(8 * time.Second):
		t.Fatal("peer read never errored after resetWrite")
	}
}

func TestWebRTC_CancelRead_StopsPeer(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	writeErr := make(chan error, 1)
	ln := listenWith(t, ctx, func(s Stream) {
		buf := make([]byte, 16*1024)
		var err error
		for i := 0; i < 100000 && err == nil; i++ {
			_, err = s.Write(buf)
			time.Sleep(time.Millisecond)
		}
		writeErr <- err
	})
	defer ln.Close()

	conn, err := DialWebRTC(ctx, ln.Address("127.0.0.1"))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	s, err := conn.OpenStream(ctx)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if _, err := s.Write([]byte("hi")); err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)
	if err := s.CancelRead(CodeCancelled); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-writeErr:
		if err == nil {
			t.Fatal("peer kept writing after cancelRead; expected its writes to fail")
		}
	case <-time.After(8 * time.Second):
		t.Fatal("peer writes never failed after cancelRead")
	}
}

// TestBothTransportsSamePort: a single listener serves a WebRTC client and a
// QUIC client concurrently on the same UDP port (SPEC §10.3).
func TestBothTransportsSamePort(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	ln := listenWith(t, ctx, echoHandler)
	defer ln.Close()
	addr := ln.Address("127.0.0.1")

	roundTrip := func(dial func(context.Context, string) (Conn, error), msg string) error {
		conn, err := dial(ctx, addr)
		if err != nil {
			return err
		}
		defer conn.Close()
		s, err := conn.OpenStream(ctx)
		if err != nil {
			return err
		}
		if _, err := s.Write([]byte(msg)); err != nil {
			return err
		}
		_ = s.CloseWrite()
		got, err := io.ReadAll(s)
		if err != nil {
			return err
		}
		if string(got) != msg {
			t.Errorf("echo mismatch: got %q want %q", got, msg)
		}
		return nil
	}

	var wg sync.WaitGroup
	errs := make([]error, 2)
	wg.Add(2)
	go func() { defer wg.Done(); errs[0] = roundTrip(DialWebRTC, "via-webrtc") }()
	go func() { defer wg.Done(); errs[1] = roundTrip(Dial, "via-quic") }()
	wg.Wait()
	if errs[0] != nil {
		t.Fatalf("webrtc client: %v", errs[0])
	}
	if errs[1] != nil {
		t.Fatalf("quic client: %v", errs[1])
	}
}
