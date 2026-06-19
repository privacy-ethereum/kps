package kps

import (
	"context"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"
)

// startEchoListener runs a kps listener whose connections echo every stream's
// bytes back until the peer finishes its write half.
func startEchoListener(t *testing.T, ctx context.Context) *Listener {
	t.Helper()
	id, err := GenerateIdentity()
	if err != nil {
		t.Fatal(err)
	}
	ln, err := Listen(ctx, "127.0.0.1:0", Options{Identity: id})
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		for {
			conn, err := ln.Accept(ctx)
			if err != nil {
				return
			}
			go func() {
				for {
					s, err := conn.AcceptStream(ctx)
					if err != nil {
						return
					}
					go func() { _, _ = io.Copy(s, s); _ = s.CloseWrite() }()
				}
			}()
		}
	}()
	return ln
}

// TestQUICEcho proves a native QUIC client dials the listener on its shared UDP
// port and round-trips bytes over an unnamed stream (SPEC §5.3, §6.3).
func TestQUICEcho(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	ln := startEchoListener(t, ctx)
	defer ln.Close()

	conn, err := Dial(ctx, ln.Address("127.0.0.1"))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	s, err := conn.OpenStream(ctx)
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	msg := []byte("hello-quic-byte-stream")
	if _, err := s.Write(msg); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := s.CloseWrite(); err != nil {
		t.Fatalf("closeWrite: %v", err)
	}
	got, err := io.ReadAll(s)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if string(got) != string(msg) {
		t.Fatalf("echo mismatch: got %q want %q", got, msg)
	}
}

// TestQUICCerthashMismatchRejected proves the client pins by certhash: dialing
// the right endpoint with the wrong certhash fails (SPEC §3).
func TestQUICCerthashMismatchRejected(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	ln := startEchoListener(t, ctx)
	defer ln.Close()

	other, err := GenerateIdentity()
	if err != nil {
		t.Fatal(err)
	}
	parts := strings.SplitN(ln.Address("127.0.0.1"), ":", 3)
	bad := fmt.Sprintf("%s:%s:%s", parts[0], parts[1], other.Certhash)

	dialCtx, dialCancel := context.WithTimeout(ctx, 5*time.Second)
	defer dialCancel()
	if _, err := Dial(dialCtx, bad); err == nil {
		t.Fatal("expected dial to fail on certhash mismatch, but it succeeded")
	}
}
