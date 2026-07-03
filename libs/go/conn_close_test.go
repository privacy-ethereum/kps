package kps

import (
	"context"
	"errors"
	"testing"
	"time"
)

// listenCapture starts a listener that hands every accepted connection to the
// returned channel (rather than echoing streams), so a test can inspect the
// server-side connection directly.
func listenCapture(t *testing.T, ctx context.Context) (*Listener, <-chan Conn) {
	t.Helper()
	id, err := GenerateIdentity()
	if err != nil {
		t.Fatal(err)
	}
	ln, err := Listen(ctx, "127.0.0.1:0", Options{Identity: id})
	if err != nil {
		t.Fatal(err)
	}
	conns := make(chan Conn, 4)
	go func() {
		for {
			conn, err := ln.Accept(ctx)
			if err != nil {
				return
			}
			conns <- conn
		}
	}()
	return ln, conns
}

// TestConnCloseCode: CloseWithError(code) on one end surfaces as the peer's
// connection close reason via Err() — QUIC CONNECTION_CLOSE and the WebRTC
// bootstrap CONNECTION_CLOSE (SPEC §8/§9), normalized to *StreamError on both.
func TestConnCloseCode(t *testing.T) {
	for _, tc := range []struct {
		name string
		dial func(context.Context, string) (Conn, error)
	}{
		{"QUIC", Dial},
		{"WebRTC", DialWebRTC},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			ln, conns := listenCapture(t, ctx)
			defer ln.Close()

			client, err := tc.dial(ctx, ln.Address("127.0.0.1"))
			if err != nil {
				t.Fatal(err)
			}
			var srv Conn
			select {
			case srv = <-conns:
			case <-ctx.Done():
				t.Fatal("server did not accept the connection")
			}

			if err := client.CloseWithError(CodeProtocolError); err != nil {
				t.Fatalf("CloseWithError: %v", err)
			}

			select {
			case <-srv.Closed():
			case <-time.After(5 * time.Second):
				t.Fatal("server connection did not close")
			}

			var se *StreamError
			if err := srv.Err(); !errors.As(err, &se) || se.Code != CodeProtocolError {
				t.Fatalf("server Err() = %v; want *StreamError code %d", err, CodeProtocolError)
			}
		})
	}
}
