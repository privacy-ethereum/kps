package kps

import (
	"bytes"
	"context"
	"io"
	"testing"
	"time"
)

// Flow-control semantics over a real Go↔Go WebRTC connection (SPEC §6.5).
// Echo tests can't catch backpressure regressions — these use a non-reading
// receiver.

func startWebRTCPair(t *testing.T) (client, server Conn) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)
	l, err := Listen(ctx, "127.0.0.1:0", Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	dialDone := make(chan Conn, 1)
	go func() {
		c, err := DialWebRTC(ctx, l.Address("127.0.0.1"))
		if err != nil {
			t.Errorf("dial: %v", err)
			close(dialDone)
			return
		}
		dialDone <- c
	}()
	s, err := l.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	c, ok := <-dialDone
	if !ok {
		t.Fatal("dial failed")
	}
	t.Cleanup(func() { _ = c.Close(); _ = s.Close() })
	return c, s
}

// A sender must block at the receiver's advertised windows while the receiving
// application does not read, and resume once it does.
func TestWebRTCBackpressure_NonReadingReceiver(t *testing.T) {
	client, server := startWebRTCPair(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	cs, err := client.OpenStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	ss, err := server.AcceptStream(ctx)
	if err != nil {
		t.Fatal(err)
	}

	// More than initialMaxStreamData (1 MiB): the tail must wait for credit.
	payload := bytes.Repeat([]byte{0xAB}, int(defaultFlowLimits.maxStreamData)+64<<10)
	wrote := make(chan error, 1)
	go func() {
		_, err := cs.Write(payload)
		if err == nil {
			err = cs.CloseWrite()
		}
		wrote <- err
	}()

	select {
	case err := <-wrote:
		t.Fatalf("write completed without the receiver reading (err=%v) — no backpressure", err)
	case <-time.After(2 * time.Second):
		// blocked, as required
	}

	// Reading drains the buffer, returns credit, and completes the write.
	got, err := io.ReadAll(ss)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("payload mismatch: got %d bytes, want %d", len(got), len(payload))
	}
	if err := <-wrote; err != nil {
		t.Fatalf("write: %v", err)
	}
}

// OpenStream must wait at the peer's MAX_STREAMS limit and resume when a
// stream retires (both halves terminal, channel closed, drained).
func TestWebRTCStreamLimit_BlocksAndRetires(t *testing.T) {
	client, server := startWebRTCPair(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	// Exhaust the peer's stream credit.
	streams := make([]Stream, 0, defaultFlowLimits.maxStreams)
	for i := uint64(0); i < defaultFlowLimits.maxStreams; i++ {
		s, err := client.OpenStream(ctx)
		if err != nil {
			t.Fatalf("open %d: %v", i, err)
		}
		streams = append(streams, s)
	}

	blockedCtx, blockedCancel := context.WithTimeout(ctx, 1500*time.Millisecond)
	defer blockedCancel()
	if _, err := client.OpenStream(blockedCtx); err == nil {
		t.Fatal("open beyond MAX_STREAMS should block")
	}

	// Retire one stream: close both ends and drain.
	ss, err := server.AcceptStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	_ = streams[0].Close()
	_ = ss.Close()
	_, _ = io.ReadAll(ss)

	s, err := client.OpenStream(ctx)
	if err != nil {
		t.Fatalf("open after retirement: %v", err)
	}
	_ = s.Close()
}
