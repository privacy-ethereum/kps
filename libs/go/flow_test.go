package kps

import (
	"bytes"
	"context"
	"fmt"
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

// TestWebRTCStreamSlots_RecycleAcrossManyStreams opens 2×MAX_STREAMS streams,
// closing each, over one WebRTC connection. Only maxStreams may be open at once,
// so completing twice that many requires the peer's stream slots to be reclaimed
// on retirement and re-granted (MAX_STREAMS) — repeatedly, not just once. A slot
// leak (a retired stream that never returns its slot) would let the first
// maxStreams opens through and then block OpenStream forever, tripping the
// context deadline. The block-and-retire case above only exercises a single
// reclaim; this exercises sustained recycling.
//
// NOTE: pion allocates SCTP stream ids monotonically, so this does NOT exercise
// stream-id *reuse*. Browsers free and reuse low ids as channels close, which is
// its own hazard (kps#4) — covered only by the Playwright browser leg.
func TestWebRTCStreamSlots_RecycleAcrossManyStreams(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	ln := listenWith(t, ctx, echoHandler)
	defer ln.Close()

	conn, err := DialWebRTC(ctx, ln.Address("127.0.0.1"))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	total := 2 * defaultFlowLimits.maxStreams
	for i := uint64(0); i < total; i++ {
		s, err := conn.OpenStream(ctx)
		if err != nil {
			// Past maxStreams this only fails if an earlier slot wasn't reclaimed.
			t.Fatalf("open %d/%d (slot not reclaimed?): %v", i, total, err)
		}
		msg := fmt.Sprintf("req-%d", i)
		if _, err := s.Write([]byte(msg)); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
		if err := s.CloseWrite(); err != nil {
			t.Fatalf("closeWrite %d: %v", i, err)
		}
		got, err := io.ReadAll(s) // reads the echo, then the peer's FIN → retires
		if err != nil {
			t.Fatalf("read %d: %v", i, err)
		}
		if string(got) != msg {
			t.Fatalf("echo %d mismatch: got %q want %q", i, got, msg)
		}
		_ = s.Close()
	}
}
