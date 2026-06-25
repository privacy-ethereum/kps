package kps

import (
	"context"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"
)

// The executable interop matrix for the QUIC transport (SPEC §10, items 2/4–8).
// The browser↔Go WebRTC path (item 1) and same-port coexistence are covered by
// the Playwright test in tests/interop. These tests dial a real QUIC client, so
// they require real UDP socket access (see tests/interop/README and CI).

// listenWith starts a kps listener that runs onStream for every accepted stream.
func listenWith(t *testing.T, ctx context.Context, onStream func(Stream)) *Listener {
	return listenWithBind(t, ctx, "127.0.0.1:0", onStream)
}

func listenWithBind(t *testing.T, ctx context.Context, bind string, onStream func(Stream)) *Listener {
	t.Helper()
	id, err := GenerateIdentity()
	if err != nil {
		t.Fatal(err)
	}
	ln, err := Listen(ctx, bind, Options{Identity: id})
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		for {
			conn, err := ln.Accept(ctx)
			if err != nil {
				return
			}
			go func(conn Conn) {
				for {
					s, err := conn.AcceptStream(ctx)
					if err != nil {
						return
					}
					go onStream(s)
				}
			}(conn)
		}
	}()
	return ln
}

func echoHandler(s Stream) {
	_, _ = io.Copy(s, s)
	_ = s.CloseWrite()
}

// TestQUIC_MultiStream: many concurrent streams on one connection (SPEC §10.5).
func TestQUIC_MultiStream(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	ln := listenWith(t, ctx, echoHandler)
	defer ln.Close()

	conn, err := Dial(ctx, ln.Address("127.0.0.1"))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			s, err := conn.OpenStream(ctx)
			if err != nil {
				t.Errorf("open %d: %v", i, err)
				return
			}
			msg := fmt.Sprintf("stream-%d-payload", i)
			if _, err := s.Write([]byte(msg)); err != nil {
				t.Errorf("write %d: %v", i, err)
				return
			}
			_ = s.CloseWrite()
			got, err := io.ReadAll(s)
			if err != nil {
				t.Errorf("read %d: %v", i, err)
				return
			}
			if string(got) != msg {
				t.Errorf("stream %d echo mismatch: got %q want %q", i, got, msg)
			}
		}(i)
	}
	wg.Wait()
}

// TestQUIC_MultiConn: multiple independent connections from one process to the
// same address (SPEC §10.4).
func TestQUIC_MultiConn(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	ln := listenWith(t, ctx, echoHandler)
	defer ln.Close()
	addr := ln.Address("127.0.0.1")

	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			conn, err := Dial(ctx, addr)
			if err != nil {
				t.Errorf("dial %d: %v", i, err)
				return
			}
			defer conn.Close()
			s, err := conn.OpenStream(ctx)
			if err != nil {
				t.Errorf("open %d: %v", i, err)
				return
			}
			msg := fmt.Sprintf("conn-%d", i)
			_, _ = s.Write([]byte(msg))
			_ = s.CloseWrite()
			got, err := io.ReadAll(s)
			if err != nil || string(got) != msg {
				t.Errorf("conn %d: got %q err %v", i, got, err)
			}
		}(i)
	}
	wg.Wait()
}

// TestQUIC_CloseWrite_PeerSeesEOF: graceful write-half close surfaces as EOF on
// the peer after all bytes (SPEC §10.6).
func TestQUIC_CloseWrite_PeerSeesEOF(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	got := make(chan string, 1)
	ln := listenWith(t, ctx, func(s Stream) {
		b, err := io.ReadAll(s) // returns nil error at EOF
		if err != nil {
			got <- "ERR:" + err.Error()
			return
		}
		got <- string(b)
	})
	defer ln.Close()

	conn, err := Dial(ctx, ln.Address("127.0.0.1"))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	s, err := conn.OpenStream(ctx)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if _, err := s.Write([]byte("hello")); err != nil {
		t.Fatal(err)
	}
	if err := s.CloseWrite(); err != nil {
		t.Fatal(err)
	}
	select {
	case v := <-got:
		if v != "hello" {
			t.Fatalf("peer saw %q, want clean EOF after \"hello\"", v)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("peer never observed EOF")
	}
}

// TestQUIC_ResetWrite_PeerSeesError: aborting the write half surfaces as a
// stream error on the peer, not EOF (SPEC §10.8).
func TestQUIC_ResetWrite_PeerSeesError(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
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

	conn, err := Dial(ctx, ln.Address("127.0.0.1"))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	s, err := conn.OpenStream(ctx)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	_, _ = s.Write([]byte("partial"))
	time.Sleep(50 * time.Millisecond)
	if err := s.ResetWrite(CodeReset); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-readErr:
		if err == nil || err == io.EOF {
			t.Fatalf("peer read ended with %v, want a stream error (not EOF)", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("peer read never errored after resetWrite")
	}
}

// TestQUIC_CancelRead_StopsPeer: cancelling the read half tells the peer to stop
// sending; the peer's writes eventually fail (SPEC §10.7).
func TestQUIC_CancelRead_StopsPeer(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
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

	conn, err := Dial(ctx, ln.Address("127.0.0.1"))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	s, err := conn.OpenStream(ctx)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	// Establish the stream at the server, then stop wanting inbound bytes.
	if _, err := s.Write([]byte("hi")); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)
	if err := s.CancelRead(CodeCancelled); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-writeErr:
		if err == nil {
			t.Fatal("peer kept writing after cancelRead; expected its writes to fail")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("peer writes never failed after cancelRead")
	}
}
