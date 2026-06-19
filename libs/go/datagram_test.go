package kps

import (
	"context"
	"errors"
	"testing"
	"time"
)

// listenDatagramEcho accepts connections and echoes every datagram back.
func listenDatagramEcho(t *testing.T, ctx context.Context) *Listener {
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
			go func(conn Conn) {
				for {
					d, err := conn.ReceiveDatagram(ctx)
					if err != nil {
						return
					}
					_ = conn.SendDatagram(d)
				}
			}(conn)
		}
	}()
	return ln
}

// datagramRoundTrip sends a datagram until an echo comes back. Datagrams are
// best-effort, so we retry rather than asserting a single send arrives.
func datagramRoundTrip(t *testing.T, ctx context.Context, dial func(context.Context, string) (Conn, error), addr string) {
	t.Helper()
	conn, err := dial(ctx, addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	got := make(chan []byte, 8)
	go func() {
		for {
			d, err := conn.ReceiveDatagram(ctx)
			if err != nil {
				return
			}
			got <- d
		}
	}()

	msg := []byte("ping-datagram")
	_ = conn.SendDatagram(msg)
	tick := time.NewTicker(100 * time.Millisecond)
	defer tick.Stop()
	deadline := time.After(10 * time.Second)
	for {
		select {
		case d := <-got:
			if string(d) == string(msg) {
				return
			}
		case <-tick.C:
			_ = conn.SendDatagram(msg) // retry (channel may not be open yet, or a datagram was dropped)
		case <-deadline:
			t.Fatal("no datagram echo within deadline")
		}
	}
}

func TestQUIC_Datagram(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	ln := listenDatagramEcho(t, ctx)
	defer ln.Close()
	datagramRoundTrip(t, ctx, Dial, ln.Address("127.0.0.1"))
}

func TestWebRTC_Datagram(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	ln := listenDatagramEcho(t, ctx)
	defer ln.Close()
	datagramRoundTrip(t, ctx, DialWebRTC, ln.Address("127.0.0.1"))
}

func TestDatagramTooLarge(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	ln := listenDatagramEcho(t, ctx)
	defer ln.Close()
	conn, err := Dial(ctx, ln.Address("127.0.0.1"))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	err = conn.SendDatagram(make([]byte, 65536))
	var tooLarge *DatagramTooLargeError
	if !errors.As(err, &tooLarge) {
		t.Fatalf("oversize SendDatagram = %v, want *DatagramTooLargeError", err)
	}
	if tooLarge.MaxDatagramPayloadSize <= 0 {
		t.Fatalf("reported limit = %d, want > 0", tooLarge.MaxDatagramPayloadSize)
	}
}
