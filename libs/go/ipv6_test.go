package kps

import (
	"context"
	"io"
	"net/netip"
	"strings"
	"testing"
	"time"
)

// IPv6 support: the bracketed address grammar, a dual-stack listener, and IP6
// SDP synthesis.

// TestIPv6_QUIC exercises QUIC over IPv6 end-to-end: a dual-stack wildcard
// listener (one socket, both families) reached via the ::1 loopback.
func TestIPv6_QUIC(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	ln := listenWithBind(t, ctx, ":0", echoHandler)
	defer ln.Close()

	addr := ln.Address("::1")
	if !strings.HasPrefix(addr, "[") {
		t.Fatalf("expected bracketed IPv6 address, got %q", addr)
	}
	conn, err := Dial(ctx, addr)
	if err != nil {
		t.Fatalf("dial %s: %v", addr, err)
	}
	defer conn.Close()
	s, err := conn.OpenStream(ctx)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if _, err := s.Write([]byte("hello-over-ipv6")); err != nil {
		t.Fatal(err)
	}
	_ = s.CloseWrite()
	got, err := io.ReadAll(s)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if string(got) != "hello-over-ipv6" {
		t.Fatalf("echo mismatch over IPv6: got %q", got)
	}
}

// TestIPv6_AddressRoundTrip covers bracketed-IPv6 parsing and formatting (v4
// stays bare).
func TestIPv6_AddressRoundTrip(t *testing.T) {
	for _, s := range []string{
		"[2001:db8::1]:4242:uEiABC",
		"[::1]:60949:uEiABC",
		"203.0.113.5:4242:uEiABC",
	} {
		a, err := ParseAddress(s)
		if err != nil {
			t.Fatalf("ParseAddress(%q): %v", s, err)
		}
		if got := joinHostPortCerthash(a.IP, a.Port, a.Certhash); got != s {
			t.Fatalf("round-trip: got %q want %q", got, s)
		}
	}
}

// TestIPv6_SDP covers the WebRTC IP6 wire change. A live v6 WebRTC handshake is
// browser / globally-addressed territory; pion's loopback ICE gathering on a
// multi-interface host won't form a ::1 candidate pair, so it isn't covered by
// the Go WebRTC client here.
func TestIPv6_SDP(t *testing.T) {
	ans6 := synthesizeWebRTCAnswer(Address{IP: "2001:db8::1", Port: 4242}, "uf", "pw", "AA:BB")
	for _, want := range []string{
		"o=- 0 0 IN IP6 ::",
		"c=IN IP6 2001:db8::1",
		"a=candidate:1 1 UDP 1 2001:db8::1 4242 typ host",
	} {
		if !strings.Contains(ans6, want) {
			t.Fatalf("v6 answer missing %q:\n%s", want, ans6)
		}
	}
	if ans4 := synthesizeWebRTCAnswer(Address{IP: "1.2.3.4", Port: 4242}, "uf", "pw", "AA:BB"); !strings.Contains(ans4, "c=IN IP4 1.2.3.4") {
		t.Fatalf("v4 answer wrong:\n%s", ans4)
	}

	if off6 := buildClientOffer("uf", "pw", 4242, netip.MustParseAddr("::1")); !strings.Contains(off6, "c=IN IP6 ::") {
		t.Fatalf("v6 offer wrong:\n%s", off6)
	}
	if off4 := buildClientOffer("uf", "pw", 4242, netip.MustParseAddr("127.0.0.1")); !strings.Contains(off4, "c=IN IP4 0.0.0.0") {
		t.Fatalf("v4 offer wrong:\n%s", off4)
	}
}
