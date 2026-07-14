package kps

import (
	"context"
	"net"
	"testing"
	"time"
)

// TestRemoteAddr verifies Conn.RemoteAddr on both transports and both sides:
// the accept side sees the client's UDP endpoint (per-IP policy, e.g. rate
// limits), the dial side sees the dialed endpoint.
func TestRemoteAddr(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	id, err := GenerateIdentity()
	if err != nil {
		t.Fatal(err)
	}
	ln, err := Listen(ctx, "127.0.0.1:0", Options{Identity: id})
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()
	addr := ln.Address("127.0.0.1")

	for _, tc := range []struct {
		name string
		dial func(context.Context, string) (Conn, error)
	}{
		{"quic", Dial},
		{"webrtc", DialWebRTC},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client, err := tc.dial(ctx, addr)
			if err != nil {
				t.Fatalf("dial: %v", err)
			}
			defer client.Close()
			server, err := ln.Accept(ctx)
			if err != nil {
				t.Fatalf("accept: %v", err)
			}
			defer server.Close()

			// Dial side: the dialed endpoint.
			cr := client.RemoteAddr()
			if cr == nil {
				t.Fatal("client RemoteAddr is nil")
			}
			cu, ok := cr.(*net.UDPAddr)
			if !ok {
				t.Fatalf("client RemoteAddr is %T, want *net.UDPAddr", cr)
			}
			if !cu.IP.Equal(net.ParseIP("127.0.0.1")) || cu.Port != ln.Port() {
				t.Fatalf("client RemoteAddr = %v, want 127.0.0.1:%d", cu, ln.Port())
			}

			// Accept side: the client's source endpoint. Not necessarily
			// loopback even for a 127.0.0.1 dial — a WebRTC client gathers
			// candidates on every interface, and the winning pair may ride a
			// LAN interface on a multi-homed host.
			sr := server.RemoteAddr()
			if sr == nil {
				t.Fatal("server RemoteAddr is nil")
			}
			su, ok := sr.(*net.UDPAddr)
			if !ok {
				t.Fatalf("server RemoteAddr is %T, want *net.UDPAddr", sr)
			}
			if su.IP.IsUnspecified() || su.IP == nil {
				t.Fatalf("server RemoteAddr = %v, want a concrete source IP", su)
			}
			if su.Port == 0 {
				t.Fatalf("server RemoteAddr = %v, want a non-zero source port", su)
			}
		})
	}
}
