package kps

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"strings"
	"sync"

	"github.com/pion/webrtc/v4"
)

// DialWebRTC opens a kps connection over the WebRTC transport from Go. Native
// clients default to QUIC (kps.Dial); this is the explicit override the spec
// allows for tests/debugging and for interop with browser-facing listeners
// (SPEC §5.4). It mirrors the browser client: the offerer synthesizes the
// server's answer from the address, derives the ICE password from the certhash,
// and lets pion pin the server's DTLS certificate against the answer fingerprint.
func DialWebRTC(ctx context.Context, addr string) (Conn, error) {
	a, err := ParseAddress(addr)
	if err != nil {
		return nil, err
	}
	digest, err := decodeCerthash(a.Certhash)
	if err != nil {
		return nil, err
	}
	ufrag, err := randUfrag()
	if err != nil {
		return nil, err
	}
	pwd := deriveICEPwd(digest, ufrag)

	se := webrtc.SettingEngine{}
	se.SetICECredentials(ufrag, pwd) // force local ICE creds (server derives the same)
	se.SetNetworkTypes([]webrtc.NetworkType{webrtc.NetworkTypeUDP4, webrtc.NetworkTypeUDP6})
	// Gather loopback candidates so a client can reach a server on the same host
	// (127.0.0.1 / ::1) — needed for IPv6 loopback, where there may be no other
	// v6 path, and harmless otherwise.
	se.SetIncludeLoopbackCandidate(true)
	api := webrtc.NewAPI(webrtc.WithSettingEngine(se))
	pc, err := api.NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		return nil, err
	}

	// Pre-allocate the negotiated bootstrap channel so the offer carries the
	// application m-line; it is not announced via DCEP and never surfaces as a
	// stream.
	negotiated := true
	var bootstrapID uint16 = 0
	if _, err := pc.CreateDataChannel("_kps_bootstrap", &webrtc.DataChannelInit{
		Negotiated: &negotiated, ID: &bootstrapID,
	}); err != nil {
		_ = pc.Close()
		return nil, err
	}

	offer, err := pc.CreateOffer(nil)
	if err != nil {
		_ = pc.Close()
		return nil, err
	}
	if err := pc.SetLocalDescription(offer); err != nil {
		_ = pc.Close()
		return nil, err
	}

	answer := synthesizeWebRTCAnswer(a, ufrag, pwd, digestToFingerprint(digest))
	if err := pc.SetRemoteDescription(webrtc.SessionDescription{Type: webrtc.SDPTypeAnswer, SDP: answer}); err != nil {
		_ = pc.Close()
		return nil, err
	}

	conn := newConn(pc)
	connected := make(chan struct{})
	var once sync.Once
	pc.OnConnectionStateChange(func(s webrtc.PeerConnectionState) {
		switch s {
		case webrtc.PeerConnectionStateConnected:
			once.Do(func() { close(connected) })
		case webrtc.PeerConnectionStateFailed, webrtc.PeerConnectionStateClosed:
			conn.markClosed(nil)
		}
	})

	select {
	case <-connected:
		return conn, nil
	case <-ctx.Done():
		_ = pc.Close()
		return nil, ctx.Err()
	}
}

func randUfrag() (string, error) {
	b := make([]byte, 9)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(b), nil
}

func digestToFingerprint(d []byte) string {
	parts := make([]string, len(d))
	for i, b := range d {
		parts[i] = fmt.Sprintf("%02X", b)
	}
	return strings.Join(parts, ":")
}

func synthesizeWebRTCAnswer(a Address, ufrag, pwd, fingerprint string) string {
	fam, anyAddr := "IP4", "0.0.0.0"
	if strings.Contains(a.IP, ":") {
		fam, anyAddr = "IP6", "::"
	}
	lines := []string{
		"v=0",
		fmt.Sprintf("o=- 0 0 IN %s %s", fam, anyAddr),
		"s=-",
		"t=0 0",
		"a=ice-lite",
		fmt.Sprintf("m=application %d UDP/DTLS/SCTP webrtc-datachannel", a.Port),
		fmt.Sprintf("c=IN %s %s", fam, a.IP),
		"a=mid:0",
		fmt.Sprintf("a=ice-ufrag:%s", ufrag),
		fmt.Sprintf("a=ice-pwd:%s", pwd),
		fmt.Sprintf("a=fingerprint:sha-256 %s", fingerprint),
		"a=setup:passive",
		"a=sctp-port:5000",
		"a=max-message-size:1048576",
		fmt.Sprintf("a=candidate:1 1 UDP 1 %s %d typ host", a.IP, a.Port),
	}
	return strings.Join(lines, "\r\n") + "\r\n"
}
