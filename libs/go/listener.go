package kps

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"strings"
	"sync"
	"time"

	"github.com/pion/stun/v3"
	"github.com/pion/webrtc/v4"
)

// Listener accepts kps connections on a UDP port. The same port serves
// any number of clients, demultiplexed by their ICE ufrag.
type Listener struct {
	identity *Identity
	udp      *net.UDPConn

	mu      sync.Mutex
	byUfrag map[string]*pcEntry
	byAddr  map[netip.AddrPort]*pcEntry

	acceptCh chan *Conn

	closeOnce sync.Once
	closed    chan struct{}
}

type Options struct {
	// Identity, when set, is used directly. The Listener writes nothing
	// to disk; the caller is responsible for persistence. Use
	// kps.GenerateIdentity / kps.IdentityFromPEM / (*Identity).PEM.
	Identity *Identity

	// KeyFile path to the persistent combined PEM (PRIVATE KEY +
	// CERTIFICATE). Created if absent. Ignored when Identity is set.
	KeyFile string
}

type pcEntry struct {
	ufrag string
	inbox chan packetIn
	conn  *pcPacketConn
	pc    *webrtc.PeerConnection
	kc    *Conn
}

type packetIn struct {
	data []byte
	from net.Addr
}

// Listen binds a UDP socket and starts accepting kps connections.
// `addr` is a host:port string in net.Dial form (use ":0" for an
// ephemeral port).
func Listen(ctx context.Context, addr string, opts Options) (*Listener, error) {
	identity := opts.Identity
	if identity == nil {
		if opts.KeyFile == "" {
			opts.KeyFile = "kps.key"
		}
		var err error
		identity, err = LoadOrCreateIdentity(opts.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("kps: load identity: %w", err)
		}
	}

	udpAddr, err := net.ResolveUDPAddr("udp4", addr)
	if err != nil {
		return nil, fmt.Errorf("kps: resolve %q: %w", addr, err)
	}
	udp, err := net.ListenUDP("udp4", udpAddr)
	if err != nil {
		return nil, fmt.Errorf("kps: listen %q: %w", addr, err)
	}

	l := &Listener{
		identity: identity,
		udp:      udp,
		byUfrag:  map[string]*pcEntry{},
		byAddr:   map[netip.AddrPort]*pcEntry{},
		acceptCh: make(chan *Conn, 16),
		closed:   make(chan struct{}),
	}
	go l.pump()
	return l, nil
}

// Address returns the public-facing kps address ("ip:port:certhash") for
// the requested ip. If ip is empty, attempts to use the bound socket's
// address; pass "127.0.0.1" or a LAN/public IP explicitly for clients
// to dial across machines.
func (l *Listener) Address(ip string) string {
	if ip == "" {
		ip = l.udp.LocalAddr().(*net.UDPAddr).IP.String()
		if ip == "0.0.0.0" || ip == "::" {
			ip = "127.0.0.1"
		}
	}
	port := l.udp.LocalAddr().(*net.UDPAddr).Port
	return fmt.Sprintf("%s:%d:%s", ip, port, l.identity.Certhash)
}

// Port returns the UDP port the listener bound to.
func (l *Listener) Port() int {
	return l.udp.LocalAddr().(*net.UDPAddr).Port
}

// Certhash returns the multibase-encoded sha-256 multihash clients pin.
func (l *Listener) Certhash() string {
	return l.identity.Certhash
}

// Accept returns the next established connection, blocking until one arrives,
// ctx is done, or the listener closes. Each Conn carries its own streams via
// Conn.AcceptStream.
func (l *Listener) Accept(ctx context.Context) (*Conn, error) {
	select {
	case c := <-l.acceptCh:
		return c, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-l.closed:
		return nil, net.ErrClosed
	}
}

func (l *Listener) Close() error {
	var err error
	l.closeOnce.Do(func() {
		close(l.closed)
		err = l.udp.Close()
		l.mu.Lock()
		for _, e := range l.byUfrag {
			_ = e.pc.Close()
		}
		l.mu.Unlock()
	})
	return err
}

func (l *Listener) pump() {
	buf := make([]byte, 1500)
	for {
		n, srcAddr, err := l.udp.ReadFromUDPAddrPort(buf)
		if err != nil {
			select {
			case <-l.closed:
				return
			default:
			}
			if errors.Is(err, net.ErrClosed) {
				return
			}
			continue
		}

		l.mu.Lock()
		entry := l.byAddr[srcAddr]
		l.mu.Unlock()

		if entry == nil && stun.IsMessage(buf[:n]) {
			ufrag := extractUfrag(buf[:n])
			if ufrag != "" {
				l.mu.Lock()
				entry = l.byUfrag[ufrag]
				if entry == nil {
					entry = l.spawnPC(ufrag)
					if entry != nil {
						l.byUfrag[ufrag] = entry
					}
				}
				if entry != nil {
					l.byAddr[srcAddr] = entry
				}
				l.mu.Unlock()
			}
		}

		if entry == nil {
			continue
		}

		pkt := make([]byte, n)
		copy(pkt, buf[:n])
		select {
		case entry.inbox <- packetIn{data: pkt, from: net.UDPAddrFromAddrPort(srcAddr)}:
		case <-l.closed:
			return
		default:
			// inbox full — drop
		}
	}
}

func extractUfrag(p []byte) string {
	msg := &stun.Message{Raw: append([]byte(nil), p...)}
	if err := msg.Decode(); err != nil {
		return ""
	}
	attr, err := msg.Get(stun.AttrUsername)
	if err != nil {
		return ""
	}
	parts := strings.SplitN(string(attr), ":", 2)
	if len(parts) == 0 {
		return ""
	}
	return parts[0]
}

func (l *Listener) spawnPC(ufrag string) *pcEntry {
	inbox := make(chan packetIn, 256)
	pcc := newPCConn(inbox, l.udp, l.udp.LocalAddr())

	se := webrtc.SettingEngine{}
	se.SetLite(true)
	se.SetICEUDPMux(&singleConnMux{conn: pcc})
	se.DisableCertificateFingerprintVerification(true)
	// Derive the ICE password from the pinned certhash (SPEC §5.2); the client
	// computes the identical value, so only a certhash-holder passes STUN
	// integrity. Pin our local creds to (ufrag, derived pwd).
	pwd := deriveICEPwd(l.identity.digest, ufrag)
	se.SetICECredentials(ufrag, pwd)

	api := webrtc.NewAPI(webrtc.WithSettingEngine(se))
	pc, err := api.NewPeerConnection(webrtc.Configuration{
		Certificates: []webrtc.Certificate{l.identity.Certificate},
	})
	if err != nil {
		return nil
	}

	// newConn owns pc.OnDataChannel: each client-opened channel surfaces as a
	// Stream on the Conn's accept queue. The negotiated bootstrap channel is
	// not announced via DCEP, so it never appears here.
	kc := newConn(pc)

	var acceptOnce sync.Once
	pc.OnConnectionStateChange(func(s webrtc.PeerConnectionState) {
		switch s {
		case webrtc.PeerConnectionStateConnected:
			acceptOnce.Do(func() {
				select {
				case l.acceptCh <- kc:
				case <-l.closed:
				}
			})
		case webrtc.PeerConnectionStateClosed, webrtc.PeerConnectionStateFailed:
			kc.markClosed(nil)
			l.removeEntry(ufrag)
		}
	})

	port := l.udp.LocalAddr().(*net.UDPAddr).Port
	offerSDP := buildClientOffer(ufrag, pwd, port)
	if err := pc.SetRemoteDescription(webrtc.SessionDescription{Type: webrtc.SDPTypeOffer, SDP: offerSDP}); err != nil {
		_ = pc.Close()
		return nil
	}
	answer, err := pc.CreateAnswer(nil)
	if err != nil {
		_ = pc.Close()
		return nil
	}
	if err := pc.SetLocalDescription(answer); err != nil {
		_ = pc.Close()
		return nil
	}

	return &pcEntry{ufrag: ufrag, inbox: inbox, conn: pcc, pc: pc, kc: kc}
}

func (l *Listener) removeEntry(ufrag string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	entry := l.byUfrag[ufrag]
	if entry == nil {
		return
	}
	delete(l.byUfrag, ufrag)
	for k, v := range l.byAddr {
		if v == entry {
			delete(l.byAddr, k)
		}
	}
	if entry.kc != nil {
		entry.kc.markClosed(nil)
	}
	_ = entry.conn.Close()
}

// buildClientOffer fabricates the SDP offer that the browser would have
// produced. We use a placeholder DTLS fingerprint because pion is
// configured with DisableCertificateFingerprintVerification — the server
// doesn't pin the browser's identity.
func buildClientOffer(ufrag, pwd string, port int) string {
	const placeholderFingerprint = "AA:BB:CC:DD:EE:FF:00:11:22:33:44:55:66:77:88:99:AA:BB:CC:DD:EE:FF:00:11:22:33:44:55:66:77:88:99"
	lines := []string{
		"v=0",
		"o=- 0 0 IN IP4 0.0.0.0",
		"s=-",
		"t=0 0",
		fmt.Sprintf("m=application %d UDP/DTLS/SCTP webrtc-datachannel", port),
		"c=IN IP4 0.0.0.0",
		"a=mid:0",
		fmt.Sprintf("a=ice-ufrag:%s", ufrag),
		fmt.Sprintf("a=ice-pwd:%s", pwd),
		fmt.Sprintf("a=fingerprint:sha-256 %s", placeholderFingerprint),
		"a=setup:active",
		"a=sctp-port:5000",
		"a=max-message-size:1048576",
	}
	return strings.Join(lines, "\r\n") + "\r\n"
}

// pcPacketConn implements net.PacketConn for a single PeerConnection.
// Reads pull from a per-PC inbox channel; writes go to the shared real
// UDP socket.
type pcPacketConn struct {
	inbox chan packetIn
	udp   *net.UDPConn
	laddr net.Addr

	closeOnce sync.Once
	closed    chan struct{}
}

func newPCConn(inbox chan packetIn, udp *net.UDPConn, laddr net.Addr) *pcPacketConn {
	return &pcPacketConn{
		inbox:  inbox,
		udp:    udp,
		laddr:  laddr,
		closed: make(chan struct{}),
	}
}

func (c *pcPacketConn) ReadFrom(p []byte) (int, net.Addr, error) {
	select {
	case e, ok := <-c.inbox:
		if !ok {
			return 0, nil, net.ErrClosed
		}
		return copy(p, e.data), e.from, nil
	case <-c.closed:
		return 0, nil, net.ErrClosed
	}
}

func (c *pcPacketConn) WriteTo(p []byte, addr net.Addr) (int, error) {
	select {
	case <-c.closed:
		return 0, net.ErrClosed
	default:
	}
	return c.udp.WriteTo(p, addr)
}

func (c *pcPacketConn) Close() error {
	c.closeOnce.Do(func() { close(c.closed) })
	return nil
}

func (c *pcPacketConn) LocalAddr() net.Addr             { return c.laddr }
func (c *pcPacketConn) SetDeadline(_ time.Time) error   { return nil }
func (c *pcPacketConn) SetReadDeadline(_ time.Time) error  { return nil }
func (c *pcPacketConn) SetWriteDeadline(_ time.Time) error { return nil }

// singleConnMux satisfies pion's UDPMux interface by always returning a
// single PacketConn regardless of ufrag — used per-PeerConnection so
// that PC's ICE agent reads from its own inbox.
type singleConnMux struct {
	conn *pcPacketConn
}

func (m *singleConnMux) GetConn(_ string, _ net.Addr) (net.PacketConn, error) {
	return m.conn, nil
}
func (m *singleConnMux) Close() error                  { return m.conn.Close() }
func (m *singleConnMux) GetListenAddresses() []net.Addr { return []net.Addr{m.conn.LocalAddr()} }
func (m *singleConnMux) RemoveConnByUfrag(_ string)     {}
