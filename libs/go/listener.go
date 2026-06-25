package kps

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"strings"
	"sync"
	"time"

	"github.com/pion/stun/v3"
	"github.com/pion/webrtc/v4"
	"github.com/quic-go/quic-go"
)

// Listener accepts kps connections on a UDP port. The same port serves
// any number of clients, demultiplexed by their ICE ufrag.
type Listener struct {
	identity *Identity
	udp      *net.UDPConn

	mu      sync.Mutex
	byUfrag map[string]*pcEntry
	byAddr  map[netip.AddrPort]*pcEntry

	acceptCh chan Conn

	// QUIC transport sharing the same UDP socket (SPEC §5.1). pump feeds it the
	// packets that are not WebRTC.
	qpc    *quicPacketConn
	quicTr *quic.Transport
	quicLn *quic.Listener

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
	kc    *webrtcConn
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

	// "udp" (not "udp4") + a wildcard host binds dual-stack on most systems
	// (Linux defaults IPV6_V6ONLY=0), so one socket and port serve both IPv4
	// and IPv6 clients. The demux and routing are address-family agnostic.
	udpAddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return nil, fmt.Errorf("kps: resolve %q: %w", addr, err)
	}
	udp, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		return nil, fmt.Errorf("kps: listen %q: %w", addr, err)
	}

	l := &Listener{
		identity: identity,
		udp:      udp,
		byUfrag:  map[string]*pcEntry{},
		byAddr:   map[netip.AddrPort]*pcEntry{},
		acceptCh: make(chan Conn, 16),
		closed:   make(chan struct{}),
	}
	if err := l.startQUIC(); err != nil {
		_ = udp.Close()
		return nil, err
	}
	go l.pump()
	go l.acceptQUIC()
	return l, nil
}

// startQUIC brings up a QUIC listener over a virtual PacketConn that pump feeds,
// sharing the same UDP socket and identity certificate (SPEC §5.1, §5.3).
func (l *Listener) startQUIC() error {
	cert, err := l.identity.tlsCertificate()
	if err != nil {
		return fmt.Errorf("kps: quic tls cert: %w", err)
	}
	tlsConf := &tls.Config{
		Certificates: []tls.Certificate{cert},
		NextProtos:   []string{alpnKPS},
		MinVersion:   tls.VersionTLS13,
	}
	l.qpc = newQUICPacketConn(l.udp)
	l.quicTr = &quic.Transport{Conn: l.qpc}
	ln, err := l.quicTr.Listen(tlsConf, &quic.Config{EnableDatagrams: true})
	if err != nil {
		return fmt.Errorf("kps: quic listen: %w", err)
	}
	l.quicLn = ln
	return nil
}

// acceptQUIC delivers accepted QUIC connections to the same queue as WebRTC.
func (l *Listener) acceptQUIC() {
	for {
		qc, err := l.quicLn.Accept(context.Background())
		if err != nil {
			return
		}
		select {
		case l.acceptCh <- newQUICConn(qc):
		case <-l.closed:
			_ = qc.CloseWithError(0, "")
			return
		}
	}
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
	return joinHostPortCerthash(ip, port, l.identity.Certhash)
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
func (l *Listener) Accept(ctx context.Context) (Conn, error) {
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
		if l.quicLn != nil {
			_ = l.quicLn.Close()
		}
		// Close the virtual PacketConn BEFORE the Transport: Transport.Close
		// waits for its read loop, which is blocked in qpc.ReadFrom until qpc
		// closes — closing them in the other order deadlocks.
		if l.qpc != nil {
			_ = l.qpc.Close()
		}
		if l.quicTr != nil {
			_ = l.quicTr.Close()
		}
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
					entry = l.spawnPC(ufrag, srcAddr.Addr())
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

		pkt := make([]byte, n)
		copy(pkt, buf[:n])

		if entry == nil {
			// Not an established WebRTC peer and not a new STUN binding: the only
			// other transport is QUIC, so hand it to the QUIC transport (SPEC §5.1).
			select {
			case l.qpc.inbox <- packetIn{data: pkt, from: net.UDPAddrFromAddrPort(srcAddr)}:
			case <-l.closed:
				return
			default:
				// QUIC inbox full — drop
			}
			continue
		}

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

func (l *Listener) spawnPC(ufrag string, clientIP netip.Addr) *pcEntry {
	inbox := make(chan packetIn, 256)
	pcc := newPCConn(inbox, l.udp, l.udp.LocalAddr())

	se := webrtc.SettingEngine{}
	se.SetLite(true)
	se.SetNetworkTypes([]webrtc.NetworkType{webrtc.NetworkTypeUDP4, webrtc.NetworkTypeUDP6})
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
	offerSDP := buildClientOffer(ufrag, pwd, port, clientIP)
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
func buildClientOffer(ufrag, pwd string, port int, clientIP netip.Addr) string {
	const placeholderFingerprint = "AA:BB:CC:DD:EE:FF:00:11:22:33:44:55:66:77:88:99:AA:BB:CC:DD:EE:FF:00:11:22:33:44:55:66:77:88:99"
	fam, anyAddr := "IP4", "0.0.0.0"
	if clientIP.IsValid() && !clientIP.Unmap().Is4() {
		fam, anyAddr = "IP6", "::"
	}
	lines := []string{
		"v=0",
		fmt.Sprintf("o=- 0 0 IN %s %s", fam, anyAddr),
		"s=-",
		"t=0 0",
		fmt.Sprintf("m=application %d UDP/DTLS/SCTP webrtc-datachannel", port),
		fmt.Sprintf("c=IN %s %s", fam, anyAddr),
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

// quicPacketConn implements net.PacketConn for the shared QUIC transport.
// Reads pull from an inbox channel (fed by pump with the non-WebRTC packets);
// writes go to the shared real UDP socket. quic.Transport demultiplexes its own
// connections by connection ID.
type quicPacketConn struct {
	inbox chan packetIn
	udp   *net.UDPConn
	laddr net.Addr

	closeOnce sync.Once
	closed    chan struct{}
}

func newQUICPacketConn(udp *net.UDPConn) *quicPacketConn {
	return &quicPacketConn{
		inbox:  make(chan packetIn, 256),
		udp:    udp,
		laddr:  udp.LocalAddr(),
		closed: make(chan struct{}),
	}
}

func (c *quicPacketConn) ReadFrom(p []byte) (int, net.Addr, error) {
	select {
	case e := <-c.inbox:
		return copy(p, e.data), e.from, nil
	case <-c.closed:
		return 0, nil, net.ErrClosed
	}
}

func (c *quicPacketConn) WriteTo(p []byte, addr net.Addr) (int, error) {
	select {
	case <-c.closed:
		return 0, net.ErrClosed
	default:
	}
	return c.udp.WriteTo(p, addr)
}

func (c *quicPacketConn) Close() error {
	c.closeOnce.Do(func() { close(c.closed) })
	return nil
}

func (c *quicPacketConn) LocalAddr() net.Addr                { return c.laddr }
func (c *quicPacketConn) SetDeadline(_ time.Time) error      { return nil }
func (c *quicPacketConn) SetReadDeadline(_ time.Time) error  { return nil }
func (c *quicPacketConn) SetWriteDeadline(_ time.Time) error { return nil }

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
