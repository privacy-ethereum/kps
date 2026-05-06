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

	handlersMu sync.RWMutex
	handlers   map[string]StreamHandler

	closeOnce sync.Once
	closed    chan struct{}
}

// StreamHandler runs in its own goroutine for each incoming stream
// matching the registered protocol name.
type StreamHandler func(*Stream)

type Options struct {
	// KeyFile path to the persistent ECDSA P-256 key (PEM-encoded
	// PKCS#8). Created if absent.
	KeyFile string
}

type pcEntry struct {
	ufrag string
	inbox chan packetIn
	conn  *pcPacketConn
	pc    *webrtc.PeerConnection
}

type packetIn struct {
	data []byte
	from net.Addr
}

// Listen binds a UDP socket and starts accepting kps connections.
// `addr` is a host:port string in net.Dial form (use ":0" for an
// ephemeral port).
func Listen(ctx context.Context, addr string, opts Options) (*Listener, error) {
	if opts.KeyFile == "" {
		opts.KeyFile = "kps.key"
	}
	identity, err := LoadOrCreateIdentity(opts.KeyFile)
	if err != nil {
		return nil, fmt.Errorf("kps: load identity: %w", err)
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
		handlers: map[string]StreamHandler{},
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

// Handle registers a handler for streams opened with the given name.
// The handler runs in its own goroutine. Replacing or registering
// concurrently with active connections is safe.
func (l *Listener) Handle(name string, handler StreamHandler) {
	l.handlersMu.Lock()
	defer l.handlersMu.Unlock()
	l.handlers[name] = handler
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
	// Pin the local ICE creds to the same value the browser saw in our
	// synthesized answer SDP (kps convention: ufrag = pwd = the value
	// the server learned from the inbound STUN binding's USERNAME).
	se.SetICECredentials(ufrag, ufrag)

	api := webrtc.NewAPI(webrtc.WithSettingEngine(se))
	pc, err := api.NewPeerConnection(webrtc.Configuration{
		Certificates: []webrtc.Certificate{l.identity.Certificate},
	})
	if err != nil {
		return nil
	}

	pc.OnDataChannel(func(dc *webrtc.DataChannel) {
		l.handlersMu.RLock()
		h, ok := l.handlers[dc.Label()]
		l.handlersMu.RUnlock()
		if !ok {
			_ = dc.Close()
			return
		}
		stream := newStream(dc)
		go func() {
			defer stream.Close()
			h(stream)
		}()
	})

	pc.OnConnectionStateChange(func(s webrtc.PeerConnectionState) {
		if s == webrtc.PeerConnectionStateClosed || s == webrtc.PeerConnectionStateFailed {
			l.removeEntry(ufrag)
		}
	})

	port := l.udp.LocalAddr().(*net.UDPAddr).Port
	offerSDP := buildClientOffer(ufrag, port)
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

	return &pcEntry{ufrag: ufrag, inbox: inbox, conn: pcc, pc: pc}
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
	_ = entry.conn.Close()
}

// buildClientOffer fabricates the SDP offer that the browser would have
// produced. We use a placeholder DTLS fingerprint because pion is
// configured with DisableCertificateFingerprintVerification — the server
// doesn't pin the browser's identity.
func buildClientOffer(ufrag string, port int) string {
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
		fmt.Sprintf("a=ice-pwd:%s", ufrag),
		fmt.Sprintf("a=fingerprint:sha-256 %s", placeholderFingerprint),
		"a=setup:active",
		"a=sctp-port:5000",
		"a=max-message-size:16384",
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
