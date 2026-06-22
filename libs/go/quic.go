package kps

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"sync/atomic"

	"github.com/quic-go/quic-go"
)

// alpnKPS is intentionally non-identifying (SECURITY.md §3): KPS-over-QUIC
// advertises the common HTTP/3 ALPN so a passive observer cannot keyword-match
// KPS on the (publicly decryptable) Initial. KPS version negotiation does not
// live in the ALPN — it belongs in the address or the first application bytes.
const alpnKPS = "h3"

// Dial opens a kps connection to a pinned address over QUIC — the default
// transport for native clients (SPEC §5.4). The server's certificate is trusted
// iff it hashes to the address's certhash; no CA/hostname validation is done.
func Dial(ctx context.Context, addr string) (Conn, error) {
	a, err := ParseAddress(addr)
	if err != nil {
		return nil, err
	}
	digest, err := decodeCerthash(a.Certhash)
	if err != nil {
		return nil, err
	}
	tlsConf := &tls.Config{
		InsecureSkipVerify:    true, // trust is by certhash (below), not PKI
		NextProtos:            []string{alpnKPS},
		VerifyPeerCertificate: pinCerthash(digest),
	}
	qc, err := quic.DialAddr(ctx, fmt.Sprintf("%s:%d", a.IP, a.Port), tlsConf, &quic.Config{EnableDatagrams: true})
	if err != nil {
		return nil, fmt.Errorf("kps: quic dial: %w", err)
	}
	return newQUICConn(qc), nil
}

// pinCerthash returns a VerifyPeerCertificate that accepts the leaf certificate
// iff sha256(DER) equals the pinned digest (SPEC §3).
func pinCerthash(want []byte) func([][]byte, [][]*x509.Certificate) error {
	return func(rawCerts [][]byte, _ [][]*x509.Certificate) error {
		if len(rawCerts) == 0 {
			return errors.New("kps: server presented no certificate")
		}
		sum := sha256.Sum256(rawCerts[0])
		if subtle.ConstantTimeCompare(sum[:], want) != 1 {
			return errors.New("kps: server certhash mismatch")
		}
		return nil
	}
}

// quicConn is the QUIC implementation of Conn. QUIC's native bidirectional
// streams map directly onto kps streams with no extra framing (SPEC §6.3).
type quicConn struct {
	qc *quic.Conn
}

func newQUICConn(qc *quic.Conn) *quicConn { return &quicConn{qc: qc} }

func (c *quicConn) OpenStream(ctx context.Context) (Stream, error) {
	qs, err := c.qc.OpenStreamSync(ctx)
	if err != nil {
		return nil, err
	}
	return &quicStream{qs: qs}, nil
}

func (c *quicConn) AcceptStream(ctx context.Context) (Stream, error) {
	qs, err := c.qc.AcceptStream(ctx)
	if err != nil {
		return nil, err
	}
	return &quicStream{qs: qs}, nil
}

func (c *quicConn) Close() error            { return c.qc.CloseWithError(0, "") }
func (c *quicConn) Closed() <-chan struct{} { return c.qc.Context().Done() }

func (c *quicConn) SendDatagram(p []byte) error {
	err := c.qc.SendDatagram(p) // QUIC DATAGRAM (RFC 9221)
	var tooLarge *quic.DatagramTooLargeError
	if errors.As(err, &tooLarge) {
		return &DatagramTooLargeError{MaxDatagramPayloadSize: int(tooLarge.MaxDatagramPayloadSize)}
	}
	return err
}

func (c *quicConn) ReceiveDatagram(ctx context.Context) ([]byte, error) {
	return c.qc.ReceiveDatagram(ctx)
}

// quicStream maps kps stream lifecycle onto QUIC's native mechanisms (SPEC §6.3):
// CloseWrite→FIN, ResetWrite→RESET_STREAM, CancelRead→STOP_SENDING.
type quicStream struct {
	qs          *quic.Stream
	writeClosed atomic.Bool
}

func (s *quicStream) Read(p []byte) (int, error)  { return s.qs.Read(p) }
func (s *quicStream) Write(p []byte) (int, error) { return s.qs.Write(p) }

func (s *quicStream) CloseWrite() error {
	if s.writeClosed.Swap(true) {
		return nil
	}
	return s.qs.Close() // closes the send direction (FIN)
}

func (s *quicStream) ResetWrite(code ErrorCode) error {
	if s.writeClosed.Swap(true) {
		return nil
	}
	s.qs.CancelWrite(quic.StreamErrorCode(code))
	return nil
}

func (s *quicStream) CancelRead(code ErrorCode) error {
	s.qs.CancelRead(quic.StreamErrorCode(code))
	return nil
}

func (s *quicStream) Close() error {
	s.qs.CancelRead(quic.StreamErrorCode(CodeClosed))
	return s.CloseWrite()
}
