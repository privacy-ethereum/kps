package kps

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"os"
	"time"

	"github.com/pion/webrtc/v4"
)

// Identity holds the server's persistent self-signed TLS cert and the
// matching multibase-encoded sha-256 multihash certhash that clients pin.
type Identity struct {
	Certificate webrtc.Certificate
	Certhash    string // multibase 'u' + multihash sha256
	digest      []byte // raw 32-byte sha-256 of the cert DER (certhash payload)
}

const certLifetime = 200 * 365 * 24 * time.Hour

// GenerateIdentity mints a fresh ECDSA P-256 key + self-signed cert.
// Use this when you want to manage the on-disk format yourself; pair
// with (*Identity).PEM() for serialization and IdentityFromPEM for load.
func GenerateIdentity() (*Identity, error) {
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, err
	}
	cert, err := buildCert(priv)
	if err != nil {
		return nil, err
	}
	return identityFromCert(cert)
}

// IdentityFromPEM parses the combined PEM produced by (*Identity).PEM().
func IdentityFromPEM(pemStr string) (*Identity, error) {
	cert, err := webrtc.CertificateFromPEM(pemStr)
	if err != nil {
		return nil, err
	}
	return identityFromCert(cert)
}

// PEM returns the combined PRIVATE KEY + CERTIFICATE PEM. Round-trips
// through IdentityFromPEM with the same certhash.
func (i *Identity) PEM() (string, error) {
	return i.Certificate.PEM()
}

// LoadOrCreateIdentity reads keyPath if it exists, otherwise generates a
// new ECDSA P-256 key + self-signed cert and writes them out together.
//
// The file holds both the PRIVATE KEY and CERTIFICATE PEM blocks, so the
// certhash is byte-stable across restarts: the cert is built once and
// then loaded verbatim on subsequent starts.
//
// For backwards compatibility, a file containing only a PRIVATE KEY block
// (the previous on-disk format) is accepted: a fresh cert is built from
// that key and the file is rewritten in the combined format. The cert
// hash will change at the migration boundary, but stay stable thereafter.
func LoadOrCreateIdentity(keyPath string) (*Identity, error) {
	data, err := os.ReadFile(keyPath)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	if err == nil {
		if cert, ok := tryLoadCombinedPEM(data); ok {
			return identityFromCert(cert)
		}
		// Legacy key-only file: build cert from key and rewrite combined PEM.
		priv, err := parseKeyPEM(data, keyPath)
		if err != nil {
			return nil, err
		}
		cert, err := buildCert(priv)
		if err != nil {
			return nil, err
		}
		if err := writeCombinedPEM(keyPath, cert); err != nil {
			return nil, fmt.Errorf("kps: rewrite %s with cert: %w", keyPath, err)
		}
		return identityFromCert(cert)
	}

	// File doesn't exist — generate fresh key + cert and persist together.
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, err
	}
	cert, err := buildCert(priv)
	if err != nil {
		return nil, err
	}
	if err := writeCombinedPEM(keyPath, cert); err != nil {
		return nil, err
	}
	return identityFromCert(cert)
}

func tryLoadCombinedPEM(data []byte) (*webrtc.Certificate, bool) {
	rest := data
	hasCert := false
	hasKey := false
	for {
		var block *pem.Block
		block, rest = pem.Decode(rest)
		if block == nil {
			break
		}
		switch block.Type {
		case "CERTIFICATE":
			hasCert = true
		case "PRIVATE KEY", "EC PRIVATE KEY":
			hasKey = true
		}
	}
	if !hasCert || !hasKey {
		return nil, false
	}
	cert, err := webrtc.CertificateFromPEM(string(data))
	if err != nil {
		return nil, false
	}
	return cert, true
}

func parseKeyPEM(data []byte, path string) (*ecdsa.PrivateKey, error) {
	block, _ := pem.Decode(data)
	if block == nil {
		return nil, fmt.Errorf("kps: %s does not contain PEM data", path)
	}
	key, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("kps: parse %s: %w", path, err)
	}
	ec, ok := key.(*ecdsa.PrivateKey)
	if !ok {
		return nil, fmt.Errorf("kps: %s is not an ECDSA key", path)
	}
	return ec, nil
}

func buildCert(priv *ecdsa.PrivateKey) (*webrtc.Certificate, error) {
	notBefore := time.Now().Add(-1 * time.Hour)
	notAfter := notBefore.Add(certLifetime)
	// The certificate is observable in cleartext on the DTLS 1.2 wire, so it
	// carries no KPS-identifying metadata: a random serial and an empty Subject
	// (SPEC §3, SECURITY.md §3) — not a fixed serial of 1 and CN "kps".
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return nil, err
	}
	tmpl := x509.Certificate{
		SerialNumber: serial,
		NotBefore:    notBefore,
		NotAfter:     notAfter,
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
	}
	return webrtc.NewCertificate(priv, tmpl)
}

// tlsCertificate returns the identity as a crypto/tls certificate for the QUIC
// transport. It is the same self-signed cert presented over DTLS, so a single
// certhash pins both transports (SPEC §3).
func (i *Identity) tlsCertificate() (tls.Certificate, error) {
	pemStr, err := i.Certificate.PEM()
	if err != nil {
		return tls.Certificate{}, err
	}
	return tls.X509KeyPair([]byte(pemStr), []byte(pemStr))
}

func writeCombinedPEM(path string, cert *webrtc.Certificate) error {
	pemStr, err := cert.PEM()
	if err != nil {
		return err
	}
	return os.WriteFile(path, []byte(pemStr), 0o600)
}

func identityFromCert(cert *webrtc.Certificate) (*Identity, error) {
	prints, err := cert.GetFingerprints()
	if err != nil {
		return nil, err
	}
	var sha256Hex string
	for _, p := range prints {
		if p.Algorithm == "sha-256" {
			sha256Hex = p.Value
			break
		}
	}
	if sha256Hex == "" {
		return nil, errors.New("kps: pion produced no sha-256 fingerprint")
	}
	digest, err := hexColonsToBytes(sha256Hex)
	if err != nil {
		return nil, fmt.Errorf("kps: parse pion fingerprint: %w", err)
	}

	mh := append([]byte{0x12, 0x20}, digest...)
	certhash := "u" + base64.RawURLEncoding.EncodeToString(mh)

	return &Identity{Certificate: *cert, Certhash: certhash, digest: digest}, nil
}

func hexColonsToBytes(s string) ([]byte, error) {
	out := make([]byte, 0, 32)
	for _, part := range splitColons(s) {
		if len(part) != 2 {
			return nil, fmt.Errorf("bad hex byte %q", part)
		}
		var b byte
		for _, c := range part {
			b <<= 4
			switch {
			case c >= '0' && c <= '9':
				b |= byte(c - '0')
			case c >= 'a' && c <= 'f':
				b |= byte(c-'a') + 10
			case c >= 'A' && c <= 'F':
				b |= byte(c-'A') + 10
			default:
				return nil, fmt.Errorf("bad hex char %q", c)
			}
		}
		out = append(out, b)
	}
	return out, nil
}

func splitColons(s string) []string {
	var parts []string
	start := 0
	for i := 0; i < len(s); i++ {
		if s[i] == ':' {
			parts = append(parts, s[start:i])
			start = i + 1
		}
	}
	parts = append(parts, s[start:])
	return parts
}
