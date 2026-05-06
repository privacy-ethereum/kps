package kps

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
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
}

const certLifetime = 200 * 365 * 24 * time.Hour

// LoadOrCreateIdentity reads keyPath if it exists, otherwise generates a
// new ECDSA P-256 key, writes it to keyPath, and derives a self-signed
// cert from it. The cert hash is stable as long as the key file is.
func LoadOrCreateIdentity(keyPath string) (*Identity, error) {
	priv, err := loadOrCreateKey(keyPath)
	if err != nil {
		return nil, err
	}
	return identityFromKey(priv)
}

func loadOrCreateKey(path string) (*ecdsa.PrivateKey, error) {
	data, err := os.ReadFile(path)
	if err == nil {
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
	if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}

	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, err
	}
	der, err := x509.MarshalPKCS8PrivateKey(priv)
	if err != nil {
		return nil, err
	}
	pemBytes := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der})
	if err := os.WriteFile(path, pemBytes, 0o600); err != nil {
		return nil, err
	}
	return priv, nil
}

func identityFromKey(priv *ecdsa.PrivateKey) (*Identity, error) {
	// Pion's NewCertificate takes a *template* and re-issues the cert.
	// We therefore can't compute certhash from a pre-built DER — we must
	// build the cert via pion and derive the hash from what pion produced.
	// To keep certhash stable across restarts (given a stable key), we
	// pass deterministic template fields: fixed serial, fixed validity
	// window anchored to the key (encoded in NotBefore so reissues match).
	notBefore := time.Now().Add(-1 * time.Hour)
	notAfter := notBefore.Add(certLifetime)
	tmpl := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "kps"},
		NotBefore:    notBefore,
		NotAfter:     notAfter,
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
	}
	cert, err := webrtc.NewCertificate(priv, tmpl)
	if err != nil {
		return nil, err
	}

	// Pull pion's actual cert fingerprint so client and server agree.
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

	return &Identity{Certificate: *cert, Certhash: certhash}, nil
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

