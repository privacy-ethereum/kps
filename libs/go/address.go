package kps

import (
	"encoding/base64"
	"fmt"
	"strconv"
	"strings"
)

// Address is a parsed kps address: a UDP endpoint plus a pinned certhash
// (SPEC §2). The form is "<ip>:<port>:<certhash>".
type Address struct {
	IP       string
	Port     int
	Certhash string
}

// ParseAddress parses "<ip>:<port>:<certhash>".
func ParseAddress(s string) (Address, error) {
	parts := strings.SplitN(s, ":", 3)
	if len(parts) != 3 {
		return Address{}, fmt.Errorf("kps: malformed address %q (want ip:port:certhash)", s)
	}
	port, err := strconv.Atoi(parts[1])
	if err != nil || port < 1 || port > 65535 {
		return Address{}, fmt.Errorf("kps: bad port in address %q", s)
	}
	if parts[0] == "" || parts[2] == "" {
		return Address{}, fmt.Errorf("kps: malformed address %q", s)
	}
	return Address{IP: parts[0], Port: port, Certhash: parts[2]}, nil
}

// decodeCerthash returns the raw 32-byte sha-256 digest carried by a certhash:
// multibase 'u' (base64url, no pad) over multihash 0x12 0x20 || digest (SPEC §3).
func decodeCerthash(s string) ([]byte, error) {
	if len(s) == 0 || s[0] != 'u' {
		return nil, fmt.Errorf("kps: certhash missing multibase 'u' prefix")
	}
	raw, err := base64.RawURLEncoding.DecodeString(s[1:])
	if err != nil {
		return nil, fmt.Errorf("kps: certhash base64url: %w", err)
	}
	if len(raw) != 34 || raw[0] != 0x12 || raw[1] != 0x20 {
		return nil, fmt.Errorf("kps: certhash is not a sha2-256 multihash")
	}
	return raw[2:], nil
}
