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

// ParseAddress parses "<ip>:<port>:<certhash>". IPv6 hosts are bracketed,
// "[<ipv6>]:<port>:<certhash>", because the literal itself contains colons.
func ParseAddress(s string) (Address, error) {
	malformed := func() (Address, error) {
		return Address{}, fmt.Errorf("kps: malformed address %q (want ip:port:certhash or [ipv6]:port:certhash)", s)
	}

	var host, rest string
	if strings.HasPrefix(s, "[") {
		end := strings.Index(s, "]")
		if end < 0 || end+1 >= len(s) || s[end+1] != ':' {
			return malformed()
		}
		host = s[1:end]
		rest = s[end+2:]
	} else {
		i := strings.IndexByte(s, ':')
		if i < 0 {
			return malformed()
		}
		host = s[:i]
		rest = s[i+1:]
	}

	// rest is "<port>:<certhash>"; the certhash never contains ':'.
	j := strings.IndexByte(rest, ':')
	if j < 0 {
		return malformed()
	}
	port, err := strconv.Atoi(rest[:j])
	if err != nil || port < 1 || port > 65535 {
		return Address{}, fmt.Errorf("kps: bad port in address %q", s)
	}
	certhash := rest[j+1:]
	if host == "" || certhash == "" {
		return malformed()
	}
	return Address{IP: host, Port: port, Certhash: certhash}, nil
}

// joinHostPortCerthash formats an address, bracketing IPv6 hosts.
func joinHostPortCerthash(ip string, port int, certhash string) string {
	if strings.Contains(ip, ":") {
		return fmt.Sprintf("[%s]:%d:%s", ip, port, certhash)
	}
	return fmt.Sprintf("%s:%d:%s", ip, port, certhash)
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
