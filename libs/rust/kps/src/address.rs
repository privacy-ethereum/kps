//! KPS address parsing (SPEC §2): `<ip>:<port>:<certhash>` with IPv6 hosts
//! bracketed, and certhash decoding (SPEC §3).

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;

use crate::error::{Error, Result};

/// A parsed kps address: a UDP endpoint plus a pinned certhash (SPEC §2).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Address {
    pub ip: String,
    pub port: u16,
    pub certhash: String,
}

/// Parses `<ip>:<port>:<certhash>`. IPv6 hosts are bracketed,
/// `[<ipv6>]:<port>:<certhash>`, because the literal itself contains colons.
pub fn parse_address(s: &str) -> Result<Address> {
    let malformed = || {
        Error::Address(format!(
            "malformed address {s:?} (want ip:port:certhash or [ipv6]:port:certhash)"
        ))
    };

    let (host, rest) = if let Some(stripped) = s.strip_prefix('[') {
        let end = stripped.find(']').ok_or_else(malformed)?;
        let host = &stripped[..end];
        let rest = stripped[end + 1..].strip_prefix(':').ok_or_else(malformed)?;
        (host, rest)
    } else {
        s.split_once(':').ok_or_else(malformed)?
    };

    // rest is "<port>:<certhash>"; the certhash never contains ':'.
    let (port_str, certhash) = rest.split_once(':').ok_or_else(malformed)?;
    let port: u16 = port_str
        .parse()
        .ok()
        .filter(|p| *p >= 1)
        .ok_or_else(|| Error::Address(format!("bad port in address {s:?}")))?;
    if host.is_empty() || certhash.is_empty() {
        return Err(malformed());
    }
    Ok(Address { ip: host.to_string(), port, certhash: certhash.to_string() })
}

/// Formats an address, bracketing IPv6 hosts.
pub fn format_address(ip: &str, port: u16, certhash: &str) -> String {
    if ip.contains(':') {
        format!("[{ip}]:{port}:{certhash}")
    } else {
        format!("{ip}:{port}:{certhash}")
    }
}

/// Returns the raw 32-byte sha-256 digest carried by a certhash: multibase 'u'
/// (base64url, no pad) over multihash `0x12 0x20 || digest` (SPEC §3).
pub fn decode_certhash(s: &str) -> Result<[u8; 32]> {
    let body = s
        .strip_prefix('u')
        .ok_or_else(|| Error::Address("certhash missing multibase 'u' prefix".into()))?;
    let raw = URL_SAFE_NO_PAD
        .decode(body)
        .map_err(|e| Error::Address(format!("certhash base64url: {e}")))?;
    if raw.len() != 34 || raw[0] != 0x12 || raw[1] != 0x20 {
        return Err(Error::Address("certhash is not a sha2-256 multihash".into()));
    }
    let mut digest = [0u8; 32];
    digest.copy_from_slice(&raw[2..]);
    Ok(digest)
}

/// Encodes a 32-byte sha-256 digest as a certhash (SPEC §3).
pub fn encode_certhash(digest: &[u8; 32]) -> String {
    let mut mh = Vec::with_capacity(34);
    mh.extend_from_slice(&[0x12, 0x20]);
    mh.extend_from_slice(digest);
    format!("u{}", URL_SAFE_NO_PAD.encode(mh))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_ipv4() {
        let a = parse_address("127.0.0.1:4433:uEiA0000").unwrap();
        assert_eq!(a, Address { ip: "127.0.0.1".into(), port: 4433, certhash: "uEiA0000".into() });
    }

    #[test]
    fn parse_ipv6_bracketed() {
        let a = parse_address("[2001:db8::1]:443:uEiAx").unwrap();
        assert_eq!(a.ip, "2001:db8::1");
        assert_eq!(a.port, 443);
        assert_eq!(a.certhash, "uEiAx");
    }

    #[test]
    fn reject_malformed() {
        for s in ["", "127.0.0.1", "127.0.0.1:443", "127.0.0.1:0:x", "127.0.0.1:70000:x",
                  "[::1]443:x", "[::1:443:x", ":443:x", "127.0.0.1:443:"] {
            assert!(parse_address(s).is_err(), "should reject {s:?}");
        }
    }

    #[test]
    fn format_round_trip() {
        for (ip, want) in [("10.0.0.1", "10.0.0.1:9:h"), ("::1", "[::1]:9:h")] {
            let s = format_address(ip, 9, "h");
            assert_eq!(s, want);
            let a = parse_address(&s).unwrap();
            assert_eq!(a.ip, ip);
        }
    }

    #[test]
    fn certhash_round_trip() {
        let digest = [0xABu8; 32];
        let ch = encode_certhash(&digest);
        assert!(ch.starts_with('u'));
        assert_eq!(decode_certhash(&ch).unwrap(), digest);
    }

    #[test]
    fn certhash_rejects_bad() {
        assert!(decode_certhash("xabc").is_err()); // wrong multibase
        assert!(decode_certhash("u").is_err());
        // valid base64url but wrong multihash prefix
        let bad = format!("u{}", URL_SAFE_NO_PAD.encode([0x13u8, 0x20].iter().chain([0u8; 32].iter()).copied().collect::<Vec<u8>>()));
        assert!(decode_certhash(&bad).is_err());
    }
}
