//! ICE credential derivation (SPEC §5.2).

use base64::engine::general_purpose::STANDARD_NO_PAD;
use base64::Engine;
use hmac::{Hmac, Mac};
use sha2::Sha256;

/// Computes the ICE password from the pinned certhash digest and the
/// connection's ufrag (SPEC §5.2). Client and server compute it identically;
/// it is only ever a MESSAGE-INTEGRITY key and is never transmitted. This
/// replaces the libp2p `ufrag == pwd` convention — removing the recomputable
/// fingerprint and gating DTLS behind certhash possession (probe resistance).
pub(crate) fn derive_ice_pwd(certhash_digest: &[u8], ufrag: &str) -> String {
    let mut mac = Hmac::<Sha256>::new_from_slice(certhash_digest)
        .expect("HMAC accepts any key length");
    mac.update(b"kps-ice-pwd-v1:");
    mac.update(ufrag.as_bytes());
    STANDARD_NO_PAD.encode(mac.finalize().into_bytes())
}

/// A random ICE ufrag (64 bits, SPEC §5.2), hex-encoded. Hex keeps it within
/// the RFC 8839 ice-char set (ALPHA / DIGIT); base64url's '-'/'_' are NOT valid
/// ice-chars and are rejected by strict stacks like libdatachannel. Matches the
/// Go and JS implementations.
pub(crate) fn rand_ufrag() -> String {
    let mut b = [0u8; 8];
    rand::RngCore::fill_bytes(&mut rand::thread_rng(), &mut b);
    hex::encode(b)
}

#[cfg(test)]
mod tests {
    use super::*;

    // Known vector shared with the Go/JS test suites: any change to the
    // derivation breaks cross-implementation ICE, so pin it exactly.
    #[test]
    fn derive_matches_reference() {
        let digest = [0x11u8; 32];
        let pwd = derive_ice_pwd(&digest, "abcd1234");
        // Computed with the Go implementation (libs/go/ice.go):
        // deriveICEPwd(0x11 * 32, "abcd1234")
        assert_eq!(pwd, "RRs7qx3BRfTDtuvZU0gi+pWO23X9mRHwsjVL+Q3WnlM");
        // Sensitive to both inputs
        assert_ne!(pwd, derive_ice_pwd(&digest, "abcd1235"));
        assert_ne!(pwd, derive_ice_pwd(&[0x12u8; 32], "abcd1234"));
    }

    #[test]
    fn ufrag_is_ice_chars() {
        let u = rand_ufrag();
        assert_eq!(u.len(), 16); // 8 bytes hex
        assert!(u.chars().all(|c| c.is_ascii_hexdigit()));
    }
}
