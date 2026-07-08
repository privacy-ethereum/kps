//! The server identity (SPEC §3): a persistent self-signed ECDSA P-256
//! certificate and its multibase-encoded sha-256 multihash certhash. The same
//! certificate is presented over WebRTC DTLS and QUIC TLS 1.3, so one certhash
//! pins both transports — the digest is over the certificate's DER bytes, the
//! same bytes a DTLS `a=fingerprint:sha-256` covers.

use std::path::Path;

use sha2::{Digest, Sha256};

use crate::address::encode_certhash;
use crate::error::{Error, Result};

/// ~200 years, matching the Go implementation. (Rationale and the residual
/// validity-period concern: see SECURITY.md.)
const CERT_LIFETIME_DAYS: i64 = 200 * 365;

/// Identity holds the server's persistent self-signed TLS cert + key and the
/// certhash that clients pin.
pub struct Identity {
    /// DER-encoded certificate — the exact bytes presented on both wires.
    pub(crate) cert_der: Vec<u8>,
    /// PKCS#8 DER private key.
    pub(crate) key_der: Vec<u8>,
    /// PKCS#8 PEM private key (kept for PEM round-trips and webrtc-rs).
    key_pem: String,
    /// Multibase 'u' + multihash sha-256 certhash (SPEC §3).
    pub certhash: String,
    /// Raw 32-byte sha-256 of the cert DER (the certhash payload).
    pub(crate) digest: [u8; 32],
}

impl Identity {
    /// Mints a fresh ECDSA P-256 key + self-signed cert. Pair with
    /// [`Identity::to_pem`] for serialization and [`Identity::from_pem`] to load.
    pub fn generate() -> Result<Self> {
        let key_pair = rcgen::KeyPair::generate_for(&rcgen::PKCS_ECDSA_P256_SHA256)
            .map_err(|e| Error::Identity(format!("generate key: {e}")))?;
        let cert_der = build_cert(&key_pair)?;
        Self::from_parts(cert_der, key_pair)
    }

    /// Parses the combined PEM produced by [`Identity::to_pem`] (PRIVATE KEY +
    /// CERTIFICATE blocks, any order) — the same on-disk format as the Go and
    /// JS implementations. The certificate DER is loaded verbatim, so the
    /// certhash is byte-stable across restarts.
    pub fn from_pem(pem_str: &str) -> Result<Self> {
        let mut cert_der: Option<Vec<u8>> = None;
        let mut key_pem: Option<String> = None;
        for block in pem::parse_many(pem_str)
            .map_err(|e| Error::Identity(format!("parse PEM: {e}")))?
        {
            match block.tag() {
                "CERTIFICATE" if cert_der.is_none() => cert_der = Some(block.contents().to_vec()),
                "PRIVATE KEY" | "EC PRIVATE KEY" if key_pem.is_none() => {
                    key_pem = Some(pem::encode(&block));
                }
                _ => {}
            }
        }
        let cert_der = cert_der.ok_or_else(|| Error::Identity("PEM has no CERTIFICATE block".into()))?;
        let key_pem = key_pem.ok_or_else(|| Error::Identity("PEM has no PRIVATE KEY block".into()))?;
        let key_pair = rcgen::KeyPair::from_pem(&key_pem)
            .map_err(|e| Error::Identity(format!("parse private key: {e}")))?;
        Self::from_parts(cert_der, key_pair)
    }

    /// The combined PRIVATE KEY + CERTIFICATE PEM. Round-trips through
    /// [`Identity::from_pem`] with the same certhash.
    pub fn to_pem(&self) -> String {
        let cert_pem = pem::encode(&pem::Pem::new("CERTIFICATE", self.cert_der.clone()));
        format!("{}{}", self.key_pem, cert_pem)
    }

    /// Reads `path` if it exists, otherwise generates a fresh identity and
    /// writes the combined PEM out (0600). The certhash is byte-stable across
    /// restarts. A legacy key-only file is accepted: a fresh cert is built from
    /// that key and the file is rewritten in the combined format.
    pub fn load_or_create(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        match std::fs::read_to_string(path) {
            Ok(data) => {
                if let Ok(id) = Self::from_pem(&data) {
                    return Ok(id);
                }
                // Legacy key-only file: build a cert from the key and rewrite.
                let key_pair = rcgen::KeyPair::from_pem(&data)
                    .map_err(|e| Error::Identity(format!("parse {}: {e}", path.display())))?;
                let cert_der = build_cert(&key_pair)?;
                let id = Self::from_parts(cert_der, key_pair)?;
                write_secret(path, id.to_pem().as_bytes())?;
                Ok(id)
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                let id = Self::generate()?;
                write_secret(path, id.to_pem().as_bytes())?;
                Ok(id)
            }
            Err(e) => Err(e.into()),
        }
    }

    fn from_parts(cert_der: Vec<u8>, key_pair: rcgen::KeyPair) -> Result<Self> {
        let digest: [u8; 32] = Sha256::digest(&cert_der).into();
        let certhash = encode_certhash(&digest);
        Ok(Identity {
            cert_der,
            key_der: key_pair.serialize_der(),
            key_pem: key_pair.serialize_pem(),
            certhash,
            digest,
        })
    }

    /// The identity as a webrtc-rs certificate for the DTLS side. Built from
    /// the stored DER verbatim so the wire fingerprint equals the certhash.
    pub(crate) fn rtc_certificate(&self) -> Result<webrtc::peer_connection::certificate::RTCCertificate> {
        let key_pair = rcgen::KeyPair::from_pem(&self.key_pem)
            .map_err(|e| Error::Identity(format!("key for DTLS: {e}")))?;
        let private_key = webrtc::dtls::crypto::CryptoPrivateKey::from_key_pair(&key_pair)
            .map_err(|e| Error::Identity(format!("key for DTLS: {e}")))?;
        let dtls_cert = webrtc::dtls::crypto::Certificate {
            certificate: vec![rustls_pki_types::CertificateDer::from(self.cert_der.clone())],
            private_key,
        };
        let expires = std::time::SystemTime::now()
            + std::time::Duration::from_secs(CERT_LIFETIME_DAYS as u64 * 24 * 3600);
        Ok(webrtc::peer_connection::certificate::RTCCertificate::from_existing(dtls_cert, expires))
    }

    /// The identity as rustls cert + key for the QUIC side. The same DER as the
    /// DTLS side, so a single certhash pins both transports (SPEC §3).
    pub(crate) fn rustls_parts(
        &self,
    ) -> Result<(Vec<rustls_pki_types::CertificateDer<'static>>, rustls_pki_types::PrivateKeyDer<'static>)>
    {
        let cert = rustls_pki_types::CertificateDer::from(self.cert_der.clone());
        let key = rustls_pki_types::PrivateKeyDer::try_from(self.key_der.clone())
            .map_err(|e| Error::Identity(format!("key for TLS: {e}")))?;
        Ok((vec![cert], key))
    }
}

/// Builds the self-signed certificate DER. The certificate is observable in
/// cleartext on the DTLS 1.2 wire, so it carries no KPS-identifying metadata:
/// a random serial and an empty Subject (SPEC §3, SECURITY.md §3).
fn build_cert(key_pair: &rcgen::KeyPair) -> Result<Vec<u8>> {
    let mut params = rcgen::CertificateParams::default();
    params.distinguished_name = rcgen::DistinguishedName::new(); // empty Subject
    let mut serial = [0u8; 16];
    rand::RngCore::fill_bytes(&mut rand::thread_rng(), &mut serial);
    serial[0] &= 0x7f; // keep the INTEGER positive
    params.serial_number = Some(rcgen::SerialNumber::from_slice(&serial));
    let now = time::OffsetDateTime::now_utc();
    params.not_before = now - time::Duration::hours(1);
    params.not_after = now + time::Duration::days(CERT_LIFETIME_DAYS);
    params.key_usages = vec![rcgen::KeyUsagePurpose::DigitalSignature];
    params.extended_key_usages = vec![
        rcgen::ExtendedKeyUsagePurpose::ServerAuth,
        rcgen::ExtendedKeyUsagePurpose::ClientAuth,
    ];
    let cert = params
        .self_signed(key_pair)
        .map_err(|e| Error::Identity(format!("self-sign: {e}")))?;
    Ok(cert.der().to_vec())
}

fn write_secret(path: &Path, data: &[u8]) -> Result<()> {
    use std::io::Write;
    let mut opts = std::fs::OpenOptions::new();
    opts.write(true).create(true).truncate(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        opts.mode(0o600);
    }
    let mut f = opts.open(path)?;
    f.write_all(data)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generate_and_round_trip() {
        let id = Identity::generate().unwrap();
        assert!(id.certhash.starts_with('u'));
        let pem = id.to_pem();
        let id2 = Identity::from_pem(&pem).unwrap();
        assert_eq!(id.certhash, id2.certhash, "certhash must be byte-stable across PEM round-trip");
        assert_eq!(id.cert_der, id2.cert_der);
    }

    #[test]
    fn certhash_is_sha256_of_der() {
        let id = Identity::generate().unwrap();
        let digest: [u8; 32] = Sha256::digest(&id.cert_der).into();
        assert_eq!(id.certhash, encode_certhash(&digest));
    }

    #[test]
    fn dtls_fingerprint_matches_certhash() {
        // The load-bearing SPEC §3 invariant: webrtc-rs's advertised sha-256
        // fingerprint is the sha256 of the same DER the certhash covers.
        let id = Identity::generate().unwrap();
        let rtc = id.rtc_certificate().unwrap();
        let fp = rtc
            .get_fingerprints()
            .into_iter()
            .find(|f| f.algorithm == "sha-256")
            .expect("sha-256 fingerprint");
        let fp_bytes = fp
            .value
            .split(':')
            .map(|h| u8::from_str_radix(h, 16).unwrap())
            .collect::<Vec<u8>>();
        assert_eq!(fp_bytes.as_slice(), &id.digest[..]);
    }

    #[test]
    fn load_or_create_persists() {
        let dir = std::env::temp_dir().join(format!("kps-cert-test-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("kps.pem");
        let a = Identity::load_or_create(&path).unwrap();
        let b = Identity::load_or_create(&path).unwrap();
        assert_eq!(a.certhash, b.certhash, "certhash stable across restarts");
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn subject_is_empty_and_serial_random() {
        // Two fresh identities must not share a serial, and the cert should not
        // contain a recognizable CN (SPEC §3: no identifying metadata).
        let a = Identity::generate().unwrap();
        let b = Identity::generate().unwrap();
        assert_ne!(a.cert_der, b.cert_der);
        // crude check: DER should not contain the bytes "kps"
        assert!(!a.cert_der.windows(3).any(|w| w == b"kps"));
    }
}
