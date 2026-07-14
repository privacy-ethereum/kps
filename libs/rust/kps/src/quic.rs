//! The QUIC transport (SPEC §5.3): one KPS connection = one QUIC connection;
//! each KPS stream is one QUIC bidirectional stream with no extra framing
//! (SPEC §6.3); KPS datagrams are QUIC DATAGRAM frames (RFC 9221).

use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use async_trait::async_trait;
use bytes::Bytes;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

use crate::address::{decode_certhash, parse_address};
use crate::api::{Conn, Stream};
use crate::cert::Identity;
use crate::error::{DatagramTooLargeError, Error, ErrorCode, Result, StreamError};

/// The ALPN is intentionally non-identifying (SECURITY.md §3): KPS-over-QUIC
/// advertises the common HTTP/3 ALPN so a passive observer cannot keyword-match
/// KPS on the (publicly decryptable) Initial. KPS version negotiation does not
/// live in the ALPN — it belongs in the address or the first application bytes.
pub(crate) const ALPN_KPS: &[u8] = b"h3";

/// Opens a kps connection to a pinned address over QUIC — the default
/// transport for native clients (SPEC §5.4). The server's certificate is
/// trusted iff it hashes to the address's certhash; no CA/hostname validation
/// is done. Apply timeouts caller-side with `tokio::time::timeout`.
pub async fn dial(addr: &str) -> Result<Box<dyn Conn>> {
    let a = parse_address(addr)?;
    let digest = decode_certhash(&a.certhash)?;

    let mut tls = rustls::ClientConfig::builder()
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(PinVerifier::new(digest)))
        .with_no_client_auth();
    tls.alpn_protocols = vec![ALPN_KPS.to_vec()];

    let crypto = quinn::crypto::rustls::QuicClientConfig::try_from(tls)
        .map_err(|e| Error::Dial(format!("tls config: {e}")))?;
    let client_config = quinn::ClientConfig::new(Arc::new(crypto));

    let remote: std::net::SocketAddr = format!(
        "{}:{}",
        if a.ip.contains(':') { format!("[{}]", a.ip) } else { a.ip.clone() },
        a.port
    )
    .parse()
    .map_err(|e| Error::Address(format!("bad ip in address: {e}")))?;

    let bind: std::net::SocketAddr =
        if remote.is_ipv6() { "[::]:0".parse().unwrap() } else { "0.0.0.0:0".parse().unwrap() };
    let mut endpoint =
        quinn::Endpoint::client(bind).map_err(|e| Error::Dial(format!("bind: {e}")))?;
    endpoint.set_default_client_config(client_config);

    // The server name is required by the API but carries no trust (the pin
    // verifier ignores it). "localhost" is innocuous on the wire; a real
    // domain would be an identifying token (SPEC §5.3).
    let conn = endpoint
        .connect(remote, "localhost")
        .map_err(|e| Error::Dial(format!("{e}")))?
        .await
        .map_err(|e| Error::Dial(format!("{e}")))?;

    Ok(Box::new(QuicConn::new(conn, Some(endpoint))))
}

/// Certhash pinning (SPEC §3): accept the presented leaf certificate iff
/// sha256(DER) equals the pinned digest. No CA / hostname validation.
#[derive(Debug)]
struct PinVerifier {
    digest: [u8; 32],
    provider: Arc<rustls::crypto::CryptoProvider>,
}

impl PinVerifier {
    fn new(digest: [u8; 32]) -> Self {
        Self { digest, provider: Arc::new(rustls::crypto::ring::default_provider()) }
    }
}

impl rustls::client::danger::ServerCertVerifier for PinVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &rustls_pki_types::CertificateDer<'_>,
        _intermediates: &[rustls_pki_types::CertificateDer<'_>],
        _server_name: &rustls_pki_types::ServerName<'_>,
        _ocsp: &[u8],
        _now: rustls_pki_types::UnixTime,
    ) -> std::result::Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        use sha2::Digest;
        let sum: [u8; 32] = sha2::Sha256::digest(end_entity.as_ref()).into();
        // Constant-time compare (the digest is not secret, but stay in habit).
        let mut diff = 0u8;
        for (a, b) in sum.iter().zip(self.digest.iter()) {
            diff |= a ^ b;
        }
        if diff != 0 {
            return Err(rustls::Error::InvalidCertificate(
                rustls::CertificateError::ApplicationVerificationFailure,
            ));
        }
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &rustls_pki_types::CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> std::result::Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &rustls_pki_types::CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> std::result::Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        self.provider.signature_verification_algorithms.supported_schemes()
    }
}

/// Builds the quinn server config presenting the identity certificate with the
/// non-identifying ALPN and datagrams enabled. Shared by the listener.
pub(crate) fn server_config(identity: &Identity) -> Result<quinn::ServerConfig> {
    let (certs, key) = identity.rustls_parts()?;
    let mut tls = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(|e| Error::Identity(format!("tls cert: {e}")))?;
    tls.alpn_protocols = vec![ALPN_KPS.to_vec()];
    let crypto = quinn::crypto::rustls::QuicServerConfig::try_from(tls)
        .map_err(|e| Error::Identity(format!("quic tls: {e}")))?;
    Ok(quinn::ServerConfig::with_crypto(Arc::new(crypto)))
}

/// The QUIC implementation of [`Conn`].
pub(crate) struct QuicConn {
    conn: quinn::Connection,
    /// Client-side: owns the dialing endpoint so its socket closes with the
    /// connection. `None` on the server (the listener owns the endpoint).
    _endpoint: Option<quinn::Endpoint>,
}

impl QuicConn {
    pub(crate) fn new(conn: quinn::Connection, endpoint: Option<quinn::Endpoint>) -> Self {
        Self { conn, _endpoint: endpoint }
    }
}

#[async_trait]
impl Conn for QuicConn {
    async fn open_stream(&self) -> Result<Box<dyn Stream>> {
        let (send, recv) = self.conn.open_bi().await.map_err(conn_err)?;
        Ok(Box::new(QuicStream::new(send, recv)))
    }

    async fn accept_stream(&self) -> Result<Box<dyn Stream>> {
        let (send, recv) = self.conn.accept_bi().await.map_err(conn_err)?;
        Ok(Box::new(QuicStream::new(send, recv)))
    }

    async fn close(&self) -> Result<()> {
        self.conn.close(quinn::VarInt::from_u32(0), b"");
        Ok(())
    }

    async fn close_with_error(&self, code: ErrorCode) -> Result<()> {
        self.conn.close(quinn::VarInt::from_u32(code.to_wire()), b"");
        Ok(())
    }

    async fn closed(&self) {
        let _ = self.conn.closed().await;
    }

    fn err(&self) -> Option<Error> {
        normalize_close(self.conn.close_reason()?)
    }

    fn remote_addr(&self) -> std::net::SocketAddr {
        self.conn.remote_address()
    }

    async fn send_datagram(&self, p: &[u8]) -> Result<()> {
        match self.conn.send_datagram(Bytes::copy_from_slice(p)) {
            Ok(()) => Ok(()),
            Err(quinn::SendDatagramError::TooLarge) => {
                Err(DatagramTooLargeError {
                    max_datagram_payload_size: self.conn.max_datagram_size().unwrap_or(0),
                }
                .into())
            }
            Err(quinn::SendDatagramError::UnsupportedByPeer)
            | Err(quinn::SendDatagramError::Disabled) => {
                Err(Error::Transport("datagrams unavailable".into()))
            }
            Err(quinn::SendDatagramError::ConnectionLost(e)) => Err(conn_err(e)),
        }
    }

    async fn receive_datagram(&self) -> Result<Vec<u8>> {
        // quinn buffers inbound datagrams in a bounded queue, dropping the
        // oldest when full — exactly the SPEC §7 receive model.
        let d = self.conn.read_datagram().await.map_err(conn_err)?;
        Ok(d.to_vec())
    }
}

/// Normalizes a quinn close reason to the kps surface: an application close
/// with a non-zero code becomes a [`StreamError`] (same type on both
/// transports); a clean/local close is `None`.
fn normalize_close(e: quinn::ConnectionError) -> Option<Error> {
    match e {
        quinn::ConnectionError::ApplicationClosed(app) => {
            let raw = u64::from(app.error_code) as u32;
            if raw == 0 {
                None
            } else {
                Some(StreamError { code: ErrorCode::from_wire(raw), remote: true }.into())
            }
        }
        quinn::ConnectionError::LocallyClosed => None,
        other => Some(Error::Transport(other.to_string())),
    }
}

fn conn_err(e: quinn::ConnectionError) -> Error {
    normalize_close(e).unwrap_or(Error::ConnClosed)
}

/// The QUIC implementation of [`Stream`] (SPEC §6.3): CloseWrite→FIN,
/// ResetWrite→RESET_STREAM, CancelRead→STOP_SENDING, all native.
pub(crate) struct QuicStream {
    send: quinn::SendStream,
    recv: quinn::RecvStream,
    write_closed: bool,
    state: Arc<Mutex<StreamState>>,
    closed_notify: Arc<tokio::sync::Notify>,
}

#[derive(Default)]
struct StreamState {
    err: Option<StreamError>,
    closed: bool,
}

impl QuicStream {
    fn new(send: quinn::SendStream, recv: quinn::RecvStream) -> Self {
        Self {
            send,
            recv,
            write_closed: false,
            state: Arc::new(Mutex::new(StreamState::default())),
            closed_notify: Arc::new(tokio::sync::Notify::new()),
        }
    }

    fn record_err(&self, err: StreamError) {
        let mut st = self.state.lock().unwrap();
        if st.err.is_none() {
            st.err = Some(err);
        }
        if !st.closed {
            st.closed = true;
            self.closed_notify.notify_waiters();
        }
    }

    fn mark_closed(&self) {
        let mut st = self.state.lock().unwrap();
        if !st.closed {
            st.closed = true;
            self.closed_notify.notify_waiters();
        }
    }

    /// Inspects an I/O error surfaced by quinn's AsyncRead/AsyncWrite impls
    /// and records a peer reset/stop code when one is carried.
    fn note_io_error(&self, e: &std::io::Error) {
        if let Some(inner) = e.get_ref() {
            if let Some(quinn::ReadError::Reset(code)) = inner.downcast_ref::<quinn::ReadError>() {
                let raw = u64::from(*code) as u32;
                self.record_err(StreamError { code: ErrorCode::from_wire(raw), remote: true });
            } else if let Some(we) = inner.downcast_ref::<quinn::WriteError>() {
                if let quinn::WriteError::Stopped(code) = we {
                    let raw = u64::from(*code) as u32;
                    self.record_err(StreamError { code: ErrorCode::from_wire(raw), remote: true });
                }
            }
        }
    }
}

impl AsyncRead for QuicStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let me = &mut *self;
        match Pin::new(&mut me.recv).poll_read(cx, buf) {
            Poll::Ready(Err(e)) => {
                me.note_io_error(&e);
                Poll::Ready(Err(e))
            }
            other => other,
        }
    }
}

impl AsyncWrite for QuicStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        let me = &mut *self;
        // NB: quinn's inherent SendStream::poll_write (WriteError) shadows the
        // trait method; call the tokio trait impl explicitly.
        match AsyncWrite::poll_write(Pin::new(&mut me.send), cx, buf) {
            Poll::Ready(Err(e)) => {
                me.note_io_error(&e);
                Poll::Ready(Err(e))
            }
            other => other,
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        AsyncWrite::poll_flush(Pin::new(&mut self.send), cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        // AsyncWrite::shutdown maps to a graceful FIN (close_write).
        self.write_closed = true;
        AsyncWrite::poll_shutdown(Pin::new(&mut self.send), cx)
    }
}

#[async_trait]
impl Stream for QuicStream {
    async fn close_write(&mut self) -> Result<()> {
        if self.write_closed {
            return Ok(());
        }
        self.write_closed = true;
        let _ = self.send.finish(); // FIN; already-closed is a no-op
        Ok(())
    }

    async fn cancel_read(&mut self, code: ErrorCode) -> Result<()> {
        let _ = self.recv.stop(quinn::VarInt::from_u32(code.to_wire()));
        Ok(())
    }

    async fn reset_write(&mut self, code: ErrorCode) -> Result<()> {
        if self.write_closed {
            return Ok(());
        }
        self.write_closed = true;
        let _ = self.send.reset(quinn::VarInt::from_u32(code.to_wire()));
        Ok(())
    }

    async fn close(&mut self) -> Result<()> {
        let _ = self.recv.stop(quinn::VarInt::from_u32(ErrorCode::Closed.to_wire()));
        let r = self.close_write().await;
        self.mark_closed();
        r
    }

    async fn close_with_error(&mut self, code: ErrorCode) -> Result<()> {
        let _ = self.recv.stop(quinn::VarInt::from_u32(code.to_wire()));
        if !self.write_closed {
            self.write_closed = true;
            let _ = self.send.reset(quinn::VarInt::from_u32(code.to_wire()));
        }
        self.mark_closed();
        Ok(())
    }

    async fn closed(&self) {
        loop {
            let notified = self.closed_notify.notified();
            if self.state.lock().unwrap().closed {
                return;
            }
            notified.await;
        }
    }

    fn err(&self) -> Option<StreamError> {
        self.state.lock().unwrap().err
    }
}
