//! The transport-neutral public API (SPEC §4, §6): `Conn` and `Stream` traits
//! implemented by both transports (WebRTC and QUIC); callers cannot tell which
//! backs a connection.
//!
//! Cancellation/timeouts are caller-side: wrap any call in
//! `tokio::time::timeout` (this mirrors the JS packages, where timeouts are
//! expressed through `AbortSignal.timeout`).

use async_trait::async_trait;
use tokio::io::{AsyncRead, AsyncWrite};

use crate::error::{Error, ErrorCode, Result, StreamError};

/// An authenticated, secure, multiplexed kps session (SPEC §4), carrying any
/// number of independent byte [`Stream`]s plus connection-level datagrams.
#[async_trait]
pub trait Conn: Send + Sync {
    /// Opens a new bidirectional byte stream, resolving when it is ready.
    async fn open_stream(&self) -> Result<Box<dyn Stream>>;

    /// Returns the next stream opened by the peer.
    async fn accept_stream(&self) -> Result<Box<dyn Stream>>;

    /// Tears down the connection and invalidates all its streams.
    async fn close(&self) -> Result<()>;

    /// Tears down the connection, conveying an application error code to the
    /// peer (QUIC CONNECTION_CLOSE; the WebRTC control channel's
    /// CONNECTION_CLOSE, SPEC §8). Delivery is best-effort.
    async fn close_with_error(&self, code: ErrorCode) -> Result<()>;

    /// Resolves when the connection ends.
    async fn closed(&self);

    /// Why the connection closed: `None` while open or after a clean close,
    /// `Some` otherwise. Best-effort — a reason is only available where the
    /// transport carries one.
    fn err(&self) -> Option<Error>;

    /// The peer's UDP endpoint (e.g. for per-IP policy such as rate limiting).
    /// It reflects the endpoint observed at connection establishment and MAY
    /// change over the connection's life (QUIC path migration, ICE
    /// renomination); on the dial side it is the dialed endpoint.
    fn remote_addr(&self) -> std::net::SocketAddr;

    /// Sends an unreliable, unordered, size-limited datagram (SPEC §7).
    /// An oversized payload fails with [`Error::DatagramTooLarge`] reporting
    /// the current limit. Delivery is best-effort.
    async fn send_datagram(&self, p: &[u8]) -> Result<()>;

    /// Receives the next inbound datagram (from a bounded drop-oldest buffer).
    async fn receive_datagram(&self) -> Result<Vec<u8>>;
}

/// An unnamed, bidirectional, reliable, ordered byte stream (SPEC §6) with no
/// message boundaries: [`AsyncRead`] + [`AsyncWrite`] with QUIC-like lifecycle
/// controls. Reads return `Ok(0)` (EOF) after the peer's write half finishes
/// gracefully, or an error carrying a [`StreamError`] if the peer reset.
#[async_trait]
pub trait Stream: AsyncRead + AsyncWrite + Send + Unpin {
    /// Gracefully finishes the local write half; the peer observes EOF after
    /// all previously written bytes.
    async fn close_write(&mut self) -> Result<()>;

    /// Stops inbound bytes (cancellation, not EOF); the peer is told to stop
    /// sending (STOP_SENDING).
    async fn cancel_read(&mut self, code: ErrorCode) -> Result<()>;

    /// Aborts the local write half; the peer observes a stream error (RESET).
    async fn reset_write(&mut self, code: ErrorCode) -> Result<()>;

    /// Tears down both halves of the stream (clean).
    async fn close(&mut self) -> Result<()>;

    /// Tears down both halves, conveying an error code to the peer
    /// (reset write + stop-sending read).
    async fn close_with_error(&mut self, code: ErrorCode) -> Result<()>;

    /// Resolves when the stream ends (either half torn down or peer gone).
    async fn closed(&self);

    /// Why the stream closed: `None` while open or after a clean close,
    /// `Some` otherwise (best-effort, transport-dependent).
    fn err(&self) -> Option<StreamError>;
}
