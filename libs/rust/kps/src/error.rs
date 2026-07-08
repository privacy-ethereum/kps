//! KPS error types: the canonical error-code registry (SPEC §9.1) and the
//! errors surfaced by connections and streams.

use std::fmt;

/// ErrorCode is the application-level reset/cancel/close code carried in
/// RESET / STOP_SENDING frames and CONNECTION_CLOSE. The values are the
/// canonical registry from SPEC §9.1, identical across implementations; an
/// unknown received code maps to `InternalError`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u32)]
pub enum ErrorCode {
    None = 0,
    Cancelled = 1,
    Closed = 2,
    Reset = 3,
    Timeout = 4,
    NetworkError = 5,
    ProtocolError = 6,
    Unsupported = 7,
    TooLarge = 8,
    QueueFull = 9,
    PermissionDenied = 10,
    InternalError = 11,
}

impl ErrorCode {
    /// Maps a wire `u32` to a code; unknown values sink to `InternalError`
    /// (SPEC §9.1), except `0` which is `None`.
    pub fn from_wire(v: u32) -> Self {
        match v {
            0 => Self::None,
            1 => Self::Cancelled,
            2 => Self::Closed,
            3 => Self::Reset,
            4 => Self::Timeout,
            5 => Self::NetworkError,
            6 => Self::ProtocolError,
            7 => Self::Unsupported,
            8 => Self::TooLarge,
            9 => Self::QueueFull,
            10 => Self::PermissionDenied,
            _ => Self::InternalError,
        }
    }

    /// The wire `u32` for this code.
    pub fn to_wire(self) -> u32 {
        self as u32
    }
}

impl fmt::Display for ErrorCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            Self::None => "none",
            Self::Cancelled => "cancelled",
            Self::Closed => "closed",
            Self::Reset => "reset",
            Self::Timeout => "timeout",
            Self::NetworkError => "network-error",
            Self::ProtocolError => "protocol-error",
            Self::Unsupported => "unsupported",
            Self::TooLarge => "too-large",
            Self::QueueFull => "queue-full",
            Self::PermissionDenied => "permission-denied",
            Self::InternalError => "internal-error",
        };
        f.write_str(s)
    }
}

/// StreamError is the error surfaced to the read side when the peer aborts its
/// write half (RESET), to the write side when the peer cancels its read
/// (STOP_SENDING), or as a connection's close reason (CONNECTION_CLOSE).
/// Mirrors Go's `*kps.StreamError`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("kps: stream reset (code {})", .code.to_wire())]
pub struct StreamError {
    pub code: ErrorCode,
    /// True when the code originated from the peer.
    pub remote: bool,
}

/// Returned by `send_datagram` when the payload exceeds the connection's
/// current datagram size limit (SPEC §7). The limit is transport- and
/// path-dependent; this error reports it, mirroring QUIC. As a rule of thumb,
/// payloads up to ~1100 bytes are safe on every connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("kps: datagram exceeds limit (max {max_datagram_payload_size} bytes)")]
pub struct DatagramTooLargeError {
    pub max_datagram_payload_size: usize,
}

/// The kps library error type.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("kps: {0}")]
    Address(String),
    #[error("kps: {0}")]
    Identity(String),
    #[error("kps: dial: {0}")]
    Dial(String),
    #[error("kps: connection closed")]
    ConnClosed,
    #[error("kps: stream closed")]
    StreamClosed,
    #[error("kps: write half closed")]
    WriteClosed,
    #[error(transparent)]
    Stream(#[from] StreamError),
    #[error(transparent)]
    DatagramTooLarge(#[from] DatagramTooLargeError),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error("kps: {0}")]
    Transport(String),
}

pub type Result<T> = std::result::Result<T, Error>;
