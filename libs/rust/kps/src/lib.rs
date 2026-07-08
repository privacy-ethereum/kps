//! KPS — Key Pinned Streams (Rust implementation of `SPEC.md`).
//!
//! A KPS endpoint is identified by a pinned self-signed certificate, not a
//! CA-signed domain name. The address you dial is `<ip>:<port>:<certhash>`.
//! KPS provides an authenticated, encrypted, multiplexed connection carrying
//! unnamed reliable bidirectional byte streams, plus connection-level
//! datagrams — over WebRTC (browser-compatible) or QUIC (native), on one UDP
//! port; the public API hides which transport a connection uses.

mod address;
mod api;
mod cert;
mod error;
mod framing;
mod ice;
mod listener;
mod quic;

pub use address::{decode_certhash, encode_certhash, format_address, parse_address, Address};
pub use api::{Conn, Stream};
pub use cert::Identity;
pub use error::{DatagramTooLargeError, Error, ErrorCode, Result, StreamError};
pub use listener::{listen, ListenOptions, Listener};
pub use quic::dial;
