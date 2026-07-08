//! The KPS listener: accepts connections from both transports on one UDP port
//! behind one address (SPEC §5).
//!
//! Phase note: currently QUIC-only on a dedicated socket; the WebRTC leg and
//! the single-socket demux (SPEC §5.1) land next and only change internals —
//! the accept surface is final.

use std::path::PathBuf;

use tokio::sync::mpsc;

use crate::address::format_address;
use crate::api::Conn;
use crate::cert::Identity;
use crate::error::{Error, Result};
use crate::quic::{server_config, QuicConn};

/// Listener options, mirroring Go's `kps.Options`.
#[derive(Default)]
pub struct ListenOptions {
    /// When set, used directly; the listener writes nothing to disk and the
    /// caller is responsible for persistence (see [`Identity`]).
    pub identity: Option<Identity>,
    /// Path to the persistent combined PEM (PRIVATE KEY + CERTIFICATE).
    /// Created if absent. Ignored when `identity` is set. Defaults to
    /// `"kps.key"`.
    pub key_file: Option<PathBuf>,
}

/// Accepts kps connections on a UDP port. The same port serves any number of
/// clients on either transport.
pub struct Listener {
    identity_certhash: String,
    port: u16,
    local_ip: String,
    accept_rx: tokio::sync::Mutex<mpsc::Receiver<Box<dyn Conn>>>,
    endpoint: quinn::Endpoint,
}

/// Binds a UDP socket and starts accepting kps connections. `addr` is a
/// `host:port` string (use `":0"` or `"127.0.0.1:0"` for an ephemeral port).
pub async fn listen(addr: &str, opts: ListenOptions) -> Result<Listener> {
    let identity = match opts.identity {
        Some(id) => id,
        None => {
            let path = opts.key_file.unwrap_or_else(|| PathBuf::from("kps.key"));
            Identity::load_or_create(path)?
        }
    };

    // A bare ":port" means the dual-stack wildcard, like Go's net.Listen.
    let bind: std::net::SocketAddr = if let Some(port) = addr.strip_prefix(':') {
        format!("[::]:{port}")
            .parse()
            .map_err(|e| Error::Address(format!("bad listen addr {addr:?}: {e}")))?
    } else {
        addr.parse()
            .map_err(|e| Error::Address(format!("bad listen addr {addr:?}: {e}")))?
    };

    let config = server_config(&identity)?;
    let endpoint = quinn::Endpoint::server(config, bind)
        .map_err(|e| Error::Transport(format!("listen {addr:?}: {e}")))?;
    let local = endpoint.local_addr()?;

    let (accept_tx, accept_rx) = mpsc::channel::<Box<dyn Conn>>(16);

    // QUIC accept loop: deliver accepted connections to the shared queue (the
    // WebRTC leg will feed the same queue).
    {
        let endpoint = endpoint.clone();
        let accept_tx = accept_tx.clone();
        tokio::spawn(async move {
            while let Some(incoming) = endpoint.accept().await {
                let accept_tx = accept_tx.clone();
                tokio::spawn(async move {
                    if let Ok(conn) = incoming.await {
                        let _ = accept_tx.send(Box::new(QuicConn::new(conn, None)) as Box<dyn Conn>).await;
                    }
                });
            }
        });
    }

    Ok(Listener {
        identity_certhash: identity.certhash.clone(),
        port: local.port(),
        local_ip: local.ip().to_string(),
        accept_rx: tokio::sync::Mutex::new(accept_rx),
        endpoint,
    })
}

impl Listener {
    /// The public-facing kps address (`ip:port:certhash`) for the requested
    /// ip. If `ip` is empty, uses the bound socket's address, falling back to
    /// `127.0.0.1` for wildcards; pass a LAN/public IP explicitly for clients
    /// dialing across machines.
    pub fn address(&self, ip: &str) -> String {
        let ip = if ip.is_empty() {
            match self.local_ip.as_str() {
                "0.0.0.0" | "::" => "127.0.0.1",
                other => other,
            }
        } else {
            ip
        };
        format_address(ip, self.port, &self.identity_certhash)
    }

    /// The UDP port the listener bound to.
    pub fn port(&self) -> u16 {
        self.port
    }

    /// The multibase-encoded sha-256 multihash clients pin.
    pub fn certhash(&self) -> &str {
        &self.identity_certhash
    }

    /// Returns the next established connection. Apply timeouts caller-side
    /// with `tokio::time::timeout`.
    pub async fn accept(&self) -> Result<Box<dyn Conn>> {
        let mut rx = self.accept_rx.lock().await;
        rx.recv().await.ok_or(Error::ConnClosed)
    }

    /// Shuts the listener down.
    pub async fn close(&self) {
        self.endpoint.close(quinn::VarInt::from_u32(0), b"");
    }
}
