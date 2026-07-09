//! The KPS listener: both transports on one UDP port behind one address
//! (SPEC §5). A pump task reads the shared socket and demuxes per SPEC §5.1:
//! known WebRTC peer address → that connection; STUN → WebRTC (by ufrag,
//! spawning an ICE-lite PeerConnection per new ufrag); everything else → QUIC
//! (which demultiplexes its own connections by connection ID).

use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use tokio::net::UdpSocket;
use tokio::sync::{mpsc, Notify};
use webrtc::api::setting_engine::SettingEngine;
use webrtc::api::APIBuilder;
use webrtc::ice::network_type::NetworkType;
use webrtc::ice::udp_mux::UDPMux;
use webrtc::ice::udp_network::UDPNetwork;
use webrtc::peer_connection::configuration::RTCConfiguration;
use webrtc::peer_connection::peer_connection_state::RTCPeerConnectionState;
use webrtc::peer_connection::sdp::session_description::RTCSessionDescription;
use webrtc::peer_connection::RTCPeerConnection;

use crate::address::format_address;
use crate::api::Conn;
use crate::cert::Identity;
use crate::conn::WebrtcConn;
use crate::error::{Error, Result};
use crate::ice::derive_ice_pwd;
use crate::quic::{server_config, QuicConn};

type Packet = (Vec<u8>, SocketAddr);

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
    certhash: String,
    port: u16,
    local_ip: String,
    accept_rx: tokio::sync::Mutex<mpsc::Receiver<Box<dyn Conn>>>,
    endpoint: quinn::Endpoint,
    shutdown: Arc<Notify>,
    entries: Entries,
}

/// Per-ufrag WebRTC routing state, shared between the pump and PC lifecycle
/// callbacks (mirrors Go's byUfrag/byAddr under one lock).
#[derive(Clone, Default)]
struct Entries(Arc<Mutex<EntriesInner>>);

#[derive(Default)]
struct EntriesInner {
    by_ufrag: HashMap<String, Entry>,
    by_addr: HashMap<SocketAddr, mpsc::Sender<Packet>>,
}

#[derive(Clone)]
struct Entry {
    inbox: mpsc::Sender<Packet>,
    pc: Arc<RTCPeerConnection>,
}

impl Entries {
    fn remove_ufrag(&self, ufrag: &str) {
        let mut inner = self.0.lock().unwrap();
        if let Some(entry) = inner.by_ufrag.remove(ufrag) {
            inner.by_addr.retain(|_, tx| !tx.same_channel(&entry.inbox));
        }
    }

    fn close_all(&self) -> Vec<Arc<RTCPeerConnection>> {
        let mut inner = self.0.lock().unwrap();
        inner.by_addr.clear();
        inner.by_ufrag.drain().map(|(_, e)| e.pc).collect()
    }
}

/// Binds a UDP socket and starts accepting kps connections. `addr` is a
/// `host:port` string (use `":0"` or `"127.0.0.1:0"` for an ephemeral port).
/// A bare `":port"` binds the dual-stack wildcard, like Go.
pub async fn listen(addr: &str, opts: ListenOptions) -> Result<Listener> {
    let identity = match opts.identity {
        Some(id) => id,
        None => {
            let path = opts.key_file.unwrap_or_else(|| PathBuf::from("kps.key"));
            Identity::load_or_create(path)?
        }
    };

    let bind: SocketAddr = if let Some(port) = addr.strip_prefix(':') {
        format!("[::]:{port}")
            .parse()
            .map_err(|e| Error::Address(format!("bad listen addr {addr:?}: {e}")))?
    } else {
        addr.parse()
            .map_err(|e| Error::Address(format!("bad listen addr {addr:?}: {e}")))?
    };

    let socket = Arc::new(
        UdpSocket::bind(bind)
            .await
            .map_err(|e| Error::Transport(format!("listen {addr:?}: {e}")))?,
    );
    let local = socket.local_addr()?;

    let (accept_tx, accept_rx) = mpsc::channel::<Box<dyn Conn>>(16);
    let shutdown = Arc::new(Notify::new());
    let entries = Entries::default();

    // QUIC transport over a virtual socket the pump feeds (SPEC §5.1, §5.3).
    let (quic_tx, quic_rx) = mpsc::channel::<Packet>(256);
    let demux_socket = Arc::new(DemuxUdpSocket {
        io: socket.clone(),
        inbox: Mutex::new(quic_rx),
        local,
    });
    let endpoint = quinn::Endpoint::new_with_abstract_socket(
        quinn::EndpointConfig::default(),
        Some(server_config(&identity)?),
        demux_socket,
        Arc::new(quinn::TokioRuntime),
    )
    .map_err(|e| Error::Transport(format!("quic endpoint: {e}")))?;

    // QUIC accept loop: deliver accepted connections to the shared queue.
    {
        let endpoint = endpoint.clone();
        let accept_tx = accept_tx.clone();
        tokio::spawn(async move {
            while let Some(incoming) = endpoint.accept().await {
                let accept_tx = accept_tx.clone();
                tokio::spawn(async move {
                    if let Ok(conn) = incoming.await {
                        let _ = accept_tx
                            .send(Box::new(QuicConn::new(conn, None)) as Box<dyn Conn>)
                            .await;
                    }
                });
            }
        });
    }

    let certhash = identity.certhash.clone();

    // The pump: demux every inbound packet (SPEC §5.1).
    tokio::spawn(pump(
        socket.clone(),
        identity,
        quic_tx,
        accept_tx,
        entries.clone(),
        shutdown.clone(),
    ));

    Ok(Listener {
        certhash,
        port: local.port(),
        local_ip: local.ip().to_string(),
        accept_rx: tokio::sync::Mutex::new(accept_rx),
        endpoint,
        shutdown,
        entries,
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
        format_address(ip, self.port, &self.certhash)
    }

    /// The UDP port the listener bound to.
    pub fn port(&self) -> u16 {
        self.port
    }

    /// The multibase-encoded sha-256 multihash clients pin.
    pub fn certhash(&self) -> &str {
        &self.certhash
    }

    /// Returns the next established connection (either transport). Apply
    /// timeouts caller-side with `tokio::time::timeout`.
    pub async fn accept(&self) -> Result<Box<dyn Conn>> {
        let mut rx = self.accept_rx.lock().await;
        rx.recv().await.ok_or(Error::ConnClosed)
    }

    /// Shuts the listener down: stops the pump, closes the QUIC endpoint and
    /// every WebRTC peer connection.
    pub async fn close(&self) {
        self.shutdown.notify_waiters();
        self.endpoint.close(quinn::VarInt::from_u32(0), b"");
        for pc in self.entries.close_all() {
            let _ = pc.close().await;
        }
    }
}

/// Reads the shared socket and routes every datagram per SPEC §5.1.
async fn pump(
    socket: Arc<UdpSocket>,
    identity: Identity,
    quic_tx: mpsc::Sender<Packet>,
    accept_tx: mpsc::Sender<Box<dyn Conn>>,
    entries: Entries,
    shutdown: Arc<Notify>,
) {
    let identity = Arc::new(identity);
    let mut buf = vec![0u8; 1500];
    loop {
        let (n, from) = tokio::select! {
            r = socket.recv_from(&mut buf) => match r {
                Ok(v) => v,
                Err(_) => continue,
            },
            _ = shutdown.notified() => return,
        };
        // On a dual-stack socket a v4 client arrives as a v4-mapped IPv6
        // address (::ffff:a.b.c.d). Normalize to pure v4 so map keys and the
        // per-connection candidate family agree; sends re-map (send_via).
        let from = SocketAddr::new(from.ip().to_canonical(), from.port());
        let pkt = buf[..n].to_vec();

        let route = {
            let inner = entries.0.lock().unwrap();
            inner.by_addr.get(&from).cloned()
        };

        let route = match route {
            Some(tx) => Some(tx),
            None if stun::message::is_message(&pkt) => {
                match extract_ufrag(&pkt) {
                    Some(ufrag) => {
                        let tx = {
                            let inner = entries.0.lock().unwrap();
                            inner.by_ufrag.get(&ufrag).map(|e| e.inbox.clone())
                        };
                        let tx = match tx {
                            Some(tx) => tx,
                            None => {
                                let (tx, rx) = mpsc::channel::<Packet>(256);
                                match spawn_pc(
                                    socket.clone(),
                                    identity.clone(),
                                    ufrag.clone(),
                                    from,
                                    rx,
                                    accept_tx.clone(),
                                    entries.clone(),
                                )
                                .await
                                {
                                    Ok(pc) => {
                                        let mut inner = entries.0.lock().unwrap();
                                        inner.by_ufrag.insert(
                                            ufrag.clone(),
                                            Entry { inbox: tx.clone(), pc },
                                        );
                                        tx
                                    }
                                    Err(_) => continue,
                                }
                            }
                        };
                        let mut inner = entries.0.lock().unwrap();
                        inner.by_addr.insert(from, tx.clone());
                        Some(tx)
                    }
                    None => None,
                }
            }
            None => None,
        };

        match route {
            Some(tx) => {
                // Inbox full → drop (same as Go).
                let _ = tx.try_send((pkt, from));
            }
            None => {
                // Not an established WebRTC peer and not a new STUN binding:
                // the only other transport is QUIC (SPEC §5.1).
                let _ = quic_tx.try_send((pkt, from));
            }
        }
    }
}

fn extract_ufrag(pkt: &[u8]) -> Option<String> {
    let mut m = stun::message::Message::new();
    m.unmarshal_binary(pkt).ok()?;
    let (attr, found) = m.attributes.get(stun::attributes::ATTR_USERNAME);
    if !found {
        return None;
    }
    let s = String::from_utf8(attr.value).ok()?;
    Some(s.split(':').next()?.to_string())
}

/// Spawns the server-side ICE-lite PeerConnection for one client ufrag,
/// reading from its own inbox and writing via the shared socket. Ported from
/// libs/go/listener.go spawnPC, with the webrtc-rs-specific single-candidate
/// constraint (see the interface filter below).
async fn spawn_pc(
    socket: Arc<UdpSocket>,
    identity: Arc<Identity>,
    ufrag: String,
    client_addr: SocketAddr,
    inbox: mpsc::Receiver<Packet>,
    accept_tx: mpsc::Sender<Box<dyn Conn>>,
    entries: Entries,
) -> Result<Arc<RTCPeerConnection>> {
    let local = socket.local_addr()?;
    let port = local.port();
    let pwd = derive_ice_pwd(&identity.digest, &ufrag);

    let pc_conn = Arc::new(PcConn {
        socket: socket.clone(),
        inbox: tokio::sync::Mutex::new(inbox),
        local,
    });

    let mut se = SettingEngine::default();
    se.set_lite(true);
    se.set_udp_network(UDPNetwork::Muxed(Arc::new(SingleConnMux { conn: pc_conn })));
    // Derive the ICE password from the pinned certhash (SPEC §5.2); the client
    // computes the identical value, so only a certhash-holder passes STUN
    // integrity. Pin our local creds to (ufrag, derived pwd).
    se.set_ice_credentials(ufrag.clone(), pwd.clone());
    se.disable_certificate_fingerprint_verification(true);
    // Restrict ICE to the client's address family: a candidate pair only forms
    // when local and remote families match. Writes still go out the one shared
    // socket regardless.
    se.set_network_types(vec![if client_addr.is_ipv6() {
        NetworkType::Udp6
    } else {
        NetworkType::Udp4
    }]);
    // CRITICAL (webrtc-rs quirk): muxed gathering creates one host candidate
    // PER interface and every candidate spawns a recv loop racing on the same
    // muxed conn — inbound STUN gets attributed to a random local candidate and
    // the controlled agent never converges. The server's local candidates are
    // cosmetic (ICE-lite; the answer SDP is never transmitted), so pin
    // gathering to the loopback interface → exactly one candidate per family →
    // one recv loop. Loopback is excluded by default, so include it explicitly.
    se.set_interface_filter(Box::new(|name: &str| name.starts_with("lo")));
    se.set_include_loopback_candidate(true);

    let api = APIBuilder::new().with_setting_engine(se).build();
    let pc = Arc::new(
        api.new_peer_connection(RTCConfiguration {
            certificates: vec![identity.rtc_certificate()?],
            ..Default::default()
        })
        .await
        .map_err(|e| Error::Transport(format!("peer connection: {e}")))?,
    );

    // WebrtcConn owns on_data_channel (client-opened channels surface as
    // streams) and creates our side of the negotiated control (ID 0) and
    // datagram (ID 1) channels (SPEC §8).
    let conn = WebrtcConn::new(pc.clone(), None).await?;
    let close_state = conn.close_state.clone();
    let conn_slot = Arc::new(Mutex::new(Some(conn)));

    {
        let pc2 = pc.clone();
        let ufrag = ufrag.clone();
        pc.on_peer_connection_state_change(Box::new(move |s: RTCPeerConnectionState| {
            let accept_tx = accept_tx.clone();
            let conn_slot = conn_slot.clone();
            let close_state = close_state.clone();
            let entries = entries.clone();
            let ufrag = ufrag.clone();
            let _pc = pc2.clone();
            Box::pin(async move {
                match s {
                    RTCPeerConnectionState::Connected => {
                        // Take the conn out of the slot before awaiting (a
                        // MutexGuard must not live across an await).
                        let taken = conn_slot.lock().unwrap().take();
                        if let Some(conn) = taken {
                            let _ = accept_tx.send(Box::new(conn) as Box<dyn Conn>).await;
                        }
                    }
                    RTCPeerConnectionState::Closed | RTCPeerConnectionState::Failed => {
                        close_state.mark(None);
                        entries.remove_ufrag(&ufrag);
                    }
                    _ => {}
                }
            })
        }));
    }

    let offer = RTCSessionDescription::offer(build_client_offer(&ufrag, &pwd, port, client_addr))
        .map_err(|e| Error::Transport(format!("offer: {e}")))?;
    pc.set_remote_description(offer)
        .await
        .map_err(|e| Error::Transport(format!("set remote: {e}")))?;
    let answer = pc
        .create_answer(None)
        .await
        .map_err(|e| Error::Transport(format!("answer: {e}")))?;
    // Never transmitted — KPS has no signaling; the client synthesizes it.
    pc.set_local_description(answer)
        .await
        .map_err(|e| Error::Transport(format!("set local: {e}")))?;

    Ok(pc)
}

/// Fabricates the SDP offer the browser would have produced (the server never
/// sees the real one). The DTLS fingerprint is a placeholder because the
/// server doesn't pin the client's identity.
fn build_client_offer(ufrag: &str, pwd: &str, port: u16, client_addr: SocketAddr) -> String {
    const PLACEHOLDER_FP: &str = "AA:BB:CC:DD:EE:FF:00:11:22:33:44:55:66:77:88:99:AA:BB:CC:DD:EE:FF:00:11:22:33:44:55:66:77:88:99";
    let (fam, any_addr) =
        if client_addr.is_ipv6() { ("IP6", "::") } else { ("IP4", "0.0.0.0") };
    [
        "v=0".to_string(),
        format!("o=- 0 0 IN {fam} {any_addr}"),
        "s=-".to_string(),
        "t=0 0".to_string(),
        format!("m=application {port} UDP/DTLS/SCTP webrtc-datachannel"),
        format!("c=IN {fam} {any_addr}"),
        "a=mid:0".to_string(),
        format!("a=ice-ufrag:{ufrag}"),
        format!("a=ice-pwd:{pwd}"),
        format!("a=fingerprint:sha-256 {PLACEHOLDER_FP}"),
        "a=setup:active".to_string(),
        "a=sctp-port:5000".to_string(),
        "a=max-message-size:1048576".to_string(),
    ]
    .join("\r\n")
        + "\r\n"
}

/// Re-maps a destination for the socket family: a v4 destination on a v6
/// (dual-stack) socket must be sent as a v4-mapped v6 address.
fn map_for_socket(local: &SocketAddr, target: SocketAddr) -> SocketAddr {
    match (local, target) {
        (SocketAddr::V6(_), SocketAddr::V4(v4)) => {
            SocketAddr::new(IpAddr::V6(v4.ip().to_ipv6_mapped()), v4.port())
        }
        _ => target,
    }
}

// ---------------------------------------------------------------------------
// WebRTC side: per-PC packet conn + single-conn mux (Go: pcPacketConn +
// singleConnMux).

/// `webrtc::util::Conn` for a single PeerConnection: reads pull from a per-PC
/// inbox fed by the pump; writes go to the shared real UDP socket.
struct PcConn {
    socket: Arc<UdpSocket>,
    inbox: tokio::sync::Mutex<mpsc::Receiver<Packet>>,
    local: SocketAddr,
}

#[async_trait::async_trait]
impl webrtc::util::Conn for PcConn {
    async fn connect(&self, _addr: SocketAddr) -> webrtc::util::Result<()> {
        Err(webrtc::util::Error::Other("connect unsupported".into()))
    }
    async fn recv(&self, buf: &mut [u8]) -> webrtc::util::Result<usize> {
        self.recv_from(buf).await.map(|(n, _)| n)
    }
    async fn recv_from(&self, buf: &mut [u8]) -> webrtc::util::Result<(usize, SocketAddr)> {
        let mut rx = self.inbox.lock().await;
        match rx.recv().await {
            Some((pkt, from)) => {
                let n = pkt.len().min(buf.len());
                buf[..n].copy_from_slice(&pkt[..n]);
                Ok((n, from))
            }
            None => Err(webrtc::util::Error::Other("inbox closed".into())),
        }
    }
    async fn send(&self, _buf: &[u8]) -> webrtc::util::Result<usize> {
        Err(webrtc::util::Error::Other("send unsupported".into()))
    }
    async fn send_to(&self, buf: &[u8], target: SocketAddr) -> webrtc::util::Result<usize> {
        let target = map_for_socket(&self.local, target);
        Ok(self.socket.send_to(buf, target).await?)
    }
    fn local_addr(&self) -> webrtc::util::Result<SocketAddr> {
        Ok(self.local)
    }
    fn remote_addr(&self) -> Option<SocketAddr> {
        None
    }
    async fn close(&self) -> webrtc::util::Result<()> {
        Ok(())
    }
    fn as_any(&self) -> &(dyn std::any::Any + Send + Sync) {
        self
    }
}

/// Satisfies the ICE agent's mux interface by always returning this PC's conn
/// regardless of ufrag — each PC's agent reads from its own inbox.
struct SingleConnMux {
    conn: Arc<PcConn>,
}

#[async_trait::async_trait]
impl UDPMux for SingleConnMux {
    async fn close(&self) -> std::result::Result<(), webrtc::util::Error> {
        Ok(())
    }
    async fn get_conn(
        self: Arc<Self>,
        _ufrag: &str,
    ) -> std::result::Result<Arc<dyn webrtc::util::Conn + Send + Sync>, webrtc::util::Error> {
        Ok(self.conn.clone() as Arc<dyn webrtc::util::Conn + Send + Sync>)
    }
    async fn remove_conn_by_ufrag(&self, _ufrag: &str) {}
}

// ---------------------------------------------------------------------------
// QUIC side: virtual AsyncUdpSocket (Go: quicPacketConn). Reads pull from an
// inbox channel fed by the pump with the non-WebRTC packets; writes go to the
// shared real UDP socket. quinn demultiplexes its own connections by
// connection ID.

struct DemuxUdpSocket {
    io: Arc<UdpSocket>,
    inbox: Mutex<mpsc::Receiver<Packet>>,
    local: SocketAddr,
}

impl std::fmt::Debug for DemuxUdpSocket {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DemuxUdpSocket").field("local", &self.local).finish()
    }
}

impl quinn::AsyncUdpSocket for DemuxUdpSocket {
    fn create_io_poller(self: Arc<Self>) -> Pin<Box<dyn quinn::UdpPoller>> {
        Box::pin(WritePoller { io: self.io.clone() })
    }

    fn try_send(&self, transmit: &quinn::udp::Transmit) -> std::io::Result<()> {
        let target = map_for_socket(&self.local, transmit.destination);
        self.io.try_send_to(transmit.contents, target).map(|_| ())
    }

    fn poll_recv(
        &self,
        cx: &mut Context,
        bufs: &mut [std::io::IoSliceMut<'_>],
        meta: &mut [quinn::udp::RecvMeta],
    ) -> Poll<std::io::Result<usize>> {
        let mut rx = self.inbox.lock().unwrap();
        match rx.poll_recv(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(None) => {
                Poll::Ready(Err(std::io::Error::new(std::io::ErrorKind::BrokenPipe, "closed")))
            }
            Poll::Ready(Some((pkt, from))) => {
                let n = pkt.len().min(bufs[0].len());
                bufs[0][..n].copy_from_slice(&pkt[..n]);
                meta[0] = quinn::udp::RecvMeta {
                    addr: from,
                    len: n,
                    stride: n,
                    ecn: None,
                    dst_ip: None,
                };
                Poll::Ready(Ok(1))
            }
        }
    }

    fn local_addr(&self) -> std::io::Result<SocketAddr> {
        Ok(self.local)
    }
}

#[derive(Debug)]
struct WritePoller {
    io: Arc<UdpSocket>,
}

impl quinn::UdpPoller for WritePoller {
    fn poll_writable(self: Pin<&mut Self>, cx: &mut Context) -> Poll<std::io::Result<()>> {
        self.io.poll_send_ready(cx)
    }
}
