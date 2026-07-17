//! The WebRTC implementation of [`Conn`] (SPEC §4): any number of independent
//! byte streams over one PeerConnection, gated on the §8 HELLO exchange, with
//! §6.5 end-to-end flow control, plus the reserved control (ID 0) and datagram
//! (ID 1) channels (SPEC §7, §8).

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use tokio::sync::{mpsc, Notify};
use webrtc::data_channel::data_channel_init::RTCDataChannelInit;
use webrtc::data_channel::data_channel_message::DataChannelMessage;
use webrtc::data_channel::data_channel_state::RTCDataChannelState;
use webrtc::data_channel::RTCDataChannel;
use webrtc::peer_connection::RTCPeerConnection;

use crate::api::{Conn, Stream};
use crate::error::{DatagramTooLargeError, Error, ErrorCode, Result, StreamError};
use crate::flow::{ConnFlow, DEFAULT_FLOW_LIMITS};
use crate::framing::{decode_control, encode_conn_close, encode_hello, ControlMsg, WIRE_VERSION};
use crate::stream::{StreamHooks, WebrtcStream};

/// Caps the WebRTC datagram payload to a sub-MTU size so a datagram travels as
/// a single unreliable SCTP message (fragmenting an unreliable message
/// multiplies its loss). Oversized sends report this limit.
const WEBRTC_MAX_DATAGRAM: usize = 1200;

/// The pre-HELLO state must be bounded (SPEC §8).
const HELLO_TIMEOUT: Duration = Duration::from_secs(15);

/// Close bookkeeping shared with the dialer/listener (they observe peer
/// connection state and call `mark_closed`).
pub(crate) struct CloseState {
    err: Mutex<Option<StreamError>>,
    closed: AtomicBool,
    notify: Notify,
}

impl CloseState {
    fn new() -> Self {
        Self { err: Mutex::new(None), closed: AtomicBool::new(false), notify: Notify::new() }
    }

    pub(crate) fn mark(&self, err: Option<StreamError>) {
        if self.closed.swap(true, Ordering::SeqCst) {
            return; // first close reason wins
        }
        *self.err.lock().unwrap() = err;
        self.notify.notify_waiters();
    }

    fn is_closed(&self) -> bool {
        self.closed.load(Ordering::SeqCst)
    }

    async fn wait(&self) {
        loop {
            let notified = self.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable(); // register BEFORE the check (lost-wakeup)
            if self.is_closed() {
                return;
            }
            notified.await;
        }
    }
}

/// HELLO-establishment bookkeeping (SPEC §8): a connection is established —
/// dial/accept complete — only after HELLO is both sent and received.
struct HelloState {
    sent: AtomicBool,
    received: AtomicBool,
    established: AtomicBool,
    notify: Notify,
}

impl HelloState {
    fn new() -> Self {
        Self {
            sent: AtomicBool::new(false),
            received: AtomicBool::new(false),
            established: AtomicBool::new(false),
            notify: Notify::new(),
        }
    }

    fn check(&self) -> bool {
        if self.sent.load(Ordering::SeqCst)
            && self.received.load(Ordering::SeqCst)
            && !self.established.swap(true, Ordering::SeqCst)
        {
            self.notify.notify_waiters();
            return true;
        }
        false
    }

    fn is_established(&self) -> bool {
        self.established.load(Ordering::SeqCst)
    }

    async fn wait(&self) {
        loop {
            let notified = self.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable(); // register BEFORE the check (lost-wakeup)
            if self.is_established() {
                return;
            }
            notified.await;
        }
    }
}

pub(crate) struct WebrtcConn {
    weak_self: std::sync::Weak<WebrtcConn>,
    pc: Arc<RTCPeerConnection>,
    pub(crate) flow: Arc<ConnFlow>,
    stream_seq: AtomicU64,
    incoming: Arc<IncomingQueue>,
    dg: Arc<RTCDataChannel>,
    dg_rx: tokio::sync::Mutex<mpsc::Receiver<Vec<u8>>>,
    control: Arc<RTCDataChannel>,
    hello: Arc<HelloState>,
    tearing_down: Arc<AtomicBool>,
    /// The peer's UDP endpoint: the first STUN source on the accept side, the
    /// dialed endpoint on the dial side.
    remote: std::net::SocketAddr,
    pub(crate) close_state: Arc<CloseState>,
}

/// The accepted-stream queue. Bounded by MAX_STREAMS credit
/// (peer_stream_opened), NOT by queue capacity: surfacing runs on the
/// transport's callback task, and blocking it stalls every later data-channel
/// event.
struct IncomingQueue {
    queue: Mutex<VecDeque<Box<dyn Stream>>>,
    /// Streams observed before mutual HELLO are staged here (SPEC §8
    /// cross-channel ordering) and surfaced at establishment.
    staged: Mutex<Vec<Box<dyn Stream>>>,
    notify: Notify,
}

impl IncomingQueue {
    fn new() -> Self {
        Self { queue: Mutex::new(VecDeque::new()), staged: Mutex::new(Vec::new()), notify: Notify::new() }
    }

    fn push(&self, s: Box<dyn Stream>) {
        self.queue.lock().unwrap().push_back(s);
        self.notify.notify_waiters();
    }

    fn stage(&self, s: Box<dyn Stream>) {
        self.staged.lock().unwrap().push(s);
    }

    fn flush_staged(&self) {
        let staged: Vec<_> = self.staged.lock().unwrap().drain(..).collect();
        if staged.is_empty() {
            return;
        }
        let mut q = self.queue.lock().unwrap();
        for s in staged {
            q.push_back(s);
        }
        drop(q);
        self.notify.notify_waiters();
    }

    fn pop(&self) -> Option<Box<dyn Stream>> {
        self.queue.lock().unwrap().pop_front()
    }
}

impl WebrtcConn {
    /// Wraps a PeerConnection. `control` is the reserved reliable channel
    /// (ID 0): the client passes the one it created pre-offer (to force the
    /// SCTP m-line); the server passes `None` and we create our side here. It
    /// carries the §8 typed control messages (HELLO, CONNECTION_CLOSE,
    /// credit). `remote` is the peer's UDP endpoint (see [`Conn::remote_addr`]).
    pub(crate) async fn new(
        pc: Arc<RTCPeerConnection>,
        control: Option<Arc<RTCDataChannel>>,
        remote: std::net::SocketAddr,
    ) -> Result<Arc<Self>> {
        let close_state = Arc::new(CloseState::new());
        let hello = Arc::new(HelloState::new());
        let tearing_down = Arc::new(AtomicBool::new(false));
        let incoming = Arc::new(IncomingQueue::new());

        // Connection-level credit advertisements drain to the control channel
        // through this queue (coalescing is unnecessary: values are absolute
        // and 9 bytes each).
        let (advert_tx, advert_rx) = mpsc::unbounded_channel::<Vec<u8>>();
        let flow = Arc::new(ConnFlow::new(DEFAULT_FLOW_LIMITS, advert_tx));

        // Reserved unreliable, unordered datagram channel (SPEC §7/§8):
        // negotiated on both sides at fixed ID 1, so it carries datagrams
        // without DCEP and never surfaces as an application stream.
        let dg = pc
            .create_data_channel(
                "_kps_datagrams",
                Some(RTCDataChannelInit {
                    negotiated: Some(1),
                    ordered: Some(false),
                    max_retransmits: Some(0),
                    ..Default::default()
                }),
            )
            .await
            .map_err(|e| Error::Transport(format!("datagram channel: {e}")))?;
        let (dg_tx, dg_rx) = mpsc::channel::<Vec<u8>>(256);
        dg.on_message(Box::new(move |msg: DataChannelMessage| {
            let dg_tx = dg_tx.clone();
            Box::pin(async move {
                // Bounded buffer: drop when full (datagrams are best-effort).
                let _ = dg_tx.try_send(msg.data.to_vec());
            })
        }));

        // Reserved reliable control channel (SPEC §8, negotiated, fixed ID 0),
        // carrying the typed control messages.
        let control = match control {
            Some(c) => c,
            None => pc
                .create_data_channel(
                    "_kps_control",
                    Some(RTCDataChannelInit { negotiated: Some(0), ..Default::default() }),
                )
                .await
                .map_err(|e| Error::Transport(format!("control channel: {e}")))?,
        };

        let conn = Arc::new_cyclic(|weak| Self {
            weak_self: weak.clone(),
            pc: pc.clone(),
            flow: flow.clone(),
            stream_seq: AtomicU64::new(0),
            incoming: incoming.clone(),
            dg: dg.clone(),
            dg_rx: tokio::sync::Mutex::new(dg_rx),
            control: control.clone(),
            hello: hello.clone(),
            tearing_down: tearing_down.clone(),
            remote,
            close_state: close_state.clone(),
        });

        // Control-writer task: credit advertisements → control channel.
        {
            let control = control.clone();
            let mut advert_rx = advert_rx;
            tokio::spawn(async move {
                while let Some(msg) = advert_rx.recv().await {
                    if control.ready_state() != RTCDataChannelState::Open {
                        continue;
                    }
                    if control.send(&Bytes::from(msg)).await.is_err() {
                        break;
                    }
                }
            });
        }

        // HELLO is sent the moment the control channel opens (it shares a
        // flight with the tail of SCTP establishment — no added RTT).
        {
            let conn2 = conn.clone();
            control.on_open(Box::new(move || {
                let conn2 = conn2.clone();
                Box::pin(async move {
                    conn2.send_hello().await;
                })
            }));
            if control.ready_state() == RTCDataChannelState::Open {
                let conn2 = conn.clone();
                tokio::spawn(async move { conn2.send_hello().await });
            }
        }
        {
            let conn2 = conn.clone();
            control.on_message(Box::new(move |msg: DataChannelMessage| {
                let conn2 = conn2.clone();
                Box::pin(async move {
                    conn2.on_control(&msg.data).await;
                })
            }));
        }
        // Loss of a reserved channel while the connection is healthy is fatal (§8).
        {
            let conn2 = conn.clone();
            control.on_close(Box::new(move || {
                let conn2 = conn2.clone();
                Box::pin(async move { conn2.reserved_channel_lost("control") })
            }));
        }
        {
            let conn2 = conn.clone();
            dg.on_close(Box::new(move || {
                let conn2 = conn2.clone();
                Box::pin(async move { conn2.reserved_channel_lost("datagram") })
            }));
        }

        // Client-opened channels surface as streams (staged until mutual
        // HELLO). Negotiated channels (control/datagram) never fire here.
        {
            let conn2 = conn.clone();
            pc.on_data_channel(Box::new(move |dc: Arc<RTCDataChannel>| {
                let conn2 = conn2.clone();
                Box::pin(async move {
                    conn2.handle_incoming(dc);
                })
            }));
        }

        // Teardown propagation: when the close state marks (transport failure,
        // CONNECTION_CLOSE, local close), fail all credit waiters. Also bound
        // the pre-HELLO state (SPEC §8).
        {
            let close_state = close_state.clone();
            let flow = flow.clone();
            let hello = hello.clone();
            let conn2 = Arc::downgrade(&conn);
            tokio::spawn(async move {
                let timeout = tokio::time::sleep(HELLO_TIMEOUT);
                tokio::pin!(timeout);
                tokio::select! {
                    _ = close_state.wait() => {}
                    _ = hello.wait() => {
                        close_state.wait().await;
                    }
                    _ = &mut timeout => {
                        if let Some(c) = conn2.upgrade() {
                            c.fatal(ErrorCode::Timeout, "HELLO timeout".into());
                        }
                        return;
                    }
                }
                let err = *close_state.err.lock().unwrap();
                flow.fail(err);
            });
        }

        Ok(conn)
    }

    fn stream_hooks(&self, peer_initiated: bool) -> StreamHooks {
        let fatal_conn = self.weak_self.clone();
        let flow = self.flow.clone();
        let td = self.tearing_down.clone();
        let cs = self.close_state.clone();
        StreamHooks {
            fatal: Arc::new(move |code, msg| {
                if let Some(c) = fatal_conn.upgrade() {
                    c.fatal(code, msg);
                }
            }),
            // Only peer-initiated streams return MAX_STREAMS credit (§6.5);
            // for self-initiated streams the peer grants it.
            retired: Arc::new(move || {
                if peer_initiated {
                    flow.peer_stream_retired();
                }
            }),
            is_teardown: Arc::new(move || td.load(Ordering::SeqCst) || cs.is_closed()),
        }
    }

    async fn send_hello(&self) {
        if self.hello.sent.swap(true, Ordering::SeqCst) {
            return;
        }
        if kps_debug_conn() {
            eprintln!("[kps-conn] sending HELLO (control state={:?})", self.control.ready_state());
        }
        let _ = self.control.send(&Bytes::from(encode_hello(&self.flow.local))).await;
        if self.hello.check() {
            self.incoming.flush_staged();
        }
    }

    async fn on_control(&self, data: &[u8]) {
        let m = match decode_control(data) {
            Ok(m) => m,
            Err(msg) => {
                self.fatal(ErrorCode::ProtocolError, msg);
                return;
            }
        };
        if kps_debug_conn() {
            eprintln!("[kps-conn] control in: {m:?}");
        }
        match m {
            ControlMsg::Hello { version, limits } => {
                if self.hello.received.swap(true, Ordering::SeqCst) {
                    self.fatal(ErrorCode::ProtocolError, "duplicate HELLO".into());
                    return;
                }
                if version != WIRE_VERSION {
                    let _ = self.control.send(&Bytes::from(encode_conn_close(ErrorCode::Unsupported))).await;
                    self.tearing_down.store(true, Ordering::SeqCst);
                    self.close_state.mark(Some(StreamError { code: ErrorCode::Unsupported, remote: false }));
                    let _ = self.pc.close().await;
                    return;
                }
                self.flow.on_peer_hello(limits);
                if self.hello.check() {
                    self.incoming.flush_staged();
                }
            }
            ControlMsg::Close(code) => {
                // Valid at any time — before HELLO it is a handshake rejection (§8).
                self.tearing_down.store(true, Ordering::SeqCst);
                if code != ErrorCode::None {
                    self.close_state.mark(Some(StreamError { code, remote: true }));
                } else {
                    self.close_state.mark(None);
                }
                let _ = self.pc.close().await;
            }
            ControlMsg::MaxData(v) => {
                if !self.hello.received.load(Ordering::SeqCst) {
                    self.fatal(ErrorCode::ProtocolError, "control message before HELLO".into());
                    return;
                }
                self.flow.on_peer_max_data(v);
            }
            ControlMsg::MaxStreams(v) => {
                if !self.hello.received.load(Ordering::SeqCst) {
                    self.fatal(ErrorCode::ProtocolError, "control message before HELLO".into());
                    return;
                }
                self.flow.on_peer_max_streams(v);
            }
        }
    }

    fn handle_incoming(&self, dc: Arc<RTCDataChannel>) {
        // A peer-opened stream consumes a slot immediately, even unaccepted or
        // pre-HELLO (§6.5).
        if let Err(msg) = self.flow.peer_stream_opened() {
            self.fatal(ErrorCode::ProtocolError, msg);
            return;
        }
        let s = WebrtcStream::new(dc, self.flow.clone(), self.stream_hooks(true));
        let boxed = Box::new(s) as Box<dyn Stream>;
        if self.hello.is_established() {
            self.incoming.push(boxed);
        } else {
            self.incoming.stage(boxed);
        }
    }

    fn reserved_channel_lost(&self, which: &str) {
        if self.tearing_down.load(Ordering::SeqCst) || self.close_state.is_closed() {
            return;
        }
        // If the PeerConnection/SCTP association itself failed, every channel
        // dies at once — that is the connection-level network-error path
        // (SPEC §8), not a peer protocol violation.
        use webrtc::peer_connection::peer_connection_state::RTCPeerConnectionState as PCState;
        match self.pc.connection_state() {
            PCState::Failed | PCState::Closed | PCState::Disconnected => {
                self.tearing_down.store(true, Ordering::SeqCst);
                self.close_state
                    .mark(Some(StreamError { code: ErrorCode::NetworkError, remote: false }));
                let pc = self.pc.clone();
                tokio::spawn(async move {
                    let _ = pc.close().await;
                });
            }
            _ => self.fatal(ErrorCode::ProtocolError, format!("reserved {which} channel lost")),
        }
    }

    /// A peer wire violation (or local fatal condition): convey the code
    /// best-effort, then tear the connection down.
    pub(crate) fn fatal(&self, code: ErrorCode, msg: String) {
        if self.tearing_down.swap(true, Ordering::SeqCst) {
            return;
        }
        if kps_debug_conn() {
            eprintln!("[kps-conn] fatal: {code} — {msg}");
        }
        let control = self.control.clone();
        let pc = self.pc.clone();
        let close_state = self.close_state.clone();
        tokio::spawn(async move {
            if control.ready_state() == RTCDataChannelState::Open {
                let _ = control.send(&Bytes::from(encode_conn_close(code))).await;
            }
            close_state.mark(Some(StreamError { code, remote: false }));
            let _ = pc.close().await;
        });
    }

    /// Blocks until the mutual HELLO exchange completes (SPEC §8: dial/accept
    /// MUST NOT complete before it) or the connection dies.
    pub(crate) async fn wait_established(&self) -> Result<()> {
        tokio::select! {
            _ = self.hello.wait() => Ok(()),
            _ = self.close_state.wait() => {
                match *self.close_state.err.lock().unwrap() {
                    Some(se) => Err(se.into()),
                    None => Err(Error::ConnClosed),
                }
            }
        }
    }
}

fn kps_debug_conn() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| std::env::var_os("KPS_DEBUG").is_some())
}

#[async_trait]
impl Conn for WebrtcConn {
    async fn open_stream(&self) -> Result<Box<dyn Stream>> {
        if self.close_state.is_closed() {
            return Err(Error::ConnClosed);
        }
        // An endpoint MUST NOT open application streams before mutual HELLO
        // (§8); dial/accept already gate on it, so this only guards early
        // callers.
        self.wait_established().await?;
        // Stream-count credit: reserve a slot (waits at the peer's limit,
        // fails on teardown), commit on successful channel creation.
        self.flow.reserve_stream_slot().await?;
        let label = format!("kps-{}", self.stream_seq.fetch_add(1, Ordering::Relaxed) + 1);
        let dc = match self.pc.create_data_channel(&label, None).await {
            Ok(dc) => dc,
            Err(e) => {
                self.flow.release_stream_slot();
                return Err(Error::Transport(format!("open stream: {e}")));
            }
        };
        self.flow.commit_stream_slot();
        let s = WebrtcStream::new(dc, self.flow.clone(), self.stream_hooks(false));
        tokio::select! {
            r = s.wait_open() => {
                r?;
                Ok(Box::new(s) as Box<dyn Stream>)
            }
            _ = self.close_state.wait() => Err(Error::ConnClosed),
        }
    }

    async fn accept_stream(&self) -> Result<Box<dyn Stream>> {
        loop {
            let notified = self.incoming.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable(); // register BEFORE the check (lost-wakeup)
            if let Some(s) = self.incoming.pop() {
                return Ok(s);
            }
            if self.close_state.is_closed() {
                return match *self.close_state.err.lock().unwrap() {
                    Some(se) => Err(se.into()),
                    None => Err(Error::ConnClosed),
                };
            }
            tokio::select! {
                _ = notified => {}
                _ = self.close_state.wait() => {}
            }
        }
    }

    async fn close(&self) -> Result<()> {
        self.tearing_down.store(true, Ordering::SeqCst);
        self.send_close_and_flush(ErrorCode::None).await;
        self.close_state.mark(None);
        self.pc.close().await.map_err(|e| Error::Transport(e.to_string()))
    }

    /// Conveys an application error code to the peer as a best-effort
    /// CONNECTION_CLOSE on the control channel (SPEC §8) before teardown.
    /// Delivery isn't guaranteed (teardown may race), matching QUIC's
    /// single-packet close.
    async fn close_with_error(&self, code: ErrorCode) -> Result<()> {
        self.tearing_down.store(true, Ordering::SeqCst);
        self.send_close_and_flush(code).await;
        if code != ErrorCode::None {
            self.close_state.mark(Some(StreamError { code, remote: false }));
        } else {
            self.close_state.mark(None);
        }
        self.pc.close().await.map_err(|e| Error::Transport(e.to_string()))
    }

    async fn closed(&self) {
        self.close_state.wait().await;
    }

    fn err(&self) -> Option<Error> {
        if !self.close_state.is_closed() {
            return None; // still open
        }
        self.close_state.err.lock().unwrap().map(Error::Stream)
    }

    fn remote_addr(&self) -> std::net::SocketAddr {
        self.remote
    }

    async fn send_datagram(&self, p: &[u8]) -> Result<()> {
        if p.len() > WEBRTC_MAX_DATAGRAM {
            return Err(DatagramTooLargeError { max_datagram_payload_size: WEBRTC_MAX_DATAGRAM }.into());
        }
        if self.dg.ready_state() != RTCDataChannelState::Open {
            return Err(Error::Transport("datagram channel not open".into()));
        }
        self.dg
            .send(&Bytes::copy_from_slice(p))
            .await
            .map_err(|e| Error::Transport(format!("send datagram: {e}")))?;
        Ok(())
    }

    async fn receive_datagram(&self) -> Result<Vec<u8>> {
        let mut rx = self.dg_rx.lock().await;
        tokio::select! {
            d = rx.recv() => d.ok_or(Error::ConnClosed),
            _ = self.close_state.wait() => Err(Error::ConnClosed),
        }
    }
}

impl WebrtcConn {
    async fn send_close_and_flush(&self, code: ErrorCode) {
        // The control channel opens asynchronously after the PC connects; a
        // close right after dial can beat it. Wait briefly for it to open,
        // send, then let the reliable message flush before tearing down SCTP
        // (pc.close() aborts in-flight data). All bounded so close stays prompt.
        let open_by = tokio::time::Instant::now() + Duration::from_millis(250);
        while self.control.ready_state() == RTCDataChannelState::Connecting
            && tokio::time::Instant::now() < open_by
        {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        if self.control.ready_state() == RTCDataChannelState::Open {
            let _ = self.control.send(&Bytes::from(encode_conn_close(code))).await;
            let flush_by = tokio::time::Instant::now() + Duration::from_millis(250);
            while self.control.buffered_amount().await > 0
                && tokio::time::Instant::now() < flush_by
            {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        }
    }
}

// Dial/listener hand out `Arc<WebrtcConn>` (the conn's callbacks hold weak
// references to it); delegate the public trait through the Arc.
#[async_trait]
impl Conn for Arc<WebrtcConn> {
    async fn open_stream(&self) -> Result<Box<dyn Stream>> {
        WebrtcConn::open_stream(self).await
    }
    async fn accept_stream(&self) -> Result<Box<dyn Stream>> {
        WebrtcConn::accept_stream(self).await
    }
    async fn close(&self) -> Result<()> {
        WebrtcConn::close(self).await
    }
    async fn close_with_error(&self, code: ErrorCode) -> Result<()> {
        WebrtcConn::close_with_error(self, code).await
    }
    async fn closed(&self) {
        WebrtcConn::closed(self).await
    }
    fn err(&self) -> Option<Error> {
        WebrtcConn::err(self)
    }
    fn remote_addr(&self) -> std::net::SocketAddr {
        WebrtcConn::remote_addr(self)
    }
    async fn send_datagram(&self, p: &[u8]) -> Result<()> {
        WebrtcConn::send_datagram(self, p).await
    }
    async fn receive_datagram(&self) -> Result<Vec<u8>> {
        WebrtcConn::receive_datagram(self).await
    }
}
