//! The WebRTC implementation of [`Conn`] (SPEC §4): any number of independent
//! byte streams over one PeerConnection, plus the reserved control (ID 0) and
//! datagram (ID 1) channels (SPEC §7, §8).

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
use crate::framing::{decode_code, encode_conn_close};
use crate::stream::WebrtcStream;

/// Caps the WebRTC datagram payload to a sub-MTU size so a datagram travels as
/// a single unreliable SCTP message (fragmenting an unreliable message
/// multiplies its loss). Oversized sends report this limit.
const WEBRTC_MAX_DATAGRAM: usize = 1200;

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
            if self.is_closed() {
                return;
            }
            notified.await;
        }
    }
}

pub(crate) struct WebrtcConn {
    pc: Arc<RTCPeerConnection>,
    stream_rx: tokio::sync::Mutex<mpsc::Receiver<Box<dyn Stream>>>,
    stream_seq: AtomicU64,
    dg: Arc<RTCDataChannel>,
    dg_rx: tokio::sync::Mutex<mpsc::Receiver<Vec<u8>>>,
    control: Arc<RTCDataChannel>,
    /// The peer's UDP endpoint: the first STUN source on the accept side, the
    /// dialed endpoint on the dial side.
    remote: std::net::SocketAddr,
    pub(crate) close_state: Arc<CloseState>,
}

impl WebrtcConn {
    /// Wraps a PeerConnection. `control` is the reserved reliable channel
    /// (ID 0): the client passes the one it created pre-offer (to force the
    /// SCTP m-line); the server passes `None` and we create our side here. It
    /// carries CONNECTION_CLOSE (SPEC §8). `remote` is the peer's UDP endpoint
    /// (see [`Conn::remote_addr`]).
    pub(crate) async fn new(
        pc: Arc<RTCPeerConnection>,
        control: Option<Arc<RTCDataChannel>>,
        remote: std::net::SocketAddr,
    ) -> Result<Self> {
        let close_state = Arc::new(CloseState::new());

        // Client-opened channels surface as streams on the accept queue.
        // Negotiated channels (control/datagram) never fire on_data_channel.
        let (stream_tx, stream_rx) = mpsc::channel::<Box<dyn Stream>>(16);
        {
            pc.on_data_channel(Box::new(move |dc: Arc<RTCDataChannel>| {
                let stream_tx = stream_tx.clone();
                Box::pin(async move {
                    let s = WebrtcStream::new(dc);
                    let _ = stream_tx.send(Box::new(s) as Box<dyn Stream>).await;
                })
            }));
        }

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

        // Reserved reliable control channel (SPEC §8, negotiated, fixed ID 0).
        // Its only message is CONNECTION_CLOSE (a big-endian u32 code) — the
        // WebRTC analogue of QUIC CONNECTION_CLOSE.
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
        {
            let close_state = close_state.clone();
            let pc2 = pc.clone();
            control.on_message(Box::new(move |msg: DataChannelMessage| {
                let close_state = close_state.clone();
                let pc2 = pc2.clone();
                Box::pin(async move {
                    let code = decode_code(&msg.data);
                    if code != ErrorCode::None {
                        close_state.mark(Some(StreamError { code, remote: true }));
                    } else {
                        close_state.mark(None);
                    }
                    let _ = pc2.close().await;
                })
            }));
        }

        Ok(Self {
            pc,
            stream_rx: tokio::sync::Mutex::new(stream_rx),
            stream_seq: AtomicU64::new(0),
            dg,
            dg_rx: tokio::sync::Mutex::new(dg_rx),
            control,
            remote,
            close_state,
        })
    }
}

#[async_trait]
impl Conn for WebrtcConn {
    async fn open_stream(&self) -> Result<Box<dyn Stream>> {
        if self.close_state.is_closed() {
            return Err(Error::ConnClosed);
        }
        let label = format!("kps-{}", self.stream_seq.fetch_add(1, Ordering::Relaxed) + 1);
        let dc = self
            .pc
            .create_data_channel(&label, None)
            .await
            .map_err(|e| Error::Transport(format!("open stream: {e}")))?;
        let s = WebrtcStream::new(dc);
        tokio::select! {
            r = s.wait_open() => {
                r?;
                Ok(Box::new(s) as Box<dyn Stream>)
            }
            _ = self.close_state.wait() => Err(Error::ConnClosed),
        }
    }

    async fn accept_stream(&self) -> Result<Box<dyn Stream>> {
        let mut rx = self.stream_rx.lock().await;
        tokio::select! {
            s = rx.recv() => s.ok_or(Error::ConnClosed),
            _ = self.close_state.wait() => {
                match *self.close_state.err.lock().unwrap() {
                    Some(se) => Err(se.into()),
                    None => Err(Error::ConnClosed),
                }
            }
        }
    }

    async fn close(&self) -> Result<()> {
        self.close_state.mark(None);
        self.pc.close().await.map_err(|e| Error::Transport(e.to_string()))
    }

    /// Conveys an application error code to the peer as a best-effort
    /// CONNECTION_CLOSE on the control channel (SPEC §8) before teardown.
    /// Delivery isn't guaranteed (teardown may race), matching QUIC's
    /// single-packet close.
    async fn close_with_error(&self, code: ErrorCode) -> Result<()> {
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
