//! The WebRTC implementation of [`Stream`] (SPEC §6.2 framing + §6.5 flow
//! control): a byte stream over one SCTP data channel. The data-channel label
//! is a non-semantic implementation detail.
//!
//! Outbound frames travel two queues into one writer task: the bounded,
//! ordered DATA queue (DATA and FIN — FIN must follow the DATA the write API
//! already accepted) and the unbounded lifecycle queue (RESET / STOP_SENDING /
//! MAX_STREAM_DATA — no ordering constraint against unsent DATA, and per §6.5
//! never blocked behind a full DATA queue). Every DATA frame reserves §6.5
//! credit BEFORE entering the queue; the writer task commits the reservation
//! when the frame reaches the transport, or releases it when a reset discards
//! queued-but-unsent DATA.

use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::sync::{mpsc, Notify};
use tokio_util::sync::PollSender;
use webrtc::data_channel::data_channel_message::DataChannelMessage;
use webrtc::data_channel::data_channel_state::RTCDataChannelState;
use webrtc::data_channel::RTCDataChannel;

use crate::api::Stream;
use crate::error::{Error, ErrorCode, Result, StreamError};
use crate::flow::ConnFlow;
use crate::framing::{
    encode_code, encode_data, encode_fin, encode_max_stream_data, parse_frame, FrameType,
    ParsedFrame, MAX_FRAME_PAYLOAD,
};

/// The SCTP send-buffer level at which the writer task pauses. This is a LOCAL
/// queue bound only — flow control is the §6.5 credit reservation.
const WRITE_BUFFER_LOW: usize = 1 << 20; // 1 MiB

/// Frame-level tracing to stderr, enabled by setting KPS_DEBUG. For debugging
/// interop issues; resolved once.
fn kps_debug() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| std::env::var_os("KPS_DEBUG").is_some())
}

/// Hooks into the owning connection.
#[derive(Clone)]
pub(crate) struct StreamHooks {
    /// A wire violation by the peer: the whole connection must fail.
    pub(crate) fatal: Arc<dyn Fn(ErrorCode, String) + Send + Sync>,
    /// Fired once when the stream fully retires (wire-complete + channel
    /// closed + drained).
    pub(crate) retired: Arc<dyn Fn() + Send + Sync>,
    /// True while the connection is closing/failed (suppresses close policing).
    pub(crate) is_teardown: Arc<dyn Fn() -> bool + Send + Sync>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Terminal {
    Fin,
    Reset,
}

/// State shared between the data-channel callbacks and the poll fns.
pub(crate) struct Shared {
    state: Mutex<State>,
    open_notify: Notify,
    closed_notify: Notify,
    buf_low: Notify,
    flow: Arc<ConnFlow>,
    id: u64,
    hooks: StreamHooks,
    dc: Arc<RTCDataChannel>,
    /// Lifecycle/credit frames: unbounded so a full DATA queue never blocks
    /// them (§6.5).
    life_tx: mpsc::UnboundedSender<Vec<u8>>,
}

#[derive(Default)]
struct State {
    inbuf: VecDeque<Bytes>,
    peer_fin: bool,                  // peer FIN observed
    peer_reset: Option<StreamError>, // peer RESET observed
    peer_stop: Option<StreamError>,  // peer STOP_SENDING observed
    local_terminal: Option<Terminal>,
    local_terminal_sent: bool, // FIN/RESET actually handed to the transport
    read_cancel: bool,         // local cancel_read
    drop_data: bool,   // writer task discards queued-but-unsent DATA
    dc_open: bool,
    dc_closed: bool,
    retired_fired: bool,
    closed: bool,
    read_waker: Option<Waker>,
}

impl Shared {
    fn wake_reader(state: &mut State) {
        if let Some(w) = state.read_waker.take() {
            w.wake();
        }
    }

    /// Handles one inbound data-channel message (exactly one frame, SPEC §6.2).
    fn on_frame(&self, data: &[u8]) {
        let f = match parse_frame(data) {
            Ok(f) => f,
            Err(msg) => {
                (self.hooks.fatal)(ErrorCode::ProtocolError, msg);
                return;
            }
        };
        if kps_debug() {
            eprintln!("[kps-stream] frame in: {f:?}");
        }
        match f {
            ParsedFrame::Data(payload) => {
                let n = payload.len() as u64;
                {
                    let st = self.state.lock().unwrap();
                    if st.peer_fin || st.peer_reset.is_some() {
                        drop(st);
                        (self.hooks.fatal)(ErrorCode::ProtocolError, "DATA after terminal frame".into());
                        return;
                    }
                }
                if let Err(msg) = self.flow.on_data_received(self.id, n) {
                    (self.hooks.fatal)(ErrorCode::ProtocolError, msg);
                    return;
                }
                let cancelled = {
                    let mut st = self.state.lock().unwrap();
                    if st.read_cancel {
                        true
                    } else {
                        st.inbuf.push_back(Bytes::copy_from_slice(payload));
                        Self::wake_reader(&mut st);
                        false
                    }
                };
                if cancelled {
                    // In-flight DATA racing our STOP_SENDING: discard = consumed.
                    self.consumed(n);
                }
            }
            ParsedFrame::Fin => {
                {
                    let mut st = self.state.lock().unwrap();
                    if st.peer_fin || st.peer_reset.is_some() {
                        drop(st);
                        (self.hooks.fatal)(ErrorCode::ProtocolError, "second terminal frame".into());
                        return;
                    }
                    st.peer_fin = true;
                    Self::wake_reader(&mut st);
                }
                self.maybe_retire();
            }
            ParsedFrame::Reset(code) => {
                let discarded = {
                    let mut st = self.state.lock().unwrap();
                    if st.peer_fin || st.peer_reset.is_some() {
                        drop(st);
                        (self.hooks.fatal)(ErrorCode::ProtocolError, "second terminal frame".into());
                        return;
                    }
                    st.peer_reset = Some(StreamError { code, remote: true });
                    // QUIC-like reset: discard buffered-but-unread bytes
                    // (counts as consumed, releasing connection credit).
                    let n: u64 = st.inbuf.iter().map(|b| b.len() as u64).sum();
                    st.inbuf.clear();
                    Self::wake_reader(&mut st);
                    n
                };
                if discarded > 0 {
                    self.consumed(discarded);
                }
                self.maybe_retire();
            }
            ParsedFrame::StopSending(code) => {
                let auto_reset = {
                    let mut st = self.state.lock().unwrap();
                    if st.peer_stop.is_some() {
                        return; // duplicate: ignore
                    }
                    st.peer_stop = Some(StreamError { code, remote: true });
                    if st.local_terminal.is_none() {
                        // No terminal handed to the transport yet: reply with
                        // RESET and discard queued-but-unsent DATA (§6.2).
                        st.local_terminal = Some(Terminal::Reset);
                        st.drop_data = true;
                        true
                    } else {
                        false
                    }
                };
                self.flow.fail_send(self.id);
                self.buf_low.notify_waiters(); // unblock the writer task promptly
                if auto_reset {
                    let _ = self.life_tx.send(encode_code(FrameType::Reset, code));
                    self.maybe_retire();
                }
            }
            ParsedFrame::MaxStreamData(v) => {
                self.flow.on_peer_max_stream_data(self.id, v);
            }
        }
    }

    /// Consumption accounting: forwards to the flow engine and sends any due
    /// stream-level credit advertisement on this stream's channel.
    fn consumed(&self, n: u64) {
        if let Some(adv) = self.flow.on_consumed(self.id, n) {
            let _ = self.life_tx.send(encode_max_stream_data(adv));
        }
    }

    /// Drives the §6.5 retirement ladder: once wire-complete and locally
    /// drained we MUST initiate the channel close; once the channel has also
    /// closed, the stream retires (hooks.retired returns MAX_STREAMS credit
    /// for peer-initiated streams and drops flow state).
    fn maybe_retire(&self) {
        let (wire_complete, drained, dc_closed, fire) = {
            let mut st = self.state.lock().unwrap();
            // Wire-complete needs the local terminal frame ON THE WIRE (handed
            // to the transport by the writer task), not merely queued.
            let wire_complete =
                st.local_terminal_sent && (st.peer_fin || st.peer_reset.is_some());
            let drained = st.inbuf.is_empty();
            let dc_closed = st.dc_closed;
            let mut fire = false;
            if wire_complete && drained && dc_closed && !st.retired_fired {
                st.retired_fired = true;
                fire = true;
            }
            (wire_complete, drained, dc_closed, fire)
        };
        if !wire_complete || !drained {
            return;
        }
        if !dc_closed {
            // Let the terminal frame flush out of the SCTP send buffer before
            // resetting the stream (bounded so close stays prompt).
            let dc = self.dc.clone();
            tokio::spawn(async move {
                let deadline = tokio::time::Instant::now() + Duration::from_millis(250);
                while dc.buffered_amount().await > 0 && tokio::time::Instant::now() < deadline {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
                let _ = dc.close().await;
            });
            return;
        }
        if fire {
            self.flow.drop_stream(self.id);
            (self.hooks.retired)();
        }
    }

    fn on_channel_close(&self) {
        let wire_complete = {
            let mut st = self.state.lock().unwrap();
            st.dc_closed = true;
            Self::wake_reader(&mut st);
            st.local_terminal_sent && (st.peer_fin || st.peer_reset.is_some())
        };
        if !wire_complete && !(self.hooks.is_teardown)() {
            // §6.5 teardown accounting: a channel disappearing mid-stream
            // leaves connection credit ambiguous — connection-fatal.
            (self.hooks.fatal)(ErrorCode::ProtocolError, "data channel closed mid-stream".into());
        }
        self.flow.fail_send(self.id);
        self.open_notify.notify_waiters();
        self.buf_low.notify_waiters();
        self.mark_closed();
        self.maybe_retire();
    }

    /// Resolves when the data channel is open (Ok) or dead (Err).
    async fn wait_open(&self) -> Result<()> {
        loop {
            let notified = self.open_notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable(); // register BEFORE the check (lost-wakeup)
            {
                let st = self.state.lock().unwrap();
                if st.dc_open {
                    return Ok(());
                }
                if st.dc_closed {
                    return Err(Error::StreamClosed);
                }
            }
            notified.await;
        }
    }

    fn mark_closed(&self) {
        let mut st = self.state.lock().unwrap();
        if !st.closed {
            st.closed = true;
            self.closed_notify.notify_waiters();
        }
    }
}

pub(crate) struct WebrtcStream {
    shared: Arc<Shared>,
    /// Ordered DATA+FIN queue to the writer task; each entry carries its
    /// reserved credit (0 for FIN).
    data_poll_tx: PollSender<(Vec<u8>, u64)>,
    data_tx: mpsc::Sender<(Vec<u8>, u64)>,
    /// Credit granted by poll_reserve but not yet queued (held across Pending
    /// from the queue reservation).
    pending_grant: u64,
}

impl WebrtcStream {
    /// Wraps a data channel as a KPS stream. Registers all callbacks and
    /// spawns the writer task; safe to call before the channel opens.
    pub(crate) fn new(dc: Arc<RTCDataChannel>, flow: Arc<ConnFlow>, hooks: StreamHooks) -> Self {
        let id = flow.new_stream();
        let (life_tx, life_rx) = mpsc::unbounded_channel::<Vec<u8>>();
        let shared = Arc::new(Shared {
            state: Mutex::new(State::default()),
            open_notify: Notify::new(),
            closed_notify: Notify::new(),
            buf_low: Notify::new(),
            flow,
            id,
            hooks,
            dc: dc.clone(),
            life_tx,
        });

        if dc.ready_state() == RTCDataChannelState::Open {
            let mut st = shared.state.lock().unwrap();
            st.dc_open = true;
        }
        {
            let shared = shared.clone();
            dc.on_open(Box::new(move || {
                let shared = shared.clone();
                Box::pin(async move {
                    shared.state.lock().unwrap().dc_open = true;
                    shared.open_notify.notify_waiters();
                })
            }));
        }
        {
            let shared = shared.clone();
            dc.on_message(Box::new(move |msg: DataChannelMessage| {
                let shared = shared.clone();
                Box::pin(async move {
                    shared.on_frame(&msg.data);
                })
            }));
        }
        {
            let shared = shared.clone();
            dc.on_close(Box::new(move || {
                let shared = shared.clone();
                Box::pin(async move {
                    if kps_debug() {
                        eprintln!("[kps-stream] dc closed");
                    }
                    shared.on_channel_close();
                })
            }));
        }

        let (data_tx, data_rx) = mpsc::channel::<(Vec<u8>, u64)>(32);
        tokio::spawn(writer_task(dc, data_rx, life_rx, shared.clone()));

        Self {
            shared,
            data_poll_tx: PollSender::new(data_tx.clone()),
            data_tx,
            pending_grant: 0,
        }
    }

    /// Resolves when the data channel is open (used by open_stream).
    pub(crate) async fn wait_open(&self) -> Result<()> {
        self.shared.wait_open().await
    }
}

/// Sends queued frames, applying the LOCAL send-buffer bound (credit was
/// already reserved before frames entered the queue). Lifecycle frames take
/// priority — they are small, credit-exempt, and per §6.5 must never be
/// blocked behind DATA. When `drop_data` is set (reset / STOP_SENDING),
/// queued-but-unsent DATA is discarded and its reservation released.
async fn writer_task(
    dc: Arc<RTCDataChannel>,
    mut data_rx: mpsc::Receiver<(Vec<u8>, u64)>,
    mut life_rx: mpsc::UnboundedReceiver<Vec<u8>>,
    shared: Arc<Shared>,
) {
    dc.set_buffered_amount_low_threshold(WRITE_BUFFER_LOW).await;
    {
        let shared = shared.clone();
        dc.on_buffered_amount_low(Box::new(move || {
            let shared = shared.clone();
            Box::pin(async move {
                shared.buf_low.notify_waiters();
            })
        }))
        .await;
    }

    if shared.wait_open().await.is_err() {
        return;
    }
    let send = |frame: Vec<u8>| {
        let dc = dc.clone();
        async move {
            let kind = frame.first().copied();
            match dc.send(&Bytes::from(frame)).await {
                Ok(n) => {
                    if kps_debug() {
                        eprintln!("[kps-stream] frame out: type={kind:?} sent={n}");
                    }
                    true
                }
                Err(e) => {
                    if kps_debug() {
                        eprintln!("[kps-stream] frame out FAILED: type={kind:?} err={e}");
                    }
                    false
                }
            }
        }
    };
    // A successfully-sent FIN/RESET makes the local write half terminal ON THE
    // WIRE — the condition §6.5 retirement needs.
    let mark_terminal_sent = |kind: Option<u8>| {
        if kind == Some(FrameType::Fin as u8) || kind == Some(FrameType::Reset as u8) {
            shared.state.lock().unwrap().local_terminal_sent = true;
            shared.maybe_retire();
        }
    };
    loop {
        tokio::select! {
            biased;
            life = life_rx.recv() => {
                let Some(frame) = life else { break };
                let kind = frame.first().copied();
                if !send(frame).await {
                    break;
                }
                mark_terminal_sent(kind);
            }
            data = data_rx.recv() => {
                let Some((frame, reserved)) = data else { break };
                let kind = frame.first().copied();
                let dropping = shared.state.lock().unwrap().drop_data;
                if dropping && kind == Some(FrameType::Data as u8) {
                    if reserved > 0 {
                        shared.flow.release(shared.id, reserved);
                    }
                    continue;
                }
                // Local send-buffer bound (not flow control).
                loop {
                    let notified = shared.buf_low.notified();
                    if shared.state.lock().unwrap().dc_closed {
                        if reserved > 0 { shared.flow.release(shared.id, reserved); }
                        return;
                    }
                    if dc.buffered_amount().await <= WRITE_BUFFER_LOW {
                        break;
                    }
                    let _ = tokio::time::timeout(Duration::from_millis(100), notified).await;
                }
                if send(frame).await {
                    if reserved > 0 {
                        shared.flow.commit(shared.id, reserved);
                    }
                    mark_terminal_sent(kind);
                } else {
                    if reserved > 0 {
                        shared.flow.release(shared.id, reserved);
                    }
                    break;
                }
            }
        }
    }
}

impl AsyncRead for WebrtcStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let consumed;
        {
            let mut st = self.shared.state.lock().unwrap();
            if let Some(chunk) = st.inbuf.front_mut() {
                let n = chunk.len().min(buf.remaining());
                buf.put_slice(&chunk[..n]);
                if n == chunk.len() {
                    st.inbuf.pop_front();
                } else {
                    let _ = chunk.split_to(n);
                }
                consumed = n as u64;
            } else {
                if st.read_cancel {
                    return Poll::Ready(Err(std::io::Error::other(Error::StreamClosed)));
                }
                if let Some(err) = st.peer_reset {
                    return Poll::Ready(Err(std::io::Error::other(err)));
                }
                if st.peer_fin {
                    return Poll::Ready(Ok(())); // EOF: no bytes written
                }
                if st.dc_closed {
                    return Poll::Ready(Err(std::io::Error::other(Error::StreamClosed)));
                }
                st.read_waker = Some(cx.waker().clone());
                return Poll::Pending;
            }
        }
        // Bytes handed to the application: consumption (§6.5) — advances the
        // receive counters and eventually re-advertises credit.
        self.shared.consumed(consumed);
        self.shared.maybe_retire();
        Poll::Ready(Ok(()))
    }
}

impl AsyncWrite for WebrtcStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        {
            let st = self.shared.state.lock().unwrap();
            if let Some(err) = st.peer_stop {
                return Poll::Ready(Err(std::io::Error::other(err)));
            }
            if st.local_terminal.is_some() {
                return Poll::Ready(Err(std::io::Error::other(Error::WriteClosed)));
            }
            if st.dc_closed {
                return Poll::Ready(Err(std::io::Error::other(Error::StreamClosed)));
            }
        }
        // Credit BEFORE the frame may enter the queue (§6.5). The grant may be
        // partial — frames split at the credit boundary as well as at
        // MAX_FRAME_PAYLOAD. A grant is held across a Pending queue
        // reservation (and released on failure or drop).
        if self.pending_grant == 0 {
            let want = buf.len().min(MAX_FRAME_PAYLOAD) as u64;
            match self.shared.flow.poll_reserve(cx, self.shared.id, want) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(e)) => return Poll::Ready(Err(std::io::Error::other(e))),
                Poll::Ready(Ok(granted)) => self.pending_grant = granted,
            }
        }
        let granted = self.pending_grant;
        match self.data_poll_tx.poll_reserve(cx) {
            Poll::Pending => Poll::Pending, // grant stays held for the next poll
            Poll::Ready(Err(_)) => {
                self.shared.flow.release(self.shared.id, granted);
                self.pending_grant = 0;
                Poll::Ready(Err(std::io::Error::other(Error::StreamClosed)))
            }
            Poll::Ready(Ok(())) => {
                let n = granted as usize;
                let item = (encode_data(&buf[..n]), granted);
                if self.data_poll_tx.send_item(item).is_err() {
                    self.shared.flow.release(self.shared.id, granted);
                    self.pending_grant = 0;
                    return Poll::Ready(Err(std::io::Error::other(Error::StreamClosed)));
                }
                self.pending_grant = 0;
                Poll::Ready(Ok(n))
            }
        }
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        // Frames are queued in order; SCTP handles delivery. Nothing to flush
        // at this layer.
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        // AsyncWrite::shutdown maps to a graceful FIN (close_write).
        {
            let mut st = self.shared.state.lock().unwrap();
            if st.local_terminal.is_some() {
                return Poll::Ready(Ok(()));
            }
            st.local_terminal = Some(Terminal::Fin);
        }
        match self.data_poll_tx.poll_reserve(cx) {
            Poll::Pending => {
                // Undo so a re-poll retries the reservation.
                self.shared.state.lock().unwrap().local_terminal = None;
                Poll::Pending
            }
            Poll::Ready(Err(_)) => Poll::Ready(Err(std::io::Error::other(Error::StreamClosed))),
            Poll::Ready(Ok(())) => {
                let _ = self.data_poll_tx.send_item((encode_fin(), 0));
                self.shared.flow.fail_send(self.shared.id);
                self.shared.maybe_retire();
                Poll::Ready(Ok(()))
            }
        }
    }
}

#[async_trait]
impl Stream for WebrtcStream {
    async fn close_write(&mut self) -> Result<()> {
        {
            let mut st = self.shared.state.lock().unwrap();
            if st.local_terminal.is_some() {
                return Ok(());
            }
            st.local_terminal = Some(Terminal::Fin);
        }
        self.shared.flow.fail_send(self.shared.id);
        // FIN rides the ordered DATA queue so it follows every accepted write.
        self.data_tx.send((encode_fin(), 0)).await.map_err(|_| Error::StreamClosed)?;
        self.shared.maybe_retire();
        Ok(())
    }

    async fn cancel_read(&mut self, code: ErrorCode) -> Result<()> {
        let (discarded, peer_terminal) = {
            let mut st = self.shared.state.lock().unwrap();
            if st.read_cancel {
                return Ok(());
            }
            st.read_cancel = true;
            let n: u64 = st.inbuf.iter().map(|b| b.len() as u64).sum();
            st.inbuf.clear();
            Shared::wake_reader(&mut st);
            (n, st.peer_fin || st.peer_reset.is_some())
        };
        self.shared.flow.mark_cancelled(self.shared.id);
        if discarded > 0 {
            self.shared.consumed(discarded); // discard is consumption (§6.5)
        }
        if !peer_terminal {
            let _ = self.shared.life_tx.send(encode_code(FrameType::StopSending, code));
        }
        self.shared.maybe_retire();
        Ok(())
    }

    async fn reset_write(&mut self, code: ErrorCode) -> Result<()> {
        {
            let mut st = self.shared.state.lock().unwrap();
            if st.local_terminal.is_some() {
                return Ok(());
            }
            st.local_terminal = Some(Terminal::Reset);
            st.drop_data = true; // discard queued-but-unsent DATA (§6.2)
        }
        self.shared.flow.fail_send(self.shared.id);
        self.shared.buf_low.notify_waiters();
        let _ = self.shared.life_tx.send(encode_code(FrameType::Reset, code));
        self.shared.maybe_retire();
        Ok(())
    }

    /// Tears down both halves. The channel itself closes at retirement — once
    /// the peer's terminal frame (a conforming peer answers STOP_SENDING with
    /// RESET) has arrived — because closing it earlier is a §6.5 protocol
    /// violation.
    async fn close(&mut self) -> Result<()> {
        let _ = self.close_write().await;
        let _ = self.cancel_read(ErrorCode::Closed).await;
        Ok(())
    }

    async fn close_with_error(&mut self, code: ErrorCode) -> Result<()> {
        let _ = self.reset_write(code).await;
        let _ = self.cancel_read(code).await;
        Ok(())
    }

    async fn closed(&self) {
        loop {
            let notified = self.shared.closed_notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable(); // register BEFORE the check (lost-wakeup)
            if self.shared.state.lock().unwrap().closed {
                return;
            }
            notified.await;
        }
    }

    fn err(&self) -> Option<StreamError> {
        self.shared.state.lock().unwrap().peer_reset
    }
}

impl Drop for WebrtcStream {
    fn drop(&mut self) {
        if self.pending_grant > 0 {
            self.shared.flow.release(self.shared.id, self.pending_grant);
        }
    }
}
