//! The WebRTC implementation of [`Stream`] (SPEC §6.2): a byte stream over one
//! SCTP data channel, framed with DATA/FIN/RESET/STOP_SENDING. The
//! data-channel label is a non-semantic implementation detail.

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
use crate::framing::{
    decode_code, encode_code, encode_data, encode_fin, FrameType, MAX_FRAME_PAYLOAD,
};

/// The SCTP send-buffer level at which a blocked write resumes; writes apply
/// backpressure above it (same value as the Go implementation).
const WRITE_BUFFER_LOW: usize = 1 << 20; // 1 MiB

/// Frame-level tracing to stderr, enabled by setting KPS_DEBUG. For debugging
/// interop issues; resolved once.
fn kps_debug() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| std::env::var_os("KPS_DEBUG").is_some())
}

/// State shared between the data-channel callbacks and the poll fns.
pub(crate) struct Shared {
    state: Mutex<State>,
    open_notify: Notify,
    closed_notify: Notify,
    buf_low: Notify,
}

#[derive(Default)]
struct State {
    inbuf: VecDeque<Bytes>,
    read_eof: bool,                  // peer FIN observed
    read_err: Option<StreamError>,   // peer RESET observed
    read_cancel: bool,               // local cancel_read
    write_closed: bool,              // local close_write/reset_write/close
    peer_stop: Option<StreamError>,  // peer STOP_SENDING observed
    dc_open: bool,
    dc_closed: bool,
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
        let Some(&first) = data.first() else { return };
        let Some(t) = FrameType::from_byte(first) else { return };
        let payload = &data[1..];
        if kps_debug() {
            eprintln!("[kps-stream] frame in: {t:?} len={}", payload.len());
        }
        let mut st = self.state.lock().unwrap();
        match t {
            FrameType::Data => {
                if st.read_cancel || st.read_eof || st.read_err.is_some() {
                    return; // dropping inbound after cancel/EOF/reset
                }
                if !payload.is_empty() {
                    st.inbuf.push_back(Bytes::copy_from_slice(payload));
                }
                Self::wake_reader(&mut st);
            }
            FrameType::Fin => {
                st.read_eof = true;
                Self::wake_reader(&mut st);
            }
            FrameType::Reset => {
                if st.read_err.is_none() {
                    st.read_err = Some(StreamError { code: decode_code(payload), remote: true });
                }
                Self::wake_reader(&mut st);
            }
            FrameType::StopSending => {
                if st.peer_stop.is_none() {
                    st.peer_stop = Some(StreamError { code: decode_code(payload), remote: true });
                }
                st.write_closed = true;
            }
        }
    }

    /// Resolves when the data channel is open (Ok) or dead (Err).
    async fn wait_open(&self) -> Result<()> {
        loop {
            let notified = self.open_notify.notified();
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
    dc: Arc<RTCDataChannel>,
    shared: Arc<Shared>,
    /// Frames queued in-order to the writer task (DATA/FIN/RESET/STOP_SENDING
    /// all travel this path, so lifecycle frames stay ordered behind data).
    frame_poll_tx: PollSender<Vec<u8>>,
    frame_tx: mpsc::Sender<Vec<u8>>,
}

impl WebrtcStream {
    /// Wraps a data channel as a KPS stream. Registers all callbacks and spawns
    /// the writer task; safe to call before the channel opens.
    pub(crate) fn new(dc: Arc<RTCDataChannel>) -> Self {
        let shared = Arc::new(Shared {
            state: Mutex::new(State::default()),
            open_notify: Notify::new(),
            closed_notify: Notify::new(),
            buf_low: Notify::new(),
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
                    {
                        let mut st = shared.state.lock().unwrap();
                        st.dc_closed = true;
                        if !st.read_eof && st.read_err.is_none() {
                            st.read_eof = true; // unexpected close reads as EOF
                        }
                        Shared::wake_reader(&mut st);
                    }
                    shared.open_notify.notify_waiters();
                    shared.buf_low.notify_waiters();
                    shared.mark_closed();
                })
            }));
        }

        let (frame_tx, frame_rx) = mpsc::channel::<Vec<u8>>(32);
        tokio::spawn(writer_task(dc.clone(), frame_rx, shared.clone()));

        Self { dc, shared, frame_poll_tx: PollSender::new(frame_tx.clone()), frame_tx }
    }

    /// Resolves when the data channel is open (used by open_stream).
    pub(crate) async fn wait_open(&self) -> Result<()> {
        self.shared.wait_open().await
    }
}

/// Sends queued frames in order, applying SCTP backpressure: while the send
/// buffer is above the threshold, wait for on_buffered_amount_low (with a
/// timeout guard against missed notifications).
async fn writer_task(dc: Arc<RTCDataChannel>, mut rx: mpsc::Receiver<Vec<u8>>, shared: Arc<Shared>) {
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
    while let Some(frame) = rx.recv().await {
        loop {
            let notified = shared.buf_low.notified();
            if shared.state.lock().unwrap().dc_closed {
                return;
            }
            if dc.buffered_amount().await <= WRITE_BUFFER_LOW {
                break;
            }
            let _ = tokio::time::timeout(Duration::from_millis(100), notified).await;
        }
        let kind = frame.first().copied();
        match dc.send(&Bytes::from(frame)).await {
            Ok(n) => {
                if kps_debug() {
                    eprintln!("[kps-stream] frame out: type={kind:?} sent={n}");
                }
            }
            Err(e) => {
                if kps_debug() {
                    eprintln!("[kps-stream] frame out FAILED: type={kind:?} err={e}");
                }
                return;
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
        let mut st = self.shared.state.lock().unwrap();
        if let Some(chunk) = st.inbuf.front_mut() {
            let n = chunk.len().min(buf.remaining());
            buf.put_slice(&chunk[..n]);
            if n == chunk.len() {
                st.inbuf.pop_front();
            } else {
                let _ = chunk.split_to(n);
            }
            return Poll::Ready(Ok(()));
        }
        if st.read_cancel {
            return Poll::Ready(Err(std::io::Error::other(Error::StreamClosed)));
        }
        if let Some(err) = st.read_err {
            return Poll::Ready(Err(std::io::Error::other(err)));
        }
        if st.read_eof {
            return Poll::Ready(Ok(())); // EOF: no bytes written
        }
        st.read_waker = Some(cx.waker().clone());
        Poll::Pending
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
            if st.write_closed {
                return Poll::Ready(Err(std::io::Error::other(Error::WriteClosed)));
            }
            if st.dc_closed {
                return Poll::Ready(Err(std::io::Error::other(Error::StreamClosed)));
            }
        }
        match self.frame_poll_tx.poll_reserve(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Err(_)) => Poll::Ready(Err(std::io::Error::other(Error::StreamClosed))),
            Poll::Ready(Ok(())) => {
                let n = buf.len().min(MAX_FRAME_PAYLOAD);
                if self.frame_poll_tx.send_item(encode_data(&buf[..n])).is_err() {
                    return Poll::Ready(Err(std::io::Error::other(Error::StreamClosed)));
                }
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
            if st.write_closed {
                return Poll::Ready(Ok(()));
            }
            st.write_closed = true;
        }
        match self.frame_poll_tx.poll_reserve(cx) {
            Poll::Pending => {
                // Undo so a re-poll retries the reservation.
                self.shared.state.lock().unwrap().write_closed = false;
                Poll::Pending
            }
            Poll::Ready(Err(_)) => Poll::Ready(Err(std::io::Error::other(Error::StreamClosed))),
            Poll::Ready(Ok(())) => {
                let _ = self.frame_poll_tx.send_item(encode_fin());
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
            if st.write_closed {
                return Ok(());
            }
            st.write_closed = true;
        }
        self.frame_tx.send(encode_fin()).await.map_err(|_| Error::StreamClosed)?;
        Ok(())
    }

    async fn cancel_read(&mut self, code: ErrorCode) -> Result<()> {
        {
            let mut st = self.shared.state.lock().unwrap();
            if st.read_cancel {
                return Ok(());
            }
            st.read_cancel = true;
            st.inbuf.clear();
            Shared::wake_reader(&mut st);
        }
        self.frame_tx
            .send(encode_code(FrameType::StopSending, code))
            .await
            .map_err(|_| Error::StreamClosed)?;
        Ok(())
    }

    async fn reset_write(&mut self, code: ErrorCode) -> Result<()> {
        {
            let mut st = self.shared.state.lock().unwrap();
            if st.write_closed {
                return Ok(());
            }
            st.write_closed = true;
        }
        self.frame_tx
            .send(encode_code(FrameType::Reset, code))
            .await
            .map_err(|_| Error::StreamClosed)?;
        Ok(())
    }

    async fn close(&mut self) -> Result<()> {
        let _ = self.close_write().await;
        let _ = self.cancel_read(ErrorCode::Closed).await;
        // Let the queued FIN drain before tearing the channel down.
        self.drain_then_close().await;
        Ok(())
    }

    async fn close_with_error(&mut self, code: ErrorCode) -> Result<()> {
        let _ = self.reset_write(code).await;
        let _ = self.cancel_read(code).await;
        self.drain_then_close().await;
        Ok(())
    }

    async fn closed(&self) {
        loop {
            let notified = self.shared.closed_notify.notified();
            if self.shared.state.lock().unwrap().closed {
                return;
            }
            notified.await;
        }
    }

    fn err(&self) -> Option<StreamError> {
        self.shared.state.lock().unwrap().read_err
    }
}

impl WebrtcStream {
    /// Waits briefly for queued frames (FIN/RESET) to reach SCTP, then closes
    /// the channel. Bounded so close stays prompt.
    async fn drain_then_close(&self) {
        let deadline = tokio::time::Instant::now() + Duration::from_millis(250);
        while self.frame_tx.capacity() < self.frame_tx.max_capacity()
            && tokio::time::Instant::now() < deadline
        {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        while self.dc.buffered_amount().await > 0 && tokio::time::Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        self.shared.mark_closed();
        let _ = self.dc.close().await;
    }
}
