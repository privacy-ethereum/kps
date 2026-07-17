//! End-to-end flow control for the WebRTC mapping (SPEC §6.5): the credit
//! engine. One [`ConnFlow`] per connection tracks both directions of
//! connection-level credit plus stream-count credit; per-stream state lives in
//! the same mutex (reservations must be atomic across streams anyway) and is
//! addressed by a stream id.
//!
//! Sender side: `sent + reserved + n ≤ peer_max` at both levels, reserved
//! atomically BEFORE a DATA frame may be queued; a blocked reservation waits
//! for peer credit (poll-based, so `AsyncWrite::poll_write` can participate).
//! Grants may be partial (a write larger than the whole window splits at the
//! window boundary, like a QUIC sender). Receiver side: `received + n ≤
//! local_max` enforced before buffering; consumption (read fulfilled or
//! explicit discard) advances counters and re-advertises credit once half a
//! window is consumed-but-unadvertised.

use std::collections::HashMap;
use std::sync::Mutex;
use std::task::{Poll, Waker};

use tokio::sync::{mpsc, Notify};

use crate::error::{Error, StreamError};
use crate::framing::{encode_max_data, encode_max_streams, MAX_OFFSET};

/// A receiver's initial windows, announced in HELLO.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct FlowLimits {
    pub max_stream_data: u64,
    pub max_data: u64,
    pub max_streams: u64,
}

/// Recommended initial windows (SPEC §6.5) — receiver policy, not protocol
/// constants.
pub(crate) const DEFAULT_FLOW_LIMITS: FlowLimits =
    FlowLimits { max_stream_data: 1 << 20, max_data: 8 << 20, max_streams: 100 };

fn sat(v: u64) -> u64 {
    v.min(MAX_OFFSET)
}

#[derive(Default)]
struct StreamState {
    // sender side
    send_sent: u64,
    send_reserved: u64,
    peer_max_explicit: u64, // largest MAX_STREAM_DATA received on this stream
    send_failed: bool,
    // receiver side
    local_max: u64, // enforcement limit
    received: u64,
    consumed: u64,
    advertised_at: u64,
    cancelled: bool, // local cancel_read: no further stream credit
}

struct FlowState {
    failed: Option<StreamError>,
    is_failed: bool,

    // sender side (peer-granted; zero until the peer's HELLO)
    peer_init_stream_data: u64,
    peer_max_data: u64,
    peer_max_streams: u64,
    conn_sent: u64,
    conn_reserved: u64,
    streams_opened: u64,
    streams_reserved: u64,

    // receiver side
    local_max_data: u64, // enforcement limit (advances at commit-to-send)
    conn_received: u64,
    conn_consumed: u64,
    conn_advertised_at: u64,
    peer_opened_streams: u64,
    peer_retired_streams: u64,
    advertised_max_streams: u64,

    streams: HashMap<u64, StreamState>,
    next_id: u64,
    wakers: Vec<Waker>, // blocked poll_reserve callers
}

pub(crate) struct ConnFlow {
    pub(crate) local: FlowLimits,
    state: Mutex<FlowState>,
    notify: Notify, // async waiters (stream-slot reservation)
    /// Connection-level credit advertisements (MAX_DATA / MAX_STREAMS bytes)
    /// drain to the control channel via the conn's control-writer task.
    advert_tx: mpsc::UnboundedSender<Vec<u8>>,
}

impl ConnFlow {
    pub(crate) fn new(local: FlowLimits, advert_tx: mpsc::UnboundedSender<Vec<u8>>) -> Self {
        Self {
            local,
            state: Mutex::new(FlowState {
                failed: None,
                is_failed: false,
                peer_init_stream_data: 0,
                peer_max_data: 0,
                peer_max_streams: 0,
                conn_sent: 0,
                conn_reserved: 0,
                streams_opened: 0,
                streams_reserved: 0,
                local_max_data: local.max_data,
                conn_received: 0,
                conn_consumed: 0,
                conn_advertised_at: 0,
                peer_opened_streams: 0,
                peer_retired_streams: 0,
                advertised_max_streams: local.max_streams,
                streams: HashMap::new(),
                next_id: 0,
                wakers: Vec::new(),
            }),
            notify: Notify::new(),
            advert_tx,
        }
    }

    fn wake_locked(&self, st: &mut FlowState) {
        for w in st.wakers.drain(..) {
            w.wake();
        }
        self.notify.notify_waiters();
    }

    /// Fail every pending and future credit wait (connection teardown).
    pub(crate) fn fail(&self, err: Option<StreamError>) {
        let mut st = self.state.lock().unwrap();
        if !st.is_failed {
            st.is_failed = true;
            st.failed = err;
            self.wake_locked(&mut st);
        }
    }

    fn failure(st: &FlowState) -> Error {
        match st.failed {
            Some(se) => Error::Stream(se),
            None => Error::ConnClosed,
        }
    }

    /// The peer's HELLO: seed every send-side limit.
    pub(crate) fn on_peer_hello(&self, l: FlowLimits) {
        let mut st = self.state.lock().unwrap();
        st.peer_init_stream_data = l.max_stream_data;
        st.peer_max_data = l.max_data;
        st.peer_max_streams = l.max_streams;
        self.wake_locked(&mut st);
    }

    /// Peer raised a limit; decreases are ignored.
    pub(crate) fn on_peer_max_data(&self, v: u64) {
        let mut st = self.state.lock().unwrap();
        if v > st.peer_max_data {
            st.peer_max_data = v;
            self.wake_locked(&mut st);
        }
    }

    pub(crate) fn on_peer_max_streams(&self, v: u64) {
        let mut st = self.state.lock().unwrap();
        if v > st.peer_max_streams {
            st.peer_max_streams = v;
            self.wake_locked(&mut st);
        }
    }

    /// Register a new stream; the id addresses its state in every other call.
    pub(crate) fn new_stream(&self) -> u64 {
        let mut st = self.state.lock().unwrap();
        st.next_id += 1;
        let id = st.next_id;
        let local_max = self.local.max_stream_data;
        st.streams.insert(id, StreamState { local_max, ..Default::default() });
        id
    }

    /// Drop a stream's state (fully retired or connection teardown).
    pub(crate) fn drop_stream(&self, id: u64) {
        let mut st = self.state.lock().unwrap();
        if let Some(s) = st.streams.remove(&id) {
            // Unsent reservations die with the stream.
            st.conn_reserved -= s.send_reserved;
            self.wake_locked(&mut st);
        }
    }

    // ---- sender: byte credit ----

    /// Poll-reserve up to `want` DATA payload bytes at both levels. Ready with
    /// the granted amount (1..=want) once at least one byte of credit is
    /// available; Pending registers the waker for the next credit event.
    /// Errors when the stream's write half failed or the connection failed
    /// (callers re-check their own state for the precise error).
    pub(crate) fn poll_reserve(
        &self,
        cx: &mut std::task::Context<'_>,
        id: u64,
        want: u64,
    ) -> Poll<crate::error::Result<u64>> {
        let mut st = self.state.lock().unwrap();
        if st.is_failed {
            return Poll::Ready(Err(Self::failure(&st)));
        }
        let peer_init = st.peer_init_stream_data;
        let conn_avail = st.peer_max_data - st.conn_sent - st.conn_reserved;
        let Some(s) = st.streams.get_mut(&id) else {
            return Poll::Ready(Err(Error::StreamClosed));
        };
        if s.send_failed {
            return Poll::Ready(Err(Error::StreamClosed));
        }
        // Effective peer window: explicit updates never lower it below the
        // HELLO initial (streams created before the peer's HELLO start at 0
        // and see the window the moment it arrives).
        let peer_max = s.peer_max_explicit.max(peer_init);
        let stream_avail = peer_max - s.send_sent - s.send_reserved;
        let grant = stream_avail.min(conn_avail).min(want);
        if grant >= 1 {
            s.send_reserved += grant;
            st.conn_reserved += grant;
            return Poll::Ready(Ok(grant));
        }
        st.wakers.push(cx.waker().clone());
        Poll::Pending
    }

    /// Bytes passed to the transport: reserved → sent, both levels.
    pub(crate) fn commit(&self, id: u64, n: u64) {
        let mut st = self.state.lock().unwrap();
        if let Some(s) = st.streams.get_mut(&id) {
            s.send_reserved -= n;
            s.send_sent += n;
            st.conn_reserved -= n;
            st.conn_sent += n;
        }
    }

    /// A reserved-but-unsent frame was discarded: release its reservation.
    pub(crate) fn release(&self, id: u64, n: u64) {
        let mut st = self.state.lock().unwrap();
        if let Some(s) = st.streams.get_mut(&id) {
            s.send_reserved -= n;
            st.conn_reserved -= n;
        }
        self.wake_locked(&mut st);
    }

    /// Fail this stream's pending and future reservations (STOP_SENDING,
    /// reset, close); blocked writers wake and re-check their own state.
    pub(crate) fn fail_send(&self, id: u64) {
        let mut st = self.state.lock().unwrap();
        if let Some(s) = st.streams.get_mut(&id) {
            if !s.send_failed {
                s.send_failed = true;
                self.wake_locked(&mut st);
            }
        }
    }

    /// MAX_STREAM_DATA from the peer; decreases are ignored.
    pub(crate) fn on_peer_max_stream_data(&self, id: u64, v: u64) {
        let mut st = self.state.lock().unwrap();
        if let Some(s) = st.streams.get_mut(&id) {
            if v > s.peer_max_explicit {
                s.peer_max_explicit = v;
                self.wake_locked(&mut st);
            }
        }
    }

    // ---- sender: stream slots ----

    /// Reserve a slot to open one stream, waiting at the peer's limit.
    /// Connection failure rejects it (so it cannot hang past teardown).
    pub(crate) async fn reserve_stream_slot(&self) -> crate::error::Result<()> {
        loop {
            let notified = self.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable(); // register BEFORE the check (lost-wakeup)
            {
                let mut st = self.state.lock().unwrap();
                if st.is_failed {
                    return Err(Self::failure(&st));
                }
                if st.streams_opened + st.streams_reserved < st.peer_max_streams {
                    st.streams_reserved += 1;
                    return Ok(());
                }
            }
            notified.await;
        }
    }

    pub(crate) fn commit_stream_slot(&self) {
        let mut st = self.state.lock().unwrap();
        st.streams_reserved -= 1;
        st.streams_opened += 1;
    }

    pub(crate) fn release_stream_slot(&self) {
        let mut st = self.state.lock().unwrap();
        st.streams_reserved -= 1;
        self.wake_locked(&mut st);
    }

    // ---- receiver: byte credit ----

    /// Enforce both receive windows atomically before `n` inbound payload
    /// bytes may be buffered. Errors (with a violation description) when the
    /// peer exceeds either window.
    pub(crate) fn on_data_received(&self, id: u64, n: u64) -> Result<(), String> {
        let mut st = self.state.lock().unwrap();
        let local_max_data = st.local_max_data;
        let conn_received = st.conn_received;
        let Some(s) = st.streams.get_mut(&id) else {
            return Ok(()); // stream state already dropped (teardown race)
        };
        if s.received + n > s.local_max {
            return Err("peer exceeded MAX_STREAM_DATA".into());
        }
        if conn_received + n > local_max_data {
            return Err("peer exceeded MAX_DATA".into());
        }
        s.received += n;
        st.conn_received += n;
        Ok(())
    }

    /// Record `n` consumed bytes (read-fulfilled to the application or
    /// explicitly discarded) and advertise replacement credit past the
    /// half-window threshold. Returns the stream-level advertisement to send
    /// on the stream's channel, if due (stream credit is withheld after
    /// cancel_read); the connection-level advertisement is queued to the
    /// control channel internally. The local enforcement limits advance here —
    /// at commit-to-send — not when the peer acknowledges (§6.5).
    pub(crate) fn on_consumed(&self, id: u64, n: u64) -> Option<u64> {
        let mut st = self.state.lock().unwrap();
        let stream_window = self.local.max_stream_data;
        let mut stream_adv = None;
        if let Some(s) = st.streams.get_mut(&id) {
            s.consumed += n;
            if !s.cancelled && s.consumed - s.advertised_at >= stream_window / 2 {
                s.advertised_at = s.consumed;
                s.local_max = sat(s.consumed + stream_window);
                stream_adv = Some(s.local_max);
            }
        }
        st.conn_consumed += n;
        if st.conn_consumed - st.conn_advertised_at >= self.local.max_data / 2 {
            st.conn_advertised_at = st.conn_consumed;
            st.local_max_data = sat(st.conn_consumed + self.local.max_data);
            let _ = self.advert_tx.send(encode_max_data(st.local_max_data));
        }
        stream_adv
    }

    /// Local cancel_read: stop granting stream credit; discards still free
    /// MAX_DATA.
    pub(crate) fn mark_cancelled(&self, id: u64) {
        let mut st = self.state.lock().unwrap();
        if let Some(s) = st.streams.get_mut(&id) {
            s.cancelled = true;
        }
    }

    // ---- receiver: stream count ----

    /// Record an observed peer-initiated stream (it consumes a slot
    /// immediately, even unaccepted or pre-HELLO). Errors when the peer
    /// exceeds MAX_STREAMS.
    pub(crate) fn peer_stream_opened(&self) -> Result<(), String> {
        let mut st = self.state.lock().unwrap();
        if st.peer_opened_streams >= st.advertised_max_streams {
            return Err("peer exceeded MAX_STREAMS".into());
        }
        st.peer_opened_streams += 1;
        Ok(())
    }

    /// Grant a replacement slot for a retired peer-initiated stream.
    pub(crate) fn peer_stream_retired(&self) {
        let mut st = self.state.lock().unwrap();
        st.peer_retired_streams += 1;
        st.advertised_max_streams = sat(self.local.max_streams + st.peer_retired_streams);
        let _ = self.advert_tx.send(encode_max_streams(st.advertised_max_streams));
    }
}
