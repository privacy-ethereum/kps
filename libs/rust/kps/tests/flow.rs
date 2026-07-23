//! Flow-control semantics over a real Rust↔Rust WebRTC connection (SPEC §6.5).
//! Echo tests can't catch backpressure regressions — these use a non-reading
//! receiver. Windows use the default limits (1 MiB stream / 8 MiB conn / 100
//! streams).

use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::time::timeout;

use kps::{Conn, Identity, ListenOptions};

const T: Duration = Duration::from_secs(20);
const STREAM_WINDOW: usize = 1 << 20; // default initialMaxStreamData
const MAX_STREAMS: usize = 100; // default initialMaxStreams

async fn webrtc_pair() -> (Arc<kps::Listener>, Box<dyn Conn>, Box<dyn Conn>) {
    let listener = kps::listen(
        "127.0.0.1:0",
        ListenOptions { identity: Some(Identity::generate().unwrap()), ..Default::default() },
    )
    .await
    .unwrap();
    let listener = Arc::new(listener);
    let addr = listener.address("127.0.0.1");
    let l = listener.clone();
    let accept = tokio::spawn(async move { l.accept().await });
    let client = timeout(T, kps::dial_webrtc(&addr)).await.unwrap().unwrap();
    let server = timeout(T, accept).await.unwrap().unwrap().unwrap();
    (listener, client, server)
}

/// A sender must block at the receiver's advertised windows while the
/// receiving application does not read, and resume once it does.
#[tokio::test]
async fn backpressure_non_reading_receiver() {
    let (_l, client, server) = webrtc_pair().await;

    let cs = timeout(T, client.open_stream()).await.unwrap().unwrap();
    let ss = timeout(T, server.accept_stream()).await.unwrap().unwrap();

    // More than initialMaxStreamData: the tail must wait for credit.
    let payload = vec![0xABu8; STREAM_WINDOW + (64 << 10)];
    let (mut _rd, mut wr) = tokio::io::split(cs);
    let p2 = payload.clone();
    let write = tokio::spawn(async move {
        wr.write_all(&p2).await?;
        wr.shutdown().await?;
        Ok::<(), std::io::Error>(())
    });

    tokio::time::sleep(Duration::from_secs(2)).await;
    assert!(!write.is_finished(), "write completed without the receiver reading — no backpressure");

    // Reading drains the buffer, returns credit, and completes the write.
    let (mut rd, _wr) = tokio::io::split(ss);
    let mut got = Vec::with_capacity(payload.len());
    timeout(T, rd.read_to_end(&mut got)).await.unwrap().unwrap();
    assert_eq!(got, payload);
    timeout(T, write).await.unwrap().unwrap().unwrap();
}

/// open_stream must wait at the peer's MAX_STREAMS limit and resume when a
/// stream retires (both halves terminal, channel closed, drained).
#[tokio::test]
async fn stream_limit_blocks_and_retires() {
    let (_l, client, server) = webrtc_pair().await;

    let mut streams = Vec::with_capacity(MAX_STREAMS);
    for i in 0..MAX_STREAMS {
        let s = timeout(T, client.open_stream())
            .await
            .unwrap_or_else(|_| panic!("open {i} timed out"))
            .unwrap();
        streams.push(s);
    }

    // The 101st open must block at the limit.
    assert!(
        timeout(Duration::from_millis(1500), client.open_stream()).await.is_err(),
        "open beyond MAX_STREAMS should block"
    );

    // Retire one stream: close both ends and drain.
    let mut ss = timeout(T, server.accept_stream()).await.unwrap().unwrap();
    let mut cs = streams.remove(0);
    let _ = cs.close().await;
    let _ = ss.close().await;

    let s = timeout(T, client.open_stream()).await.expect("open after retirement").unwrap();
    drop(s);
}

/// Open 2×MAX_STREAMS streams, closing each, over one WebRTC connection. Only
/// MAX_STREAMS may be open at once, so completing twice that many requires the
/// peer's stream slots to be reclaimed on retirement and re-granted
/// (MAX_STREAMS) repeatedly, not just once. A slot leak (a retired stream that
/// never returns its slot) would let the first MAX_STREAMS opens through and
/// then block open_stream forever, tripping the timeout. The block-and-retire
/// test above exercises only a single reclaim; this exercises sustained
/// recycling.
///
/// NOTE: webrtc-rs allocates SCTP stream ids monotonically, so this does NOT
/// exercise stream-id *reuse*. Browsers free and reuse low ids as channels
/// close, its own hazard (kps#4) — covered only by the Playwright browser leg.
#[tokio::test]
async fn stream_slots_recycle_across_many_streams() {
    let (_l, client, server) = webrtc_pair().await;

    // Server: echo every stream (copy input back, then FIN), forever.
    tokio::spawn(async move {
        while let Ok(s) = server.accept_stream().await {
            tokio::spawn(async move {
                let (mut rd, mut wr) = tokio::io::split(s);
                let _ = tokio::io::copy(&mut rd, &mut wr).await;
                let _ = wr.shutdown().await;
            });
        }
    });

    let total = 2 * MAX_STREAMS;
    for i in 0..total {
        // Past MAX_STREAMS this can only proceed if an earlier slot was reclaimed.
        let s = timeout(T, client.open_stream())
            .await
            .unwrap_or_else(|_| panic!("open {i}/{total} timed out (slot not reclaimed?)"))
            .unwrap();
        let (mut rd, mut wr) = tokio::io::split(s);
        let msg = format!("req-{i}").into_bytes();
        timeout(T, wr.write_all(&msg)).await.unwrap().unwrap();
        timeout(T, wr.shutdown()).await.unwrap().unwrap(); // FIN
        let mut got = Vec::new();
        timeout(T, rd.read_to_end(&mut got)).await.unwrap().unwrap(); // echo + peer FIN → retire
        assert_eq!(got, msg, "echo {i} mismatch");
    }
}
