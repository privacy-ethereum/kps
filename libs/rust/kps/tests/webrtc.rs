//! Rust↔Rust WebRTC integration: dial_webrtc → listener over the §6.2 framed
//! data channels — echo, concurrent streams, datagrams, close codes. Mirrors
//! the QUIC suite; together with it this exercises the single-port demux from
//! the WebRTC side.

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::{datagram_round_trip, echo_round_trip, start_echo_server, T};
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::time::timeout;

use kps::{Conn, ErrorCode, Identity, ListenOptions};

#[tokio::test]
async fn webrtc_echo() {
    let (_l, addr) = start_echo_server().await;
    let conn = timeout(T, kps::dial_webrtc(&addr)).await.unwrap().unwrap();
    let echoed = echo_round_trip(conn.as_ref(), b"hello kps over webrtc").await;
    assert_eq!(echoed, b"hello kps over webrtc");
    conn.close().await.unwrap();
}

#[tokio::test]
async fn webrtc_multi_stream() {
    let (_l, addr) = start_echo_server().await;
    let conn: Arc<dyn Conn> =
        Arc::from(timeout(T, kps::dial_webrtc(&addr)).await.unwrap().unwrap());
    let mut tasks = Vec::new();
    for i in 0..4 {
        let conn = conn.clone();
        tasks.push(tokio::spawn(async move {
            let payload = format!("webrtc-stream-{i}").into_bytes();
            let echoed = echo_round_trip(conn.as_ref(), &payload).await;
            assert_eq!(echoed, payload);
        }));
    }
    for t in tasks {
        t.await.unwrap();
    }
}

#[tokio::test]
async fn webrtc_large_payload() {
    let (_l, addr) = start_echo_server().await;
    let conn = timeout(T, kps::dial_webrtc(&addr)).await.unwrap().unwrap();
    // Exercises frame splitting (>16 KiB) and SCTP backpressure.
    let payload: Vec<u8> = (0..(1 << 20)).map(|i| (i % 249) as u8).collect(); // 1 MiB
    let echoed = echo_round_trip(conn.as_ref(), &payload).await;
    assert_eq!(echoed.len(), payload.len());
    assert_eq!(echoed, payload, "large echo must be byte-exact");
}

#[tokio::test]
async fn webrtc_datagram_echo() {
    let (_l, addr) = start_echo_server().await;
    let conn = timeout(T, kps::dial_webrtc(&addr)).await.unwrap().unwrap();
    assert!(datagram_round_trip(conn.as_ref(), b"kps webrtc datagram").await);
}

#[tokio::test]
async fn webrtc_datagram_too_large() {
    let (_l, addr) = start_echo_server().await;
    let conn = timeout(T, kps::dial_webrtc(&addr)).await.unwrap().unwrap();
    let huge = vec![0u8; 4096];
    match conn.send_datagram(&huge).await {
        Err(kps::Error::DatagramTooLarge(e)) => {
            assert!(e.max_datagram_payload_size > 0);
            assert!(e.max_datagram_payload_size < huge.len());
        }
        other => panic!("expected DatagramTooLarge, got {other:?}"),
    }
}

#[tokio::test]
async fn webrtc_stream_reset_code() {
    // reset_write(code) → peer's read side observes a stream error with the
    // code, via the §6.2 RESET frame.
    let listener = kps::listen(
        "127.0.0.1:0",
        ListenOptions { identity: Some(Identity::generate().unwrap()), ..Default::default() },
    )
    .await
    .unwrap();
    let addr = listener.address("127.0.0.1");

    let server_side = tokio::spawn(async move {
        let conn = listener.accept().await.unwrap();
        let mut stream = conn.accept_stream().await.unwrap();
        let mut buf = Vec::new();
        let read_res = timeout(T, stream.read_to_end(&mut buf)).await.unwrap();
        (read_res.err(), stream.err())
    });

    let conn = timeout(T, kps::dial_webrtc(&addr)).await.unwrap().unwrap();
    let mut stream = timeout(T, conn.open_stream()).await.unwrap().unwrap();
    stream.write_all(b"partial").await.unwrap();
    stream.reset_write(ErrorCode::Reset).await.unwrap();

    let (io_err, stream_err) = server_side.await.unwrap();
    assert!(io_err.is_some(), "peer read must fail, not EOF");
    let se = stream_err.expect("stream err() must carry the reset");
    assert_eq!(se.code, ErrorCode::Reset);
    assert!(se.remote);
}

#[tokio::test]
async fn webrtc_conn_close_code() {
    // close_with_error(code) travels as CONNECTION_CLOSE on the reserved
    // control channel (SPEC §8) and surfaces as the peer's close reason.
    let listener = kps::listen(
        "127.0.0.1:0",
        ListenOptions { identity: Some(Identity::generate().unwrap()), ..Default::default() },
    )
    .await
    .unwrap();
    let addr = listener.address("127.0.0.1");

    let server_side = tokio::spawn(async move {
        let conn = listener.accept().await.unwrap();
        timeout(T, conn.closed()).await.unwrap();
        conn.err()
    });

    let conn = timeout(T, kps::dial_webrtc(&addr)).await.unwrap().unwrap();
    let _ = timeout(T, conn.open_stream()).await.unwrap().unwrap();
    conn.close_with_error(ErrorCode::ProtocolError).await.unwrap();

    let err = server_side.await.unwrap();
    match err {
        Some(kps::Error::Stream(se)) => {
            assert_eq!(se.code, ErrorCode::ProtocolError);
            assert!(se.remote);
        }
        other => panic!("expected StreamError(ProtocolError), got {other:?}"),
    }
}

#[tokio::test]
async fn webrtc_bad_certhash_rejected() {
    // A corrupted certhash derives the wrong ICE pwd AND the wrong pinned
    // fingerprint, so the handshake can never complete — the dial must not
    // produce a connection.
    let (_l, addr) = start_echo_server().await;
    let mut chars: Vec<char> = addr.chars().collect();
    let n = chars.len();
    chars[n - 1] = if chars[n - 1] == 'A' { 'B' } else { 'A' };
    let bad_addr: String = chars.into_iter().collect();

    match timeout(Duration::from_secs(5), kps::dial_webrtc(&bad_addr)).await {
        Err(_elapsed) => {}  // timed out: never connected
        Ok(Err(_)) => {}     // failed outright
        Ok(Ok(_)) => panic!("dial must not succeed with a bad certhash"),
    }
}

#[tokio::test]
async fn single_port_both_transports() {
    // SPEC §10.3: a WebRTC client and a QUIC client on the same listener UDP
    // port, concurrently, one advertised address.
    let (_l, addr) = start_echo_server().await;

    let a1 = addr.clone();
    let quic_task = tokio::spawn(async move {
        let conn = timeout(T, kps::dial(&a1)).await.unwrap().unwrap();
        let echoed = echo_round_trip(conn.as_ref(), b"via-quic").await;
        assert_eq!(echoed, b"via-quic");
    });
    let a2 = addr.clone();
    let webrtc_task = tokio::spawn(async move {
        let conn = timeout(T, kps::dial_webrtc(&a2)).await.unwrap().unwrap();
        let echoed = echo_round_trip(conn.as_ref(), b"via-webrtc").await;
        assert_eq!(echoed, b"via-webrtc");
    });

    quic_task.await.unwrap();
    webrtc_task.await.unwrap();
}
