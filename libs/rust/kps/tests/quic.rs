//! Rust↔Rust QUIC integration: dial → stream → echo, datagrams, certhash
//! pinning, and connection close codes. Mirrors libs/go's quic_test.go /
//! conn_close_test.go and the JS integration suite.

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::{echo_round_trip, start_echo_server, T};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::time::timeout;

use kps::{Conn, ErrorCode, Identity, ListenOptions};

#[tokio::test]
async fn quic_echo() {
    let (_l, addr) = start_echo_server().await;
    let conn = timeout(T, kps::dial(&addr)).await.unwrap().unwrap();
    let echoed = echo_round_trip(conn.as_ref(), b"hello kps over quic").await;
    assert_eq!(echoed, b"hello kps over quic");
    conn.close().await.unwrap();
}

#[tokio::test]
async fn quic_multi_stream() {
    let (_l, addr) = start_echo_server().await;
    let conn: Arc<dyn Conn> = Arc::from(timeout(T, kps::dial(&addr)).await.unwrap().unwrap());
    let mut tasks = Vec::new();
    for i in 0..8 {
        let conn = conn.clone();
        tasks.push(tokio::spawn(async move {
            let payload = format!("stream-{i}-payload").into_bytes();
            let echoed = echo_round_trip(conn.as_ref(), &payload).await;
            assert_eq!(echoed, payload);
        }));
    }
    for t in tasks {
        t.await.unwrap();
    }
}

/// Open+close far more streams than the QUIC concurrent-stream budget (~100 by
/// default) over one connection. Passes only if each retired stream frees its
/// slot so the budget recycles; a lifecycle leak (a stream not closed under the
/// hood) would exhaust it and block open_stream. QUIC delegates the slot budget
/// to quinn; this guards KPS's stream open/close/retire lifecycle over it.
#[tokio::test]
async fn quic_stream_cycling() {
    let (_l, addr) = start_echo_server().await;
    let conn = timeout(T, kps::dial(&addr)).await.unwrap().unwrap();
    const N: usize = 200;
    for i in 0..N {
        let msg = format!("cycle-{i}").into_bytes();
        let echoed = echo_round_trip(conn.as_ref(), &msg).await;
        assert_eq!(echoed, msg, "stream {i}");
    }
    conn.close().await.unwrap();
}

#[tokio::test]
async fn quic_large_payload() {
    let (_l, addr) = start_echo_server().await;
    let conn = timeout(T, kps::dial(&addr)).await.unwrap().unwrap();
    let payload: Vec<u8> = (0..(4 << 20)).map(|i| (i % 251) as u8).collect(); // 4 MiB
    let echoed = echo_round_trip(conn.as_ref(), &payload).await;
    assert_eq!(echoed.len(), payload.len());
    assert_eq!(echoed, payload, "large echo must be byte-exact");
}

#[tokio::test]
async fn quic_datagram_echo() {
    let (_l, addr) = start_echo_server().await;
    let conn = timeout(T, kps::dial(&addr)).await.unwrap().unwrap();
    // Datagrams are best-effort; retry a few times.
    let payload = b"kps datagram".to_vec();
    for _ in 0..10 {
        conn.send_datagram(&payload).await.unwrap();
        match timeout(Duration::from_secs(1), conn.receive_datagram()).await {
            Ok(Ok(d)) => {
                assert_eq!(d, payload);
                return;
            }
            _ => continue,
        }
    }
    panic!("no datagram echoed after 10 attempts");
}

#[tokio::test]
async fn quic_datagram_too_large() {
    let (_l, addr) = start_echo_server().await;
    let conn = timeout(T, kps::dial(&addr)).await.unwrap().unwrap();
    let huge = vec![0u8; 1 << 20];
    match conn.send_datagram(&huge).await {
        Err(kps::Error::DatagramTooLarge(e)) => {
            assert!(e.max_datagram_payload_size > 0);
            assert!(e.max_datagram_payload_size < huge.len());
        }
        other => panic!("expected DatagramTooLarge, got {other:?}"),
    }
}

#[tokio::test]
async fn quic_bad_certhash_rejected() {
    let (_l, addr) = start_echo_server().await;
    // Corrupt the certhash: flip the last character of the digest part.
    let mut chars: Vec<char> = addr.chars().collect();
    let n = chars.len();
    chars[n - 1] = if chars[n - 1] == 'A' { 'B' } else { 'A' };
    let bad_addr: String = chars.into_iter().collect();

    match timeout(T, kps::dial(&bad_addr)).await.unwrap() {
        Err(_) => {}
        Ok(_) => panic!("dial must reject a certhash mismatch"),
    }
}

#[tokio::test]
async fn quic_stream_reset_code() {
    // reset_write(code) surfaces at the peer's read side as a stream error
    // carrying the code (SPEC §9).
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

    let conn = timeout(T, kps::dial(&addr)).await.unwrap().unwrap();
    let mut stream = timeout(T, conn.open_stream()).await.unwrap().unwrap();
    stream.write_all(b"partial").await.unwrap();
    stream.flush().await.unwrap();
    stream.reset_write(ErrorCode::Reset).await.unwrap();

    let (io_err, stream_err) = server_side.await.unwrap();
    assert!(io_err.is_some(), "peer read must fail, not EOF");
    let se = stream_err.expect("stream err() must carry the reset");
    assert_eq!(se.code, ErrorCode::Reset);
    assert!(se.remote);
}

#[tokio::test]
async fn quic_conn_close_code() {
    // Port of Go's TestConnCloseCode: close_with_error(ProtocolError) surfaces
    // at the peer as err() = StreamError{code: ProtocolError, remote: true}.
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

    let conn = timeout(T, kps::dial(&addr)).await.unwrap().unwrap();
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
