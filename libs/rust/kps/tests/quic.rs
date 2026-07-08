//! Rust↔Rust QUIC integration: dial → stream → echo, datagrams, certhash
//! pinning, and connection close codes. Mirrors libs/go's quic_test.go /
//! conn_close_test.go and the JS integration suite.

use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::time::timeout;

use kps::{Conn, ErrorCode, Identity, ListenOptions};

const T: Duration = Duration::from_secs(10);

/// Starts a listener that echoes every stream (mirror bytes until EOF, then
/// finish the write half) and echoes datagrams.
async fn start_echo_server() -> (Arc<kps::Listener>, String) {
    let listener = kps::listen(
        "127.0.0.1:0",
        ListenOptions { identity: Some(Identity::generate().unwrap()), ..Default::default() },
    )
    .await
    .unwrap();
    let listener = Arc::new(listener);
    let addr = listener.address("127.0.0.1");

    let l = listener.clone();
    tokio::spawn(async move {
        while let Ok(conn) = l.accept().await {
            tokio::spawn(async move {
                let conn: Arc<dyn Conn> = Arc::from(conn);
                // stream echo
                let c = conn.clone();
                tokio::spawn(async move {
                    while let Ok(stream) = c.accept_stream().await {
                        tokio::spawn(async move {
                            let (mut rd, mut wr) = tokio::io::split(stream);
                            let _ = tokio::io::copy(&mut rd, &mut wr).await;
                            let _ = wr.shutdown().await; // FIN: peer sees EOF
                        });
                    }
                });
                // datagram echo
                while let Ok(d) = conn.receive_datagram().await {
                    let _ = conn.send_datagram(&d).await;
                }
            });
        }
    });

    (listener, addr)
}

/// Full-duplex echo assertion: write + FIN concurrently with draining, so
/// payloads larger than a flow-control window can't deadlock.
async fn echo_round_trip(conn: &dyn Conn, payload: &[u8]) -> Vec<u8> {
    let stream = timeout(T, conn.open_stream()).await.unwrap().unwrap();
    let (mut rd, mut wr) = tokio::io::split(stream);
    let (_, out) = tokio::join!(
        async {
            timeout(T, wr.write_all(payload)).await.unwrap().unwrap();
            timeout(T, wr.shutdown()).await.unwrap().unwrap();
        },
        async {
            let mut out = Vec::with_capacity(payload.len());
            timeout(T, rd.read_to_end(&mut out)).await.unwrap().unwrap();
            out
        }
    );
    out
}

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
