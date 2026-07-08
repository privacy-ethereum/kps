//! Shared fixtures for the integration tests: an in-process echo listener and
//! a full-duplex echo assertion.

use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::time::timeout;

use kps::{Conn, Identity, ListenOptions};

pub const T: Duration = Duration::from_secs(15);

/// Starts a listener that echoes every stream (mirror bytes until EOF, then
/// finish the write half) and echoes datagrams — both transports, one port.
pub async fn start_echo_server() -> (Arc<kps::Listener>, String) {
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
pub async fn echo_round_trip(conn: &dyn Conn, payload: &[u8]) -> Vec<u8> {
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

/// Sends a datagram and waits for its echo, retrying (datagrams are
/// best-effort and the channel may still be opening).
pub async fn datagram_round_trip(conn: &dyn Conn, payload: &[u8]) -> bool {
    for _ in 0..20 {
        if conn.send_datagram(payload).await.is_err() {
            tokio::time::sleep(Duration::from_millis(50)).await;
            continue;
        }
        match timeout(Duration::from_millis(500), conn.receive_datagram()).await {
            Ok(Ok(d)) if d == payload => return true,
            _ => continue,
        }
    }
    false
}
