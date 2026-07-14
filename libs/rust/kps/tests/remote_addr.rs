//! Conn::remote_addr on both transports and both sides: the accept side sees
//! the client's UDP endpoint (per-IP policy, e.g. rate limits), the dial side
//! sees the dialed endpoint. Mirrors Go's TestRemoteAddr.

mod common;

use common::{start_echo_server, T};
use tokio::time::timeout;

async fn check(addr: &str, listener: &kps::Listener, webrtc: bool) {
    let dial = |a: String| async move {
        if webrtc { kps::dial_webrtc(&a).await } else { kps::dial(&a).await }
    };
    let client = timeout(T, dial(addr.to_string())).await.unwrap().unwrap();

    // Dial side: the dialed endpoint.
    let cr = client.remote_addr();
    assert!(cr.ip().is_loopback(), "client remote_addr {cr} should be loopback");
    assert_eq!(cr.port(), listener.port(), "client remote_addr {cr} should be the dialed port");

    // Keep the connection alive long enough for the echo server task to have
    // accepted it (the shared echo server consumes accept()).
    let _stream = timeout(T, client.open_stream()).await.unwrap().unwrap();
    client.close().await.unwrap();
}

#[tokio::test]
async fn remote_addr_dial_side() {
    let (listener, addr) = start_echo_server().await;
    check(&addr, &listener, false).await; // quic
    check(&addr, &listener, true).await; // webrtc
}

#[tokio::test]
async fn remote_addr_accept_side() {
    // A dedicated listener (not the echo helper) so the test owns accept().
    let listener = kps::listen(
        "127.0.0.1:0",
        kps::ListenOptions { identity: Some(kps::Identity::generate().unwrap()), ..Default::default() },
    )
    .await
    .unwrap();
    let addr = listener.address("127.0.0.1");

    for webrtc in [false, true] {
        let client = if webrtc {
            timeout(T, kps::dial_webrtc(&addr)).await.unwrap().unwrap()
        } else {
            timeout(T, kps::dial(&addr)).await.unwrap().unwrap()
        };
        let server = timeout(T, listener.accept()).await.unwrap().unwrap();

        // The client's source is a real local endpoint, but not necessarily
        // loopback: a WebRTC client gathers candidates on every interface, and
        // the winning pair may ride a LAN interface even for a 127.0.0.1 dial.
        let sr = server.remote_addr();
        assert!(!sr.ip().is_unspecified(), "server remote_addr {sr} should be concrete (webrtc={webrtc})");
        assert_ne!(sr.port(), 0, "server remote_addr {sr} should have a source port (webrtc={webrtc})");

        client.close().await.unwrap();
    }
}
