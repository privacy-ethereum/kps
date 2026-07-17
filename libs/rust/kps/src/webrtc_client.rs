//! The WebRTC dialer (SPEC §5.2). Native clients default to QUIC
//! ([`crate::dial`]); this is the explicit override the spec allows for
//! tests/debugging and for interop with browser-facing listeners (SPEC §5.4).
//! It mirrors the browser client: the offerer synthesizes the server's answer
//! from the address, derives the ICE password from the certhash, and pins the
//! server's DTLS certificate via the answer fingerprint.

use std::sync::Arc;

use webrtc::api::setting_engine::SettingEngine;
use webrtc::api::APIBuilder;
use webrtc::data_channel::data_channel_init::RTCDataChannelInit;
use webrtc::ice::network_type::NetworkType;
use webrtc::peer_connection::configuration::RTCConfiguration;
use webrtc::peer_connection::peer_connection_state::RTCPeerConnectionState;
use webrtc::peer_connection::sdp::session_description::RTCSessionDescription;

use crate::address::{decode_certhash, parse_address, Address};
use crate::api::Conn;
use crate::conn::WebrtcConn;
use crate::error::{Error, Result};
use crate::ice::{derive_ice_pwd, rand_ufrag};

/// Opens a kps connection over the WebRTC transport. The server's DTLS
/// certificate is pinned via the synthesized answer's fingerprint (= the
/// address certhash); no signaling server is involved. Apply timeouts
/// caller-side with `tokio::time::timeout`.
pub async fn dial_webrtc(addr: &str) -> Result<Box<dyn Conn>> {
    let a = parse_address(addr)?;
    let digest = decode_certhash(&a.certhash)?;
    let ufrag = rand_ufrag();
    let pwd = derive_ice_pwd(&digest, &ufrag);

    let mut se = SettingEngine::default();
    // Force local ICE creds (the server derives the same pwd from the certhash).
    se.set_ice_credentials(ufrag.clone(), pwd.clone());
    se.set_network_types(vec![NetworkType::Udp4, NetworkType::Udp6]);
    // Gather loopback candidates so a client can reach a server on the same
    // host (127.0.0.1 / ::1) — needed for loopback tests, harmless otherwise.
    se.set_include_loopback_candidate(true);

    let api = APIBuilder::new().with_setting_engine(se).build();
    let pc = Arc::new(
        api.new_peer_connection(RTCConfiguration::default())
            .await
            .map_err(|e| Error::Dial(format!("peer connection: {e}")))?,
    );

    // Pre-allocate the negotiated control channel (ID 0) before the offer so
    // the offer carries the application m-line; it also carries
    // CONNECTION_CLOSE (SPEC §8). Not announced via DCEP; never a stream.
    let control = pc
        .create_data_channel(
            "_kps_control",
            Some(RTCDataChannelInit { negotiated: Some(0), ..Default::default() }),
        )
        .await
        .map_err(|e| Error::Dial(format!("control channel: {e}")))?;

    // WebrtcConn::new also creates the negotiated datagram channel (ID 1),
    // which must exist before the offer for the same reason.
    let remote: std::net::SocketAddr = format!(
        "{}:{}",
        if a.ip.contains(':') { format!("[{}]", a.ip) } else { a.ip.clone() },
        a.port
    )
    .parse()
    .map_err(|e| Error::Address(format!("bad ip in address: {e}")))?;
    let conn = WebrtcConn::new(pc.clone(), Some(control), remote).await?;
    let close_state = conn.close_state.clone();

    pc.on_peer_connection_state_change(Box::new(move |s: RTCPeerConnectionState| {
        match s {
            RTCPeerConnectionState::Failed | RTCPeerConnectionState::Closed => {
                close_state.mark(None);
            }
            _ => {}
        }
        Box::pin(async {})
    }));

    let offer = pc
        .create_offer(None)
        .await
        .map_err(|e| Error::Dial(format!("create offer: {e}")))?;
    pc.set_local_description(offer)
        .await
        .map_err(|e| Error::Dial(format!("set local: {e}")))?;

    let answer_sdp = synthesize_webrtc_answer(&a, &ufrag, &pwd, &digest_to_fingerprint(&digest));
    let answer = RTCSessionDescription::answer(answer_sdp)
        .map_err(|e| Error::Dial(format!("synthesize answer: {e}")))?;
    pc.set_remote_description(answer)
        .await
        .map_err(|e| Error::Dial(format!("set remote: {e}")))?;

    // Established = transport up AND mutual HELLO (SPEC §8): dial MUST NOT
    // complete before the HELLO exchange.
    if let Err(e) = conn.wait_established().await {
        let _ = pc.close().await;
        return Err(Error::Dial(format!("connection failed: {e}")));
    }
    Ok(Box::new(conn))
}

fn digest_to_fingerprint(d: &[u8]) -> String {
    d.iter().map(|b| format!("{b:02X}")).collect::<Vec<_>>().join(":")
}

/// Fabricates the answer SDP the server would have sent (it never actually
/// transmits one — SPEC §5.2): ICE-lite, the derived ICE creds, the pinned
/// fingerprint, and a host candidate at the dialed endpoint.
fn synthesize_webrtc_answer(a: &Address, ufrag: &str, pwd: &str, fingerprint: &str) -> String {
    let (fam, any_addr) = if a.ip.contains(':') { ("IP6", "::") } else { ("IP4", "0.0.0.0") };
    [
        "v=0".to_string(),
        format!("o=- 0 0 IN {fam} {any_addr}"),
        "s=-".to_string(),
        "t=0 0".to_string(),
        "a=ice-lite".to_string(),
        format!("m=application {} UDP/DTLS/SCTP webrtc-datachannel", a.port),
        format!("c=IN {fam} {}", a.ip),
        "a=mid:0".to_string(),
        format!("a=ice-ufrag:{ufrag}"),
        format!("a=ice-pwd:{pwd}"),
        format!("a=fingerprint:sha-256 {fingerprint}"),
        "a=setup:passive".to_string(),
        "a=sctp-port:5000".to_string(),
        "a=max-message-size:1048576".to_string(),
        format!("a=candidate:1 1 UDP 1 {} {} typ host", a.ip, a.port),
    ]
    .join("\r\n")
        + "\r\n"
}
