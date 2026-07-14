# kps (Rust)

The Rust implementation of [KPS](../../README.md) — see
[`SPEC.md`](../../SPEC.md) for the protocol. Dial `ip:port:certhash`, get an
authenticated, encrypted, multiplexed connection with unnamed byte streams and
datagrams; serve both transports (QUIC + WebRTC) on one UDP port.

## Layout

- `kps/` — the library crate: `dial` (QUIC, the native default),
  `dial_webrtc` (explicit override / browser-facing listeners), `listen`
  (both transports, one port), `Identity` (persistent self-signed cert).
- `kps-server/`, `kps-dial/` — echo server / echo client binaries with the
  same flags and output contract as `libs/go`'s `cmd/server` and `cmd/dial`;
  used by the cross-implementation test harnesses.

## Quick taste

```rust
// Server: echo every stream.
let ln = kps::listen(":4242", kps::ListenOptions::default()).await?;
println!("{}", ln.address("")); // 192.168.x.y:4242:uEi...
while let Ok(conn) = ln.accept().await {
    tokio::spawn(async move {
        while let Ok(stream) = conn.accept_stream().await {
            tokio::spawn(async move {
                let (mut rd, mut wr) = tokio::io::split(stream);
                let _ = tokio::io::copy(&mut rd, &mut wr).await;
                let _ = wr.shutdown().await; // FIN → peer sees EOF
            });
        }
    });
}
```

```rust
// Client (QUIC by default; kps::dial_webrtc for the WebRTC transport).
let conn = kps::dial("192.168.x.y:4242:uEi...").await?;
let mut stream = conn.open_stream().await?;
stream.write_all(b"hello").await?;
stream.close_write().await?;
let mut echoed = Vec::new();
stream.read_to_end(&mut echoed).await?; // "hello"
```

Timeouts/cancellation are caller-side: wrap any call in
`tokio::time::timeout` (the JS packages express the same thing through
`AbortSignal.timeout`).

## Tests

```
cargo test --workspace
```

Unit tests cover the core (address, certhash, ICE-pwd derivation with a
Go-pinned vector, §6.2 framing); integration tests run Rust↔Rust over both
transports, including both on one port concurrently. The cross-implementation
matrix (Rust ↔ Go ↔ JS ↔ browser) lives in `libs/js/test/integration/` and
`tests/interop/`.

## Vendored webrtc-sctp patch

`[patch.crates-io]` in `Cargo.toml` swaps `webrtc-sctp` for the
`vendor/webrtc-sctp` orphan branch of this repo (pinned by rev): verbatim crates.io 0.13.0
plus a drain-the-reassembly-queue-before-honoring-`read_shutdown` fix (grep
`KPS PATCH`). Without it, a peer that writes and promptly closes a data
channel loses data at a webrtc-rs receiver ~10% of the time. Reported
upstream as [webrtc-rs/webrtc#816](https://github.com/webrtc-rs/webrtc/issues/816);
remove when a release ships an equivalent fix.
