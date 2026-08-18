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

## Using kps in your project

The crate is **not published to crates.io** (it depends on two vendored
webrtc-rs forks — see below). Depend on it via git:

```toml
[dependencies]
kps = { git = "https://github.com/ethereum/kps" }

# REQUIRED: kps only builds with the two vendored webrtc-rs forks below.
# Cargo applies [patch] ONLY from the root workspace manifest — a patch in a
# dependency does NOT propagate to you — so you MUST copy this block verbatim
# into YOUR workspace-root Cargo.toml. Without it the build fails to compile
# (or hits the bugs the patches fix).
[patch.crates-io]
webrtc-sctp = { git = "https://github.com/ethereum/kps.git", rev = "a73a0a8f9e4f76f3b89ba1155562ec836bcf3f3b" }
webrtc      = { git = "https://github.com/ethereum/kps.git", rev = "4982e1c1cdce3baa3f703a746367a5e53ca64928" }
```

Both `git` lines must point at the **same revs** this repo's
[`libs/rust/Cargo.toml`](Cargo.toml) pins; bump them together if you update
`kps`. This friction is temporary — it goes away once the fixes land upstream
(see below) and the vendored forks can be dropped.

## Tests

```
cargo test --workspace
```

Unit tests cover the core (address, certhash, ICE-pwd derivation with a
Go-pinned vector, §6.2 framing); integration tests run Rust↔Rust over both
transports, including both on one port concurrently. The cross-implementation
matrix (Rust ↔ Go ↔ JS ↔ browser) lives in `libs/js/test/integration/` and
`tests/interop/`.

## Vendored webrtc-rs patches (required)

`kps` swaps two crates.io dependencies for patched forks via
`[patch.crates-io]` in [`Cargo.toml`](Cargo.toml). Each fork is an **orphan
branch of this repo** holding a *verbatim* crates.io copy plus one or two fix
commits (grep `KPS PATCH` for the exact diff), pinned by rev so builds don't
float with the branch. Anyone consuming `kps` must replicate the same patch
block (see [Using kps in your project](#using-kps-in-your-project)) — Cargo
does not inherit `[patch]` from a dependency.

| Crate | Branch / rev | Fix | Upstream |
|-------|--------------|-----|----------|
| `webrtc-sctp` | `vendor/webrtc-sctp` @ `a73a0a8` | (1) drain the reassembly queue before honoring `read_shutdown` — a peer that writes then promptly closes a channel otherwise loses data at a webrtc-rs receiver ~10% of the time; (2) accept the in-sequence chunk at a full receive buffer — otherwise a tail-of-burst drop deadlocks the association at a permanent zero window | [#816](https://github.com/webrtc-rs/webrtc/issues/816), [#822](https://github.com/webrtc-rs/webrtc/issues/822) |
| `webrtc` | `vendor/webrtc` @ `4982e1c` | (1) adopt a negotiated data channel's stream in the accept loop — an early inbound message (KPS's HELLO) otherwise gets parsed as DCEP, fails with `InvalidMessageType`, and silently kills the accept loop for the whole connection; (2) gate the id-reuse claim on `ReadyState::Open` — otherwise a stale CLOSED channel (never pruned from `data_channels`) shadows a browser's reused SCTP stream id and the new stream is swallowed (read hangs after ~2 streams) | [#821](https://github.com/webrtc-rs/webrtc/issues/821); (2) is a regression from (1), not upstream ([kps#4](https://github.com/ethereum/kps/issues/4)) |

Each branch is `<verbatim crates.io copy>` then `<KPS PATCH commit(s)>`, so the
diff against upstream is exactly those commits. Drop the patches (and this
section) once releases ship equivalent fixes; bump the pinned revs here and in
any consumer's workspace together if the forks are updated.
