# KPS: Key Pinned Streams

![Browser dials a KPS server over UDP directly — no signalling server, CA, or domain registrar required.](demos/chat/web/public/banner.avif)

A small library for opening secure, multiplexed byte streams to a peer
identified by a cryptographic certificate hash, not by a domain name signed by a
certificate authority.

The address you dial is just `<ip>:<port>:<certhash>`. The certhash pins the
server's self-signed certificate, so as long as the address reaches you intact,
the connection cannot be intercepted. The same address works from the **browser**
(over WebRTC) and from **native code** (over QUIC), on one UDP port — the public
API hides which transport a connection uses.

See [`SPEC.md`](SPEC.md) for the wire protocol and [`SECURITY.md`](SECURITY.md)
for the trust and censorship-resistance model.

## What this is for

The browser-to-server channel that the web is built on requires a CA-signed
cert for a registered domain — a model that solves authentication by delegating
to authorities. KPS gives you the same thing without those dependencies: server
identity *is* its certificate, and clients pin its hash out-of-band (in code, in
a config file, in a QR code, however).

WebRTC makes it work from a browser today; QUIC makes it work from native
clients (Go, Rust, and later CLI/mobile) against the same listener. One
self-signed certificate serves both — it's presented in the WebRTC DTLS
handshake and the QUIC TLS 1.3 handshake, and a single certhash pins both.

## Inspiration

KPS descends from
[**WebRTC Direct**](https://github.com/libp2p/specs/blob/master/webrtc/webrtc-direct.md)
as implemented in libp2p: a DTLS handshake against a self-signed cert whose hash
is published out-of-band, with ICE-lite + synthesized SDP so no signaling server
is needed. The earlier exploration lives at
[**voltrevo/webrtc-direct-demo**](https://github.com/voltrevo/webrtc-direct-demo).
The transport trick is great; the libp2p baggage on top (multistream-select,
Noise XX on a separate key, peer store, discovery, varint protobuf framing) pays
for properties that aren't needed for "talk securely to a known server" — so KPS
strips it down to the pinned-key stream, and adds a native QUIC transport.

What's intentionally **not** here:
- **Peer discovery** — KPS clients dial a known address.
- **Stream names / protocol negotiation** — streams are unnamed; applications
  route and frame inside the stream bytes.
- **A second pinned key** beyond the cert — the cert hash is the identity.
- **App-level message framing** — streams are raw byte streams; the application
  chooses its own encoding.

## What works

Browser, native-Go, native-Rust, and Node clients interoperate with a Go, Rust,
**or** Node (`@kpstreams/server`) listener, end-to-end encrypted. Both
transports — **WebRTC** (browser) and **QUIC** (native / Node) — share one UDP
port and one address, with the API hiding which is in use.

- **Unnamed, multiplexed byte streams** — reliable, ordered, no message
  boundaries; QUIC-like lifecycle (`closeWrite` → EOF, `cancelRead`, `resetWrite`).
  Any number of streams per connection, any number of connections per port.
- **Datagrams** — unreliable, unordered, size-limited messages on every
  connection (QUIC DATAGRAM; a reserved unreliable channel on WebRTC).
- **Censorship-resistant handshake** — the ICE password is derived from the
  certhash (no recomputable fingerprint), the QUIC ALPN is non-identifying, and
  the certificate carries no KPS-identifying metadata (see `SECURITY.md`).

An interop test matrix exercises all of this: Go and Rust tests cover their
libraries (multi-conn, multi-stream, the stream lifecycle, datagrams,
connection-close codes, and both transports on one port simultaneously); a Node
`node:test` suite drives `@kpstreams/quic-client` and `@kpstreams/server`
end-to-end and cross-implementation against the Go and Rust servers (streams,
datagrams, backpressure, certhash-pinning rejection, abort), plus Go ↔ Rust
both ways; and a Playwright-driven Chromium covers the real browser ↔ Go,
browser ↔ JS-server, and browser ↔ Rust paths. CI runs the lot.

## Layout

```
libs/js/        TypeScript npm packages (@kpstreams/*): core, webrtc-client, quic-client, server
libs/go/        Go library + cmd/server & cmd/dial CLIs — import "github.com/ethereum/kps/libs/go"
libs/rust/      Rust library (kps) + kps-server & kps-dial CLIs
demos/chat/     Chat + eth-rpc demo (server-go + web) consuming the libraries
tests/interop/  Playwright interop test — a browser dials the Go, JS and Rust servers
```

## Quick taste

Go server (echoes each stream's bytes back):

```go
ln, _ := kps.Listen(ctx, ":4242", kps.Options{KeyFile: "kps.key"})
fmt.Println(ln.Address("")) // 192.168.x.y:4242:uEi...

for {
    conn, err := ln.Accept(ctx)
    if err != nil { return }
    go func() {
        for {
            s, err := conn.AcceptStream(ctx)
            if err != nil { return }
            go func() { io.Copy(s, s); s.CloseWrite() }()
        }
    }()
}
```

Native Go client (QUIC by default):

```go
conn, _ := kps.Dial(ctx, "192.168.x.y:4242:uEi...")
s, _ := conn.OpenStream(ctx)
s.Write([]byte("hello"))
s.CloseWrite()
io.Copy(os.Stdout, s) // "hello"
```

Native Rust client (QUIC by default):

```rust
let conn = kps::dial("192.168.x.y:4242:uEi...").await?;
let mut s = conn.open_stream().await?;
s.write_all(b"hello").await?;
s.close_write().await?;
let mut echoed = Vec::new();
s.read_to_end(&mut echoed).await?; // "hello"
```

Browser (WebRTC):

```js
import { dial } from '@kpstreams/webrtc-client'

const conn = await dial('192.168.x.y:4242:uEi...')
const stream = await conn.openStream()

const writer = stream.writable.getWriter()
await writer.write(new TextEncoder().encode('hello'))
await writer.close()

const reader = stream.readable.getReader()
const { value } = await reader.read()
console.log(new TextDecoder().decode(value)) // "hello"
```

## Status

Pre-1.0, but built end-to-end: a monorepo with [`SPEC.md`](SPEC.md) as the source
of truth, two interoperating transports on one port, unnamed byte streams with a
full lifecycle, mandatory datagrams, a censorship-resistant handshake, and an
interop matrix + CI. Rough edges remain.
