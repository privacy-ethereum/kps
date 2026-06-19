# KPS — key-pinned stream

![Browser dials a KPS server over UDP directly — no signalling server, CA, or domain registrar required.](demos/chat/web/public/banner.avif)

A small library for opening TCP-like streams between two parties identified
by a cryptographic public key, not by a domain name signed by a certificate
authority.

The address you dial is just `<ip>:<port>:<keyhash>`. The keyhash pins the
server's TLS cert, so as long as the address reaches you intact, the
connection cannot be intercepted.

## What this is for

The browser-to-server channel that the web is built on requires a CA-signed
cert for a registered domain — a model that solves authentication by
delegating to authorities. KPS gives you the same thing without those
dependencies: server identity is its key. Clients pin the key out-of-band
(in code, in a config file, in a QR code, in a multiaddr, however).

The first transport is WebRTC-based, so it works from a browser today. The
abstraction is transport-agnostic; a server-to-server variant over plain
TCP+TLS is a natural follow-up — same `dial(addr) → stream` interface, no
WebRTC machinery when both ends are servers.

## Inspiration

This project descends directly from
[**WebRTC Direct**](https://github.com/libp2p/specs/blob/master/webrtc/webrtc-direct.md)
as implemented in libp2p, which works the same way at the wire level —
DTLS handshake against a self-signed cert whose hash is published
out-of-band, and ICE-lite + synthesized SDP so no signaling server is
needed.

The earlier exploration that motivated splitting this out lives at
[**voltrevo/webrtc-direct-demo**](https://github.com/voltrevo/webrtc-direct-demo).
That demo runs WebRTC Direct via libp2p and uses it to talk to a chat
server and a JSON-RPC proxy without a CA-signed domain. The transport
trick is great; the libp2p baggage on top (multistream-select, Noise XX
on a key separate from the cert, peer store, connection manager, peer
discovery, varint-framed protobuf wire formats, etc.) is paying for
properties that aren't needed for "browsers securely talk to a known
server" — so KPS strips it down to just the pinned-key stream.

What's intentionally **not** here:
- Peer discovery — KPS clients dial a known address.
- Multistream-select — stream name is the data-channel label.
- A second pinned key beyond the cert — the cert hash is the identity.
- Any framing or message format — message-oriented data channels pass
  bytes through; the application chooses its own encoding.

## What works

Browser ↔ Go server, end-to-end encrypted, message-oriented streams.
Multiplexed: any number of named streams on one connection; any number of
clients on one UDP port. Integration test (`tests/`) drives a Chromium
through the full handshake against a freshly spawned server and round-trips
a message in ~170 ms.

## Layout

```
libs/js/        TypeScript browser/client library — Connection, Stream, address helpers
libs/go/        Go library + cmd/server demo CLI — import "github.com/voltrevo/kps/libs/go"
demos/chat/     Chat + eth-rpc demo (server-go + web) consuming the libraries
tests/interop/  Playwright interop test — browser dials the Go server
```

## Quick taste

Server:

```go
listener, _ := kps.Listen(ctx, ":4242", kps.Options{KeyFile: "kps.key"})
listener.Handle("echo", func(s *kps.Stream) {
    for {
        buf, err := s.Recv()
        if err != nil { return }
        s.Send(buf)
    }
})
fmt.Println(listener.Address("")) // 192.168.x.y:4242:uEi...
```

Browser:

```js
import { dial } from '@kps/client'

const conn = await dial('192.168.x.y:4242:uEi...')
const stream = await conn.openStream('echo')
stream.send('hello')
for await (const buf of stream) {
  console.log(new TextDecoder().decode(buf))
  break
}
```

## Status

Pre-1.0. The transport works, the API is sketched, but rough edges remain
(error handling, reconnection, backpressure ergonomics, server-side
connection accept events, structured logging). The shape is settled
enough to build on.