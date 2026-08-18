# @kpstreams/quic-client

Native (Node) **QUIC** client for **KPS** (Key-Pinned Streams): dial a server
pinned by its certificate hash over QUIC — TLS 1.3, ALPN, native streams and
datagrams, no CA. Returns the same transport-neutral
[`@kpstreams/core`](https://www.npmjs.com/package/@kpstreams/core) `Connection`
as the WebRTC client, so callers program against one API regardless of transport.

Requires **Node ≥ 20**. Has a native dependency (`@infisical/quic`, prebuilt).

## Install

```sh
npm install @kpstreams/quic-client
```

## Usage

```ts
import { dial } from '@kpstreams/quic-client'

const conn = await dial('203.0.113.5:41108:uEiD...')   // opts: { signal? } — e.g. AbortSignal.timeout(ms)

const stream = await conn.openStream()
const writer = stream.writable.getWriter()
await writer.write(new TextEncoder().encode('hello'))
await writer.close()
for await (const chunk of stream.readable as any) { /* ... */ }

await conn.sendDatagram(new Uint8Array([1, 2, 3]))     // unreliable
// const dg = await conn.receiveDatagram()             // next inbound datagram
await conn.close()                                     // also closes the QUIC socket
```

One-shot convenience — dial, open one stream, and tie the connection's lifetime
to it (mirrors `@kpstreams/webrtc-client`):

```ts
import { openStream } from '@kpstreams/quic-client'
const stream = await openStream('203.0.113.5:41108:uEiD...')
```

## How trust works

No CA, no domain. The client pins the server's leaf certificate during the TLS
handshake: it accepts iff `sha256(cert)` equals the certhash in the address
(returning `CryptoError.BadCertificate` otherwise). QUIC carries
FIN/RESET/STOP_SENDING and datagrams natively, so — unlike the WebRTC client —
this package does **not** use `@kpstreams/core/webrtc`.

Interops with [`@kpstreams/server`](https://www.npmjs.com/package/@kpstreams/server)
and the Go server (`github.com/ethereum/kps/libs/go`). Verified: QUIC
stream echo against both a JS and the Go reference server, plus a datagram
round-trip against the JS server.

> The QUIC `Stream`/`Connection` adapters are currently duplicated with
> `@kpstreams/server`; a shared internal package could dedupe them later.

## License

MIT
