# @kpstreams/webrtc-client

Browser **WebRTC** client for **KPS** (Key-Pinned Streams): dial a server pinned
by its certificate hash and open/accept unnamed, reliable byte streams — plus
unreliable datagrams. No CA, no domain, no signaling server. Zero native
dependencies (pure WebRTC in the browser).

An address is `ip:port:certhash`. The client derives the ICE password from the
certhash and pins the server's DTLS certificate against it, so only the holder of
that exact certificate can complete the handshake.

## Install

```sh
npm install @kpstreams/webrtc-client
```

## Usage

```ts
import { dial } from '@kpstreams/webrtc-client'

const conn = await dial('203.0.113.5:41108:uEiD...')

// open a byte stream (WHATWG ReadableStream / WritableStream of Uint8Array)
const stream = await conn.openStream()
const writer = stream.writable.getWriter()
await writer.write(new TextEncoder().encode('hello'))
await writer.close()                 // half-close; peer sees EOF

for await (const chunk of stream.readable as any) { /* ... */ }

// unreliable datagrams (≤ ~1100 bytes is safe; oversized send rejects 'too-large')
await conn.datagrams.send(new Uint8Array([1, 2, 3]))

await conn.close()
```

One-shot convenience — dial, open one stream, and tie the connection's lifetime
to it:

```ts
import { openStream } from '@kpstreams/webrtc-client'
const stream = await openStream('203.0.113.5:41108:uEiD...')
```

## API

- `dial(addr, opts?): Promise<Connection>` — `opts: { signal?, timeoutMs? }` (default 15 s).
- `openStream(addr, opts?): Promise<Stream>` — one-shot; closing the stream closes the connection.
- `Connection` / `Stream` classes implementing the [`@kpstreams/core`](https://www.npmjs.com/package/@kpstreams/core) contract.
- `parseAddress` / `formatAddress` re-exported from core for convenience.

Built on the browser's `RTCPeerConnection` (and `crypto.subtle`), so it runs in a
secure context (https / localhost). A Node WebRTC client is a separate future
package.

## License

MIT
