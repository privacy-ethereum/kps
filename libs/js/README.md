# key-pinned-streams

The browser client for **KPS** (Key-Pinned Streams): open secure, multiplexed
byte streams to a server identified by its **certificate hash**, not by a
CA-signed domain name. In the browser this runs over WebRTC; the same server is
reachable from native code over QUIC.

The address you dial is just `<ip>:<port>:<certhash>`. The certhash pins the
server's self-signed certificate, so as long as the address reaches you intact,
the connection cannot be intercepted — no domain, no certificate authority, no
signalling server.

This package is the JavaScript/TypeScript **client**. The server is a Go library
(`github.com/privacy-ethereum/kps/libs/go`). See the
[project README](https://github.com/privacy-ethereum/kps#readme) and
[protocol spec](https://github.com/privacy-ethereum/kps/blob/main/SPEC.md).

## Install

```sh
npm install key-pinned-streams
```

## Usage

```js
import { dial } from 'key-pinned-streams'

const conn = await dial('192.168.x.y:4242:uEi...')
const stream = await conn.openStream()

// write bytes
const writer = stream.writable.getWriter()
await writer.write(new TextEncoder().encode('hello'))
await writer.close()              // graceful EOF for the peer

// read bytes
const reader = stream.readable.getReader()
const { value } = await reader.read()
console.log(new TextDecoder().decode(value))
```

Streams are **unnamed, reliable, ordered byte streams** with no message
boundaries — frame and route your own protocol inside the bytes.

```js
await stream.closeWrite()          // finish the local write half (peer sees EOF)
await stream.cancelRead(reason)    // stop wanting inbound bytes
await stream.resetWrite(reason)    // abort the local write half (peer sees an error)
await stream.close(reason)         // tear down both halves

const inbound = await conn.acceptStream()   // accept a peer-opened stream
await conn.close()
```

Connection-level **datagrams** (unreliable, unordered, size-limited):

```js
await conn.datagrams.send(bytes)   // rejects { code: 'too-large', maxDatagramPayloadSize } if over the limit
for await (const dg of conn.datagrams.incoming) { /* ... */ }
```

One-shot convenience (dials, opens one stream, and ties the connection's lifetime
to that stream):

```js
import { openStream } from 'key-pinned-streams'
const stream = await openStream('192.168.x.y:4242:uEi...')
```

## Environment

Browser-targeted: it uses `RTCPeerConnection`, `crypto.subtle`, and WHATWG
`ReadableStream`/`WritableStream`. ESM only.

## License

MIT
