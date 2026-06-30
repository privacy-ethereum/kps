# @kpstreams/quic-client

QUIC client for **KPS** (Key-Pinned Streams).

> **Status: stub — not yet implemented.** `dial()` throws. This package reserves
> the name and the public surface; it currently only re-exports the
> [`@kpstreams/core`](https://www.npmjs.com/package/@kpstreams/core) contract.

## Intended shape

A native (Node) client that dials `ip:port:certhash` over QUIC — TLS 1.3 with the
server certificate pinned against the certhash (no CA), ALPN, and native QUIC
streams + datagrams — returning the same core `Connection` as every other KPS
transport:

```ts
import { dial } from '@kpstreams/quic-client'   // throws today
const conn = await dial('203.0.113.5:41108:uEiD...')
```

Unlike the WebRTC client it does **not** depend on `@kpstreams/core/webrtc` —
QUIC carries FIN/RESET/STOP_SENDING and datagrams natively, so there's no
datachannel framing to share.

Server-side QUIC accept already exists in
[`@kpstreams/server`](https://www.npmjs.com/package/@kpstreams/server), and the
Go implementation (`github.com/privacy-ethereum/kps/libs/go`) ships a working
QUIC client today.

## License

MIT
