# @kpstreams/server

The accepting end of **KPS** (Key-Pinned Streams) for Node. Accepts **both
WebRTC and QUIC** clients on a **single public UDP port** under one pinned
self-signed identity, and hands you a transport-neutral
[`@kpstreams/core`](https://www.npmjs.com/package/@kpstreams/core) `Connection`
for either.

Requires **Node ≥ 20** (global WebCrypto). Has native dependencies
(`node-datachannel` for WebRTC, `@infisical/quic` for QUIC) — both ship
prebuilt binaries.

## Install

```sh
npm install @kpstreams/server
```

## Usage

```ts
import { listen } from '@kpstreams/server'

const ln = await listen({ port: 41108 })   // creates ./kps-cert.pem on first run
console.log('dial me at', ln.address('203.0.113.5'))
// -> 203.0.113.5:41108:uEiD...  (same address for WebRTC and QUIC)

for (;;) {
  const conn = await ln.accept()            // a connected peer (either transport)
  ;(async () => {
    const stream = await conn.acceptStream()
    await stream.readable.pipeTo(stream.writable)   // echo
  })()
}
```

`ListenOptions`:

| field | default | meaning |
|---|---|---|
| `port` | — | public UDP port |
| `address` | `'0.0.0.0'` | public bind (dual-stack wildcard) |
| `certPath` / `keyPath` | `kps-cert.pem` / `kps-key.pem` | persisted identity (created on first run; keep stable to keep the certhash stable) |
| `transports` | `['webrtc','quic']` | which transports to accept |

`Listener`: `address(ip)`, `accept({ signal? })`, `close()`, plus `certhash` / `port`.

## How the single port works

`node-datachannel` and `@infisical/quic` each own their UDP socket and (unlike
pion / quic-go) can't be handed an external one, so they can't natively share a
port. This package fronts them with a small userspace **demux relay**: one public
socket classifies each client's first packet (STUN → WebRTC, else → QUIC) and
forwards it — via a per-client loopback NAT socket — to the matching backend on a
private loopback port, NAT-ing responses back out the single public port. Clients
only ever talk to the public address, so ICE and QUIC are unaffected.

## Caveats

- **QUIC datagrams** are sent/received through `@infisical/quic`'s native quiche
  connection (its high-level API doesn't expose datagrams yet) — best-effort, and
  coupled to library internals.
- The relay keeps **one socket per active client** (idle-GC'd) — may limit scale.
- `loadOrCreateIdentity()` is exported if you want to manage the cert yourself.

## License

MIT
