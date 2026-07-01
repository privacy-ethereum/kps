# KPS — JavaScript / TypeScript packages

This is the npm workspace for **KPS** (Key-Pinned Streams): secure, multiplexed
byte streams to a server identified by its **certificate hash**, not by a
CA-signed domain name.

The address you dial is just `<ip>:<port>:<certhash>`. The certhash pins the
server's self-signed certificate, so as long as the address reaches you intact,
the connection cannot be intercepted — no domain, no certificate authority, no
signalling server. In the browser this runs over WebRTC; native clients use
QUIC; the same server address serves both.

See the [project README](https://github.com/privacy-ethereum/kps#readme) and the
[protocol spec](https://github.com/privacy-ethereum/kps/blob/main/SPEC.md). The
server also has a Go implementation at
`github.com/privacy-ethereum/kps/libs/go`.

## Packages

| Package | What it is | Status |
|---|---|---|
| [`@kpstreams/core`](packages/core) | Transport-neutral surface — addresses, certhash, error model, the `Connection`/`Stream`/`Datagrams` contract, plus the WebRTC wire protocol under `@kpstreams/core/webrtc`. Zero runtime deps. | ✅ |
| [`@kpstreams/webrtc-client`](packages/webrtc-client) | Browser WebRTC client: `dial()` a server and open/accept byte streams + datagrams. | ✅ |
| [`@kpstreams/server`](packages/server) | Node server accepting **both** WebRTC and QUIC on a single public port under one pinned identity. Requires Node ≥ 20. | ✅ |
| [`@kpstreams/quic-client`](packages/quic-client) | Native QUIC client. | 🚧 stub |

Clients are per-transport (a client knows its environment); the server is
singular (it accepts whoever dials). Transport is the package boundary because
that's the axis that controls weight — the browser WebRTC client pulls in nothing
native, while QUIC needs a native binding.

## Which do I install?

- **Browser app connecting to a KPS server** → `@kpstreams/webrtc-client`
- **Running a KPS server in Node** → `@kpstreams/server`
- **Building your own transport / tooling** → `@kpstreams/core`

## Developing this workspace

```sh
npm install          # links the workspace packages
npm run build        # builds core first, then the rest
```

Each package builds with `tsc` to its own `dist/`; `@kpstreams/core` must build
before the packages that depend on it (the root `build` script orders this).

## License

MIT — see [LICENSE](LICENSE).
