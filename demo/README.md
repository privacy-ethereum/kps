# KPS demo

A port of [`webrtc-direct-demo`](https://github.com/voltrevo/webrtc-direct-demo)
to **KPS**.

Same two services on top — public bulletin + E2EE direct messages, and a
multi-network Ethereum block explorer powered by a JSON-RPC proxy — wired
to a single KPS connection instead of libp2p WebRTC Direct.

## What's different from the libp2p version

| | libp2p webrtc-direct-demo | KPS demo |
|---|---|---|
| address | `/ip4/.../udp/.../webrtc-direct/certhash/.../p2p/...` | `<ip>:<port>:<keyhash>` |
| handshake | DTLS (cert-pinned) + Noise XX (peer-id-pinned) | DTLS only (cert-pinned) |
| connection-layer identity | server's libp2p peer ID | none — cert hash *is* the identity |
| application-layer identity | libp2p peer ID for both ends | per-client Ed25519 keypair (KPS doesn't expose one) |
| stream framing | multistream-select protocol IDs | data-channel labels (`chat`, `eth-rpc`) |
| server | Node.js + js-libp2p | Go + `kps/server` |

The DM E2EE story is the same: each browser mints an Ed25519 keypair, signs
its ECDH-P256 DM key with it, the server verifies and republishes the
signature in the roster, other peers verify before deriving a shared key.
The server can route ciphertext but can't read or forge DMs.

## Layout

```
server/    Go server (chat + eth-rpc handlers)
web/       Vite app (chat UI + block explorer)
web/extension/  MV3 manifest + bg script for Chrome unpacked-extension build
```

## Run

The web app depends on the KPS client, which lives one level up. Build it
once first:

```sh
cd ../client
npm install
npm run build
```

Server (Go 1.24+):

```sh
cd demo/server
go run .          # binds a free UDP port; persists port + cert in state.json
# or: go run . -listen :41108 -ip 192.168.1.50
```

It prints the address to dial, e.g.:

```
listening; dial from the browser:
  192.168.1.50:41108:uEi...
```

Web (Node 20+):

```sh
cd demo/web
npm install
npm run dev       # vite dev server
```

Open the printed URL, click **Try it**, paste the server's address,
**Connect**.

### Pick the right address

KPS is just UDP underneath. The browser must reach the server's UDP
port directly:

- **Same machine** — leave the default; the server will print a
  `127.0.0.1:...` line.
- **Same LAN** — start the server with `-ip 192.168.x.x` (or whatever
  your LAN address is); the printed address will use that IP.
- **Across the internet** — public IP, or forward the UDP port.

## Chromium extension build (no CA dependency for the page either)

```sh
cd demo/web
npm run build:extension
```

Output lands in `demo/web/dist-extension/`. Load it in Chrome via
**Extensions → Developer mode → Load unpacked**, point at that folder,
click the toolbar icon. The page loads from `chrome-extension://<id>/`,
trusted at install time rather than by a public CA — neither the page nor
its connection to the server depends on a domain registrar or a CA.

## Deploy the web app to GitHub Pages

The workflow at [`.github/workflows/pages.yml`](../.github/workflows/pages.yml)
builds `demo/web/` and publishes on every push to `main` that touches
`demo/web/**` or `client/**`. To enable it on your fork:

1. **Settings → Pages**, set **Source** to **GitHub Actions**.
2. Push to `main`.

The web app uses `base: './'` in `vite.config.js`, so it works at any
subpath URL.

## Wire protocols

### `chat` stream — line-delimited JSON

Client → server:
```jsonc
{ "type": "hello", "idPublicKey": "<b64 raw Ed25519 pubkey>",
  "dmPublicKey": "<b64 raw P-256 pubkey>",
  "dmSignature": "<b64 Ed25519(dmSigPayload)>",
  "name": "<optional>" }

{ "type": "bulletin", "id": <num>, "text": "..." }

{ "type": "dm", "id": <num>, "to": "<peerId>",
  "iv": "<b64>", "ciphertext": "<b64 AES-GCM-256>" }
```

Server → client:
```jsonc
{ "type": "roster", "peers": [
  { "peerId": "<idPublicKey>", "idPublicKey": "...",
    "dmPublicKey": "...", "dmSignature": "...", "name": "..." },
  ...
] }

{ "type": "bulletin", "from": "<peerId>", "text": "..." }
{ "type": "dm", "from": "<peerId>", "iv": "...", "ciphertext": "..." }
{ "type": "ack", "id": <num> }
{ "type": "dm-fail", "id": <num>, "reason": "..." }
```

`peerId` is the base64 of the raw 32-byte Ed25519 public key. No separate
peer-ID hashing layer — the verification key is its own ID.

`dmSigPayload` = `"kps-webrtc-dm-key-v1:" || rawDmPublicKey`.

### `eth-rpc` stream — line-delimited JSON

Client:
```json
{ "network": "ethereum",
  "req": { "jsonrpc": "2.0", "id": 1, "method": "eth_blockNumber", "params": [] } }
```

Server replies with the upstream JSON-RPC response verbatim.

Networks: `ethereum`, `arbitrum`, `optimism`, `base`, `polygon` (also
chain IDs and short aliases — `eth`, `arb`, `op`, `poly`, `matic`).

## Inspiration / origin

This descends directly from the libp2p WebRTC Direct demo
([voltrevo/webrtc-direct-demo](https://github.com/voltrevo/webrtc-direct-demo)).
The transport trick is the same; this version strips out everything KPS
intentionally drops (Noise XX, multistream-select, peer store, varint
framing) and substitutes a minimal application-level Ed25519 identity for
the DM-signing path.
