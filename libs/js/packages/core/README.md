# @kpstreams/core

Transport-neutral core for **KPS** (Key-Pinned Streams) — the pieces every
transport and both ends share. Zero runtime dependencies.

KPS connects to a server pinned by the hash of its self-signed certificate (no
CA, no domain): an address is just `ip:port:certhash`. This package has no
transport of its own — install a client (`@kpstreams/webrtc-client`,
`@kpstreams/quic-client`) or `@kpstreams/server`.

## Install

```sh
npm install @kpstreams/core
```

## Two entry points

### `@kpstreams/core` — the transport-neutral surface

```ts
import {
  parseAddress, formatAddress,      // "ip:port:certhash" <-> { ip, port, certhash }
  decodeCerthash, encodeCerthash,   // certhash <-> raw 32-byte sha-256 digest
  reasonFrom, streamError,          // error-model helpers
} from '@kpstreams/core'

import type {
  Address,
  Connection, Stream, Datagrams,    // the contract every transport implements
  DialOptions, OpenStreamOptions, AcceptStreamOptions,
  ConnCloseInfo, StreamCloseInfo,
  KpsErrorCode, KpsReason,          // canonical error codes (SPEC §9.1)
} from '@kpstreams/core'
```

The contract in one glance:

```ts
interface Connection {
  readonly closed: Promise<ConnCloseInfo>
  readonly datagrams: Datagrams
  readonly state: 'connecting' | 'open' | 'closed'
  openStream(opts?: OpenStreamOptions): Promise<Stream>
  acceptStream(opts?: AcceptStreamOptions): Promise<Stream>
  close(reason?: KpsReason): Promise<void>
}

interface Stream {                  // an unnamed, reliable, ordered byte stream
  readonly readable: ReadableStream<Uint8Array>
  readonly writable: WritableStream<Uint8Array>
  readonly closed: Promise<StreamCloseInfo>
  closeWrite(): Promise<void>       // half-close (peer sees EOF)
  cancelRead(reason?: KpsReason): Promise<void>
  resetWrite(reason?: KpsReason): Promise<void>
  close(reason?: KpsReason): Promise<void>
}
```

### `@kpstreams/core/webrtc` — the WebRTC wire protocol

The §6.2 datachannel framing, the certhash→SDP fingerprint, and the SDP/ICE
synthesis. Imported by **both** `@kpstreams/webrtc-client` and the WebRTC path of
`@kpstreams/server` (the server speaks the same framing). QUIC needs none of it.

```ts
import {
  encodeData, encodeFin, encodeCode, decodeFrame, codeToNum, numToCode,
  FRAME_DATA, FRAME_FIN, FRAME_RESET, FRAME_STOP_SENDING, MAX_FRAME_PAYLOAD,
  digestToSdpFingerprint,
  generateUfrag, deriveICEPwd, synthesizeAnswer, buildClientOffer,
  rewriteOfferUfrag, extractUfragFromLocalOffer,
} from '@kpstreams/core/webrtc'
```

## License

MIT
