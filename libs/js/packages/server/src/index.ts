// @kpstreams/server — the KPS-accepting end (stub).
//
// One identity (self-signed cert) presented over both transports. Will import
// @kpstreams/core/webrtc (the same datachannel framing the client speaks) for
// the WebRTC-answerer path, plus a QUIC binding for the QUIC-accept path. It is
// NOT a consumer of @kpstreams/webrtc-client — that is why the wire protocol
// lives in core.

import type { Connection } from '@kpstreams/core'
export type { Connection } from '@kpstreams/core'

export interface ListenOptions {
  // keyFile | identity, listen address, etc.
  listen?: string
}

export interface Listener {
  address(ip: string): string // "ip:port:certhash"
  accept(): Promise<Connection> // a connected peer, transport-agnostic
  close(): Promise<void>
}

// TODO: implement WebRTC answerer (node-datachannel + @kpstreams/core/webrtc)
// and standard QUIC accept under one pinned identity.
export function listen(_opts: ListenOptions): Promise<Listener> {
  throw new Error('@kpstreams/server: not implemented')
}
