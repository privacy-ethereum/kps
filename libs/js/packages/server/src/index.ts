// @kpstreams/server — the KPS-accepting end. Accepts both WebRTC and QUIC
// clients on a single public UDP port under one pinned identity (a demux relay
// fronts per-transport loopback backends), presenting the transport-neutral core
// Connection/Stream for either.

export { listen } from './listener.js'
export type { ListenOptions, Listener } from './listener.js'
export { Connection } from './connection.js'
export { Stream } from './stream.js'
export { loadOrCreateIdentity } from './identity.js'
export type { Identity } from './identity.js'

// Re-export the transport-neutral contract callers program against.
export type {
  Connection as ConnectionContract, Stream as StreamContract,
  Datagrams, ConnCloseInfo, StreamCloseInfo, KpsReason, KpsErrorCode,
} from '@kpstreams/core'
