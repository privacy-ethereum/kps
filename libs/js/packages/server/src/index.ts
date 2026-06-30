// @kpstreams/server — the KPS-accepting end. Currently implements the WebRTC
// transport (browser/webrtc-client peers): one IceUdpMuxListener accepting many
// clients under a single pinned identity, presenting the transport-neutral core
// Connection/Stream. (QUIC-accept is future work.)

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
