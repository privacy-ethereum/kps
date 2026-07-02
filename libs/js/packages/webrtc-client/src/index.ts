// @kpstreams/webrtc-client — the browser WebRTC client: dial a kps server and
// open/accept unnamed byte streams + datagrams. Implements the @kpstreams/core
// contract; callers program against the core Connection/Stream interfaces.

export { dial } from './connection.js'
export { openStream } from './open-stream.js'

// Convenience re-exports so callers don't also need to import @kpstreams/core.
// (Kept identical to @kpstreams/quic-client — same job, same surface.)
export { parseAddress, formatAddress } from '@kpstreams/core'
export type {
  Address, Connection, Stream,
  DialOptions, OpenStreamOptions,
  ConnCloseInfo, StreamCloseInfo,
  KpsErrorCode, KpsReason,
} from '@kpstreams/core'
