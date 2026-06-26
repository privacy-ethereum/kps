// @kpstreams/webrtc-client — the browser WebRTC client: dial a kps server and
// open/accept unnamed byte streams. Implements the @kpstreams/core contract.

export { dial, Connection } from './connection.js'
export { Stream } from './stream.js'
export { openStream } from './open-stream.js'

// Convenience re-exports so callers don't also need to import @kpstreams/core.
export { parseAddress, formatAddress } from '@kpstreams/core'
export type {
  Address, KpsErrorCode, KpsReason,
  DialOptions, ConnCloseInfo, StreamCloseInfo, Datagrams,
} from '@kpstreams/core'
