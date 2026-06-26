// @kpstreams/core — the transport-neutral surface: addresses, certhash, the
// error model, and the Connection/Stream/Datagrams contract. Zero runtime deps.
// The WebRTC wire protocol is a separate subpath: "@kpstreams/core/webrtc".

export type { Address } from './address.js'
export { parseAddress, formatAddress } from './address.js'
export { decodeCerthash, encodeCerthash } from './certhash.js'
export type { KpsErrorCode, KpsReason } from './errors.js'
export { reasonFrom, streamError } from './errors.js'
export type {
  Connection, Stream, Datagrams,
  DialOptions, OpenStreamOptions, AcceptStreamOptions,
  ConnCloseInfo, StreamCloseInfo,
} from './types.js'
