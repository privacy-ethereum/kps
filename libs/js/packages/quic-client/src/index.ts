// @kpstreams/quic-client — QUIC client for KPS (stub).
//
// QUIC provides FIN/RESET/STOP_SENDING + datagrams natively, so this package
// does NOT import @kpstreams/core/webrtc; it only implements the core contract.

import type { Connection, DialOptions } from '@kpstreams/core'
export { parseAddress, formatAddress } from '@kpstreams/core'
export type { Address, Connection, Stream, DialOptions } from '@kpstreams/core'

// TODO: implement over a node QUIC binding (e.g. @matrixai/quic), returning a
// core Connection with TLS cert pinning against the certhash.
export function dial(_addr: string, _opts?: DialOptions): Promise<Connection> {
  throw new Error('@kpstreams/quic-client: not implemented')
}
