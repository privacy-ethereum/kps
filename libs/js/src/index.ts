export { dial, Connection, type DialOptions, type ConnCloseInfo, type Datagrams } from './connection.js'
export { Stream, type StreamCloseInfo } from './stream.js'
export { parseAddress, formatAddress, type Address } from './address.js'
export type { KpsErrorCode, KpsReason } from './framing.js'

import { dial, type DialOptions } from './connection.js'
import type { Stream } from './stream.js'

// One-shot convenience over dial → openStream. The returned stream owns the
// hidden connection: closing the stream closes the connection (SPEC / API doc).
export async function openStream(addr: string, opts?: DialOptions): Promise<Stream> {
  const conn = await dial(addr, opts)
  try {
    const stream = await conn.openStream({ signal: opts?.signal })
    void stream.closed.finally(() => { void conn.close() })
    return stream
  } catch (err) {
    await conn.close()
    throw err
  }
}
