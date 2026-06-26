import { dial } from './connection.js'
import type { DialOptions } from '@kpstreams/core'
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
