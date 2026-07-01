// QUIC connection adapter: wraps @infisical/quic's QUICConnection as a core
// Connection. Streams come via the connection's stream event / newStream.
//
// Datagrams: @infisical/quic's high-level API doesn't surface QUIC DATAGRAMs, so
// we reach the native quiche connection (`conn`) for dgramSend, and poll
// dgramRecv (there's no receive event). This couples us to library internals and
// is best-effort; if a future @infisical/quic exposes datagrams, switch to it.

import {
  reasonFrom,
  type Connection as CoreConnection, type Stream as CoreStream,
  type ConnCloseInfo, type Datagrams, type KpsReason,
  type OpenStreamOptions, type AcceptStreamOptions,
} from '@kpstreams/core'
import quicPkg from '@infisical/quic'
import type { QUICConnection } from '@infisical/quic'
import { QuicStream } from './quic-stream.js'

// CommonJS module with non-analyzable named exports — destructure the default.
const { events: quicEvents } = quicPkg as unknown as { events: { EventQUICConnectionStream: { name: string } } }

const MAX_DATAGRAM = 1200 // parity with the WebRTC cap; ~1100 is safe everywhere

interface NativeConn {
  dgramSend(data: Uint8Array): void | null
  dgramRecvVec(): Uint8Array | null
}

function makeDatagrams(qc: QUICConnection): Datagrams {
  const native = (): NativeConn | undefined => (qc as unknown as { conn?: NativeConn }).conn
  const queue: Uint8Array[] = []
  let waiter: ((v: Uint8Array) => void) | null = null
  let stopped = false

  // No receive event exists; poll the native datagram recv queue.
  const poll = setInterval(() => {
    const c = native()
    if (!c) return
    for (;;) {
      let d: Uint8Array | null = null
      try { d = c.dgramRecvVec() } catch { break }
      if (!d) break
      if (waiter) { const w = waiter; waiter = null; w(d) }
      else { queue.push(d); if (queue.length > 256) queue.shift() }
    }
  }, 15)
  if (typeof (poll as { unref?: () => void }).unref === 'function') (poll as { unref: () => void }).unref()
  qc.closedP.finally(() => { stopped = true; clearInterval(poll) }).catch(() => {})

  const incoming = new ReadableStream<Uint8Array>({
    pull(controller) {
      const next = queue.shift()
      if (next) { controller.enqueue(next); return }
      return new Promise<void>(resolve => { waiter = (v) => { controller.enqueue(v); resolve() } })
    },
  })

  return {
    async send(data: Uint8Array) {
      if (data.length > MAX_DATAGRAM) {
        const e = new Error(`kps: datagram exceeds limit (max ${MAX_DATAGRAM} bytes)`)
        Object.assign(e, { code: 'too-large', maxDatagramPayloadSize: MAX_DATAGRAM })
        throw e
      }
      const c = native()
      if (!c || stopped) throw new Error('kps: datagram channel not available')
      try { c.dgramSend(data) } catch (err) {
        throw new Error('kps: datagram send failed: ' + (err as Error).message)
      }
      // Best-effort flush of the queued datagram (no public flush API).
      try { (qc as unknown as { send?: () => unknown }).send?.() } catch { /* ignore */ }
    },
    incoming,
  }
}

export class QuicConnection implements CoreConnection {
  readonly closed: Promise<ConnCloseInfo>
  readonly datagrams: Datagrams
  state: 'connecting' | 'open' | 'closed' = 'open' // delivered already-established

  #qc: QUICConnection
  #incoming: CoreStream[] = []
  #waiters: Array<{ resolve: (s: CoreStream) => void; reject: (e: Error) => void }> = []

  constructor(qc: QUICConnection) {
    this.#qc = qc
    this.datagrams = makeDatagrams(qc)
    this.closed = qc.closedP.then(
      () => ({ ok: true } as ConnCloseInfo),
      (e) => ({ ok: false, reason: reasonFrom(e) }),
    )
    qc.addEventListener(quicEvents.EventQUICConnectionStream.name, (e: Event) => {
      const qs = (e as unknown as { detail: ConstructorParameters<typeof QuicStream>[0] }).detail
      this.#enqueue(new QuicStream(qs))
    })
    qc.closedP.finally(() => {
      this.state = 'closed'
      for (const w of this.#waiters) w.reject(new Error('kps: connection closed'))
      this.#waiters = []
    }).catch(() => {})
  }

  async openStream(_opts: OpenStreamOptions = {}): Promise<CoreStream> {
    if (this.state !== 'open') throw new Error(`kps: connection is ${this.state}`)
    const qs = (this.#qc as unknown as { newStream: (t?: 'bidi' | 'uni') => ConstructorParameters<typeof QuicStream>[0] }).newStream('bidi')
    return new QuicStream(qs)
  }

  acceptStream(opts: AcceptStreamOptions = {}): Promise<CoreStream> {
    const ready = this.#incoming.shift()
    if (ready) return Promise.resolve(ready)
    if (this.state === 'closed') return Promise.reject(new Error('kps: connection is closed'))
    const signal = opts.signal
    return new Promise<CoreStream>((resolve, reject) => {
      const waiter = {
        resolve: (s: CoreStream) => { signal?.removeEventListener('abort', onAbort); resolve(s) },
        reject: (e: Error) => { signal?.removeEventListener('abort', onAbort); reject(e) },
      }
      const onAbort = () => {
        const i = this.#waiters.indexOf(waiter)
        if (i >= 0) this.#waiters.splice(i, 1)
        reject(new Error('kps: acceptStream aborted'))
      }
      this.#waiters.push(waiter)
      signal?.addEventListener('abort', onAbort, { once: true })
    })
  }

  async close(_reason?: KpsReason): Promise<void> {
    const qc = this.#qc as unknown as { stop?: (o?: unknown) => Promise<void> }
    try { await qc.stop?.({ force: true }) } catch { /* ignore */ }
  }

  #enqueue(stream: CoreStream): void {
    const w = this.#waiters.shift()
    if (w) w.resolve(stream)
    else this.#incoming.push(stream)
  }
}
