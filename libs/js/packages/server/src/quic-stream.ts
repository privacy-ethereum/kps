// QUIC stream adapter: @infisical/quic's QUICStream is already a
// ReadableWritablePair of Uint8Array (web streams), so it maps almost 1:1 onto
// the core Stream. QUIC carries FIN/RESET/STOP_SENDING natively, so there is no
// §6.2 framing here — closing/cancelling/aborting the web streams drives the
// QUIC stream lifecycle directly.

import { reasonFrom, type Stream as CoreStream, type StreamCloseInfo, type KpsReason } from '@kpstreams/core'
import type { QUICStream } from '@infisical/quic'

export class QuicStream implements CoreStream {
  readonly readable: ReadableStream<Uint8Array>
  readonly writable: WritableStream<Uint8Array>
  readonly closed: Promise<StreamCloseInfo>

  #qs: QUICStream

  constructor(qs: QUICStream) {
    this.#qs = qs
    // Node's stream/web ReadableStream is the same runtime object as the global
    // ReadableStream the core type refers to; the cast bridges the lib typings.
    this.readable = qs.readable as unknown as ReadableStream<Uint8Array>
    this.writable = qs.writable as unknown as WritableStream<Uint8Array>
    this.closed = qs.closedP.then(
      () => ({ ok: true } as StreamCloseInfo),
      (e) => ({ ok: false, reason: reasonFrom(e) }),
    )
  }

  async closeWrite(): Promise<void> {
    const w = this.writable.getWriter()
    try { await w.close() } finally { try { w.releaseLock() } catch { /* ignore */ } }
  }

  async cancelRead(reason?: KpsReason): Promise<void> {
    try { await this.readable.cancel(reason?.message ?? reason?.code ?? 'cancelled') } catch { /* ignore */ }
  }

  async resetWrite(reason?: KpsReason): Promise<void> {
    const w = this.writable.getWriter()
    try { await w.abort(reason?.message ?? reason?.code ?? 'reset') } catch { /* ignore */ } finally { try { w.releaseLock() } catch { /* ignore */ } }
  }

  async close(): Promise<void> {
    const qs = this.#qs as unknown as { destroy?: (opts?: { force?: boolean }) => Promise<void> }
    try { await qs.destroy?.({ force: true }) } catch { /* ignore */ }
  }
}
