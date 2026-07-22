// QUIC stream adapter: @infisical/quic's QUICStream is already a
// ReadableWritablePair of Uint8Array (web streams), so it maps almost 1:1 onto
// the core Stream. QUIC carries FIN/RESET/STOP_SENDING natively, so there is no
// §6.2 framing here — closing/cancelling/aborting the web streams drives the
// QUIC stream lifecycle directly.

import { reasonFrom, streamError, type Stream as CoreStream, type StreamCloseInfo, type KpsReason } from '@kpstreams/core'
import type { QUICStream } from '@infisical/quic'

// Reaching into @infisical/quic's stream internals (as the rest of this adapter
// already does for newStream/destroy/conn). These are the web-stream controllers
// and the half-closed flags @infisical/quic keeps on each QUICStream. Because
// these are internals with no API contract, @infisical/quic is pinned to an
// EXACT version in package.json — a minor bump could rename/restructure them.
interface NativeStreamInternals {
  readableController?: { error(e?: unknown): void }
  writableController?: { error(e?: unknown): void }
  _readClosed?: boolean
  _writeClosed?: boolean
}

// Error a native QUIC stream's still-open halves for a LOCAL teardown, BEFORE
// @infisical/quic's force-destroy runs. A local connection/stream close is NOT a
// stream FIN: a holder that only has the stream (not the connection) must be
// able to tell the stream was cut off rather than finishing normally, so a
// pending read/write MUST reject — it must not look like a clean EOF (SPEC §9.2).
//
// @infisical/quic's force-destroy does reject them, but with a bare nullish
// reason: QUICConnection.errorLast defaults to `null` (a direct destroy passes
// `undefined`), so a parked `reader.read()` rejects with `null`. That's
// un-introspectable, and Node's default handler turns an unhandled `null` into a
// confusing process crash (kps#3; reported upstream — the library should reject
// with the Error it already builds, not bare `null`). Pre-empt it: error each
// still-open half with a proper KPS 'closed' reason so the rejection is a real,
// coded Error. The library's subsequent error(null) is then a no-op (the
// controller is already errored). Idempotent/guarded — controller methods throw
// once a half is settled.
export function errorOpenHalves(qs: unknown): void {
  const s = qs as NativeStreamInternals
  const err = streamError({ code: 'closed' })
  if (!s._readClosed) {
    try { s.readableController?.error(err) } catch { /* already settled */ }
  }
  if (!s._writeClosed) {
    try { s.writableController?.error(err) } catch { /* already settled */ }
  }
}

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
    // Drive the native readableCancel directly rather than readable.cancel():
    // the WHATWG cancel() runs cancel→close, which would surface a pending read
    // as EOF. Per §9.2 a local cancel must *error* the read (default code
    // 'cancelled'); readableCancel errors the read half with our coded reason
    // AND shuts the quiche read side (STOP_SENDING), and — operating on the
    // controller — works whether or not a reader is locked. Idempotent
    // (`_readClosed` guard) and internals-dependent (hence the exact-version pin).
    const s = this.#qs as unknown as { readableCancel?: (reason?: unknown) => void }
    try { s.readableCancel?.(streamError(reason ?? { code: 'cancelled' })) } catch { /* already settled */ }
  }

  async resetWrite(reason?: KpsReason): Promise<void> {
    const w = this.writable.getWriter()
    try { await w.abort(reason?.message ?? reason?.code ?? 'reset') } catch { /* ignore */ } finally { try { w.releaseLock() } catch { /* ignore */ } }
  }

  async close(): Promise<void> {
    // Error any still-open half first so force-destroy surfaces a proper KPS
    // 'closed' reason instead of a bare `null` (kps#3). A pending read/write
    // still rejects — a local close is not a FIN — just with a real Error.
    errorOpenHalves(this.#qs)
    const qs = this.#qs as unknown as { destroy?: (opts?: { force?: boolean }) => Promise<void> }
    try { await qs.destroy?.({ force: true }) } catch { /* ignore */ }
  }
}
