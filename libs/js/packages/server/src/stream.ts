// Server-side Stream: the §6.2 framing (DATA/FIN/RESET/STOP_SENDING) over a
// node-datachannel DataChannel, presented as the transport-neutral core Stream
// (WHATWG readable/writable). Mirrors the browser webrtc-client Stream; only the
// channel API differs (node-datachannel callbacks vs browser RTCDataChannel).

import {
  decodeFrame, encodeData, encodeFin, encodeCode,
  codeToNum, numToCode,
  FRAME_DATA, FRAME_FIN, FRAME_RESET, FRAME_STOP_SENDING, MAX_FRAME_PAYLOAD,
} from '@kpstreams/core/webrtc'
import {
  reasonFrom, streamError,
  type KpsReason, type StreamCloseInfo, type Stream as CoreStream,
} from '@kpstreams/core'
import type { DataChannel } from 'node-datachannel'

const BUFFERED_AMOUNT_LOW = 1 << 20 // 1 MiB

function toBytes(msg: string | Buffer | ArrayBuffer): Uint8Array | null {
  if (typeof msg === 'string') return null // KPS stream frames are binary
  return msg instanceof Uint8Array ? msg : new Uint8Array(msg)
}

export class Stream implements CoreStream {
  readonly readable: ReadableStream<Uint8Array>
  readonly writable: WritableStream<Uint8Array>
  readonly closed: Promise<StreamCloseInfo>

  #dc: DataChannel
  #rc: ReadableStreamDefaultController<Uint8Array> | null = null
  #readEnded = false
  #writeClosed = false
  #peerStop: KpsReason | null = null
  #closeResolve!: (info: StreamCloseInfo) => void
  #closeSettled = false

  constructor(dc: DataChannel) {
    this.#dc = dc
    dc.setBufferedAmountLowThreshold(BUFFERED_AMOUNT_LOW)
    this.closed = new Promise<StreamCloseInfo>(res => { this.#closeResolve = res })

    this.readable = new ReadableStream<Uint8Array>({
      start: (controller) => { this.#rc = controller },
      cancel: (reason) => { void this.cancelRead(reasonFrom(reason) ?? { code: 'cancelled' }) },
    })
    this.writable = new WritableStream<Uint8Array>({
      write: (chunk) => this.#writeChunk(chunk),
      close: () => this.closeWrite(),
      abort: (reason) => this.resetWrite(reasonFrom(reason) ?? { code: 'reset' }),
    })

    dc.onMessage((msg) => this.#onMessage(msg))
    dc.onClosed(() => this.#settle({ ok: true }))
    dc.onError((e) => this.#settle({ ok: false, reason: { code: 'network-error', message: e } }))
  }

  #onMessage(msg: string | Buffer | ArrayBuffer): void {
    const data = toBytes(msg)
    if (!data || data.length === 0) return
    const f = decodeFrame(data)
    switch (f.type) {
      case FRAME_DATA:
        if (!this.#readEnded && this.#rc) { try { this.#rc.enqueue(f.payload.slice()) } catch { /* reader gone */ } }
        break
      case FRAME_FIN:
        this.#endRead(null); break
      case FRAME_RESET:
        this.#endRead({ code: numToCode(f.code) ?? 'reset' }); break
      case FRAME_STOP_SENDING:
        this.#peerStop = { code: numToCode(f.code) ?? 'cancelled' }; this.#writeClosed = true; break
    }
  }

  #endRead(reason: KpsReason | null): void {
    if (this.#readEnded) return
    this.#readEnded = true
    if (this.#rc) {
      try { if (reason) this.#rc.error(streamError(reason)); else this.#rc.close() } catch { /* already closed */ }
    }
  }

  async #writeChunk(chunk: Uint8Array): Promise<void> {
    if (this.#peerStop) throw streamError(this.#peerStop)
    for (let off = 0; off < chunk.length; off += MAX_FRAME_PAYLOAD) {
      if (!this.#dc.isOpen()) throw new Error('kps: stream is closed')
      if (this.#peerStop) throw streamError(this.#peerStop)
      if (this.#dc.bufferedAmount() >= BUFFERED_AMOUNT_LOW) await this.#drain()
      this.#dc.sendMessageBinary(encodeData(chunk.subarray(off, off + MAX_FRAME_PAYLOAD)))
    }
  }

  #drain(): Promise<void> {
    if (this.#dc.bufferedAmount() < BUFFERED_AMOUNT_LOW) return Promise.resolve()
    return new Promise(resolve => { this.#dc.onBufferedAmountLow(() => resolve()) })
  }

  /** Gracefully finish the local write half; the peer observes EOF. */
  async closeWrite(): Promise<void> {
    if (this.#writeClosed) return
    this.#writeClosed = true
    if (this.#dc.isOpen()) this.#dc.sendMessageBinary(encodeFin())
  }

  /** Stop wanting inbound bytes (not EOF); the peer is told to stop sending. */
  async cancelRead(reason?: KpsReason): Promise<void> {
    if (this.#dc.isOpen()) this.#dc.sendMessageBinary(encodeCode(FRAME_STOP_SENDING, codeToNum(reason?.code ?? 'cancelled')))
    this.#endRead(null)
  }

  /** Abort the local write half; the peer observes a stream error, not EOF. */
  async resetWrite(reason?: KpsReason): Promise<void> {
    if (this.#writeClosed) return
    this.#writeClosed = true
    if (this.#dc.isOpen()) this.#dc.sendMessageBinary(encodeCode(FRAME_RESET, codeToNum(reason?.code ?? 'reset')))
  }

  /** Tear down both halves of the stream. */
  async close(reason?: KpsReason): Promise<void> {
    try { await this.closeWrite() } catch { /* ignore */ }
    try { await this.cancelRead(reason ?? { code: 'closed' }) } catch { /* ignore */ }
    try { this.#dc.close() } catch { /* ignore */ }
  }

  #settle(info: StreamCloseInfo): void {
    this.#endRead(info.ok ? null : info.reason ?? { code: 'network-error' })
    if (this.#closeSettled) return
    this.#closeSettled = true
    this.#closeResolve(info)
  }
}
