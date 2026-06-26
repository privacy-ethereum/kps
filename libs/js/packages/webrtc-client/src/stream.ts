// Stream — an unnamed, bidirectional, reliable, ordered byte stream over a kps
// connection (SPEC §6). Byte-oriented: no message boundaries. Inbound/outbound
// bytes are exposed as WHATWG ReadableStream/WritableStream; the §6.2 framing
// (DATA/FIN/RESET/STOP_SENDING) over the data channel is internal.

import {
  decodeFrame, encodeData, encodeFin, encodeCode,
  codeToNum, numToCode,
  FRAME_DATA, FRAME_FIN, FRAME_RESET, FRAME_STOP_SENDING,
  MAX_FRAME_PAYLOAD,
} from '@kpstreams/core/webrtc'
import {
  reasonFrom, streamError,
  type KpsReason, type StreamCloseInfo, type Stream as CoreStream,
} from '@kpstreams/core'

const BUFFERED_AMOUNT_LOW = 1 << 20 // 1 MiB

// RTCDataChannel.send wants an ArrayBuffer-backed view; copy to a fresh,
// exactly-sized ArrayBuffer (also detaches from any SharedArrayBuffer typing).
function toArrayBuffer(u8: Uint8Array): ArrayBuffer {
  return u8.buffer.slice(u8.byteOffset, u8.byteOffset + u8.byteLength) as ArrayBuffer
}

export class Stream implements CoreStream {
  readonly readable: ReadableStream<Uint8Array>
  readonly writable: WritableStream<Uint8Array>
  readonly closed: Promise<StreamCloseInfo>

  #channel: RTCDataChannel
  #rc: ReadableStreamDefaultController<Uint8Array> | null = null
  #readEnded = false
  #writeClosed = false
  #peerStop: KpsReason | null = null
  #closeResolve!: (info: StreamCloseInfo) => void
  #closeSettled = false

  constructor(channel: RTCDataChannel) {
    this.#channel = channel
    channel.binaryType = 'arraybuffer'
    channel.bufferedAmountLowThreshold = BUFFERED_AMOUNT_LOW

    this.closed = new Promise<StreamCloseInfo>(res => { this.#closeResolve = res })

    this.readable = new ReadableStream<Uint8Array>({
      start: (controller) => { this.#rc = controller },
      cancel: (reason) => { void this.cancelRead(reasonFrom(reason) ?? { code: 'cancelled' }) }
    })

    this.writable = new WritableStream<Uint8Array>({
      write: (chunk) => this.#writeChunk(chunk),
      close: () => this.closeWrite(),
      abort: (reason) => this.resetWrite(reasonFrom(reason) ?? { code: 'reset' })
    })

    channel.addEventListener('message', (e) => this.#onMessage(e as MessageEvent))
    channel.addEventListener('close', () => this.#settle({ ok: true }))
    channel.addEventListener('error', (e) => {
      const err = (e as RTCErrorEvent).error
      this.#settle({ ok: false, reason: { code: 'network-error', message: err?.message } })
    })
  }

  #onMessage(e: MessageEvent): void {
    const raw = e.data as ArrayBuffer | string
    const data = typeof raw === 'string' ? new TextEncoder().encode(raw) : new Uint8Array(raw)
    if (data.length === 0) return
    const f = decodeFrame(data)
    switch (f.type) {
      case FRAME_DATA:
        if (!this.#readEnded && this.#rc) {
          try { this.#rc.enqueue(f.payload.slice()) } catch { /* reader gone */ }
        }
        break
      case FRAME_FIN:
        this.#endRead(null)
        break
      case FRAME_RESET:
        this.#endRead({ code: numToCode(f.code) ?? 'reset' })
        break
      case FRAME_STOP_SENDING:
        this.#peerStop = { code: numToCode(f.code) ?? 'cancelled' }
        this.#writeClosed = true
        break
    }
  }

  #endRead(reason: KpsReason | null): void {
    if (this.#readEnded) return
    this.#readEnded = true
    if (this.#rc) {
      try {
        if (reason) this.#rc.error(streamError(reason))
        else this.#rc.close()
      } catch { /* already closed */ }
    }
  }

  async #writeChunk(chunk: Uint8Array): Promise<void> {
    if (this.#peerStop) throw streamError(this.#peerStop)
    for (let off = 0; off < chunk.length; off += MAX_FRAME_PAYLOAD) {
      if (this.#channel.readyState !== 'open') throw new Error(`kps: stream is ${this.#channel.readyState}`)
      if (this.#peerStop) throw streamError(this.#peerStop)
      if (this.#channel.bufferedAmount >= BUFFERED_AMOUNT_LOW) await this.#drain()
      this.#channel.send(toArrayBuffer(encodeData(chunk.subarray(off, off + MAX_FRAME_PAYLOAD))))
    }
  }

  #drain(): Promise<void> {
    if (this.#channel.bufferedAmount < BUFFERED_AMOUNT_LOW) return Promise.resolve()
    return new Promise(resolve => {
      const onLow = () => { this.#channel.removeEventListener('bufferedamountlow', onLow); resolve() }
      this.#channel.addEventListener('bufferedamountlow', onLow)
    })
  }

  /** Gracefully finish the local write half; the peer observes EOF after all written bytes. */
  async closeWrite(): Promise<void> {
    if (this.#writeClosed) return
    this.#writeClosed = true
    if (this.#channel.readyState === 'open') this.#channel.send(toArrayBuffer(encodeFin()))
  }

  /** Stop wanting inbound bytes (not EOF); the peer is told to stop sending. */
  async cancelRead(reason?: KpsReason): Promise<void> {
    if (this.#channel.readyState === 'open') {
      this.#channel.send(toArrayBuffer(encodeCode(FRAME_STOP_SENDING, codeToNum(reason?.code ?? 'cancelled'))))
    }
    this.#endRead(null)
  }

  /** Abort the local write half; the peer observes a stream error rather than EOF. */
  async resetWrite(reason?: KpsReason): Promise<void> {
    if (this.#writeClosed) return
    this.#writeClosed = true
    if (this.#channel.readyState === 'open') {
      this.#channel.send(toArrayBuffer(encodeCode(FRAME_RESET, codeToNum(reason?.code ?? 'reset'))))
    }
  }

  /** Tear down both halves of the stream. */
  async close(reason?: KpsReason): Promise<void> {
    try { await this.closeWrite() } catch { /* ignore */ }
    try { await this.cancelRead(reason ?? { code: 'closed' }) } catch { /* ignore */ }
    try { this.#channel.close() } catch { /* ignore */ }
  }

  #settle(info: StreamCloseInfo): void {
    this.#endRead(info.ok ? null : info.reason ?? { code: 'network-error' })
    if (this.#closeSettled) return
    this.#closeSettled = true
    this.#closeResolve(info)
  }
}
