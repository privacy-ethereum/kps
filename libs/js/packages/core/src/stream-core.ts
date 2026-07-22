// KpsStream — the shared WebRTC stream engine (SPEC §6.2 framing + §6.5 flow
// control) over a ChannelLike. Both @kpstreams/webrtc-client (browser
// RTCDataChannel) and @kpstreams/server (node-datachannel) instantiate this;
// only their channel adapters differ.
//
// Read side: inbound DATA lands in a KPS-owned buffer whose size is bounded by
// the advertised receive window (enforced by StreamFlow), and is handed to the
// application through a zero-high-water-mark pull ReadableStream — bytes leave
// the buffer only when a read is being fulfilled, which is the consumption
// event that returns credit. Write side: every DATA frame reserves credit
// before it may be sent; `bufferedAmount` remains only a local queue bound.

import {
  parseFrame, encodeData, encodeFin, encodeCode, encodeMaxStreamData,
  codeToNum, numToCode,
  FRAME_RESET, FRAME_STOP_SENDING,
  MAX_FRAME_PAYLOAD,
} from './framing.js'
import { streamError, reasonFrom, type KpsReason } from './errors.js'
import type { StreamCloseInfo, Stream as CoreStream } from './types.js'
import type { ChannelLike } from './channel.js'
import type { ConnFlow, StreamFlow } from './flow.js'

// Local send-queue bound (NOT flow control): above this we stop handing frames
// to the transport until bufferedAmountLow fires.
const LOCAL_SEND_BUFFER_LOW = 1 << 20 // 1 MiB

// Hooks into the owning connection.
export interface StreamHooks {
  /** A wire violation by the peer: the whole connection must fail. */
  fatal(reason: KpsReason): void
  /** Fired once when the stream fully retires (wire-complete + closed + drained). */
  retired(): void
  /** True while the connection is closing/failed (suppresses close policing). */
  isTeardown(): boolean
}

class Wakeable {
  #resolvers: Array<() => void> = []
  wake(): void {
    const rs = this.#resolvers
    this.#resolvers = []
    for (const r of rs) r()
  }
  wait(): Promise<void> {
    return new Promise(res => this.#resolvers.push(res))
  }
}

export class KpsStream implements CoreStream {
  readonly readable: ReadableStream<Uint8Array>
  readonly writable: WritableStream<Uint8Array>
  readonly closed: Promise<StreamCloseInfo>
  /** Resolves when the channel opens; rejects if it dies first. */
  readonly opened: Promise<void>

  #ch: ChannelLike
  #sf: StreamFlow
  #hooks: StreamHooks

  #inbuf: Uint8Array[] = []
  #peerFin = false
  #peerReset: KpsReason | null = null
  #peerStop: KpsReason | null = null
  #localTerminal: 'fin' | 'reset' | null = null
  #readCancelled = false
  // The reason a locally-terminated read half surfaces to a pending/subsequent
  // read. Per SPEC §9.2, EOF is reserved for the peer's FIN; a local
  // cancelRead/close or a connection teardown must make the read *error*.
  #readError: KpsReason | null = null
  #channelClosed = false
  #retiredFired = false

  #readWake = new Wakeable()
  #drainWake = new Wakeable()
  #closeResolve!: (info: StreamCloseInfo) => void
  #closeSettled = false
  #openResolve!: () => void
  #openReject!: (e: Error) => void
  #openSettled = false

  constructor(ch: ChannelLike, connFlow: ConnFlow, hooks: StreamHooks) {
    this.#ch = ch
    this.#hooks = hooks
    // Credit advertisements for this stream travel on this stream's channel.
    this.#sf = connFlow.newStream(v => {
      if (this.#ch.isOpen()) this.#ch.send(encodeMaxStreamData(v))
    })

    this.closed = new Promise<StreamCloseInfo>(res => { this.#closeResolve = res })
    this.opened = new Promise<void>((res, rej) => { this.#openResolve = res; this.#openReject = rej })
    this.opened.catch(() => { /* observed via openStream when it matters */ })

    ch.setBufferedAmountLowThreshold(LOCAL_SEND_BUFFER_LOW)
    ch.onBufferedAmountLow(() => this.#drainWake.wake())
    ch.onOpen(() => this.#settleOpen(null))
    if (ch.isOpen()) this.#settleOpen(null)
    ch.onMessage(d => this.#onFrame(d))
    ch.onClose(() => this.#onChannelClose())
    ch.onError(msg => {
      this.#settle({ ok: false, reason: { code: 'network-error', message: msg } })
      this.#settleOpen(new Error(`kps: stream failed: ${msg}`))
      this.#readWake.wake()
      this.#drainWake.wake()
    })

    this.readable = new ReadableStream<Uint8Array>(
      {
        pull: async (controller) => {
          for (;;) {
            // Local termination of the read half is an error, never EOF (§9.2):
            // cancelRead/close and connection teardown all land here. (A
            // consumer's own reader.cancel() still sees {done:true} — WHATWG
            // closes the stream, so this error() is a late no-op for them.)
            if (this.#readCancelled) { controller.error(streamError(this.#readError ?? { code: 'cancelled' })); return }
            const chunk = this.#inbuf.shift()
            if (chunk) {
              controller.enqueue(chunk)
              // The pull fulfills a pending read (zero HWM): consumption.
              this.#sf.onConsumed(chunk.length)
              this.#maybeRetire()
              return
            }
            if (this.#peerReset) { controller.error(streamError(this.#peerReset)); return }
            if (this.#peerFin) { controller.close(); return }
            if (this.#channelClosed) {
              controller.error(streamError({ code: 'network-error', message: 'kps: stream closed' }))
              return
            }
            await this.#readWake.wait()
          }
        },
        cancel: (reason) => { void this.cancelRead(reasonFrom(reason) ?? { code: 'cancelled' }) }
      },
      { highWaterMark: 0 }
    )

    this.writable = new WritableStream<Uint8Array>({
      write: (chunk) => this.#writeChunk(chunk),
      close: () => this.closeWrite(),
      abort: (reason) => this.resetWrite(reasonFrom(reason) ?? { code: 'reset' })
    })
  }

  // ---- inbound ----

  #onFrame(data: Uint8Array): void {
    let f
    try {
      f = parseFrame(data)
    } catch (e) {
      this.#hooks.fatal({ code: 'protocol-error', message: (e as Error).message })
      return
    }
    switch (f.type) {
      case 'data': {
        if (this.#peerFin || this.#peerReset) {
          this.#hooks.fatal({ code: 'protocol-error', message: 'DATA after terminal frame' })
          return
        }
        try {
          this.#sf.onDataReceived(f.payload.length)
        } catch (e) {
          this.#hooks.fatal({ code: 'protocol-error', message: (e as Error).message })
          return
        }
        if (this.#readCancelled) {
          // In-flight DATA racing our STOP_SENDING: discard = consumed.
          this.#sf.onConsumed(f.payload.length)
          return
        }
        this.#inbuf.push(f.payload.slice()) // copy out of the message buffer
        this.#readWake.wake()
        return
      }
      case 'fin': {
        if (this.#peerFin || this.#peerReset) {
          this.#hooks.fatal({ code: 'protocol-error', message: 'second terminal frame' })
          return
        }
        this.#peerFin = true
        this.#readWake.wake()
        this.#maybeRetire()
        return
      }
      case 'reset': {
        if (this.#peerFin || this.#peerReset) {
          this.#hooks.fatal({ code: 'protocol-error', message: 'second terminal frame' })
          return
        }
        this.#peerReset = { code: numToCode(f.code) ?? 'reset' }
        // QUIC-like reset: discard buffered-but-unread bytes (counts as
        // consumed, releasing connection credit) and surface the error.
        this.#discardInbuf()
        this.#readWake.wake()
        this.#maybeRetire()
        return
      }
      case 'stop-sending': {
        if (this.#peerStop) return // duplicate: ignore
        this.#peerStop = { code: numToCode(f.code) ?? 'cancelled' }
        this.#sf.failSend(streamError(this.#peerStop))
        if (!this.#localTerminal) {
          // No terminal handed to the transport yet: reply with RESET (§6.2).
          this.#localTerminal = 'reset'
          if (this.#ch.isOpen()) this.#ch.send(encodeCode(FRAME_RESET, f.code))
          this.#maybeRetire()
        }
        return
      }
      case 'max-stream-data':
        this.#sf.onPeerMaxStreamData(f.value)
        return
    }
  }

  #onChannelClose(): void {
    this.#channelClosed = true
    const wireComplete = this.#localTerminal !== null && (this.#peerFin || this.#peerReset !== null)
    if (!wireComplete && !this.#hooks.isTeardown()) {
      // §6.5 teardown accounting: a channel disappearing mid-stream leaves
      // connection credit ambiguous — connection-fatal.
      this.#hooks.fatal({ code: 'protocol-error', message: 'data channel closed mid-stream' })
    }
    this.#sf.failSend(streamError({ code: 'network-error', message: 'kps: stream closed' }))
    this.#settleOpen(new Error('kps: stream closed before opening'))
    this.#settle({ ok: !this.#peerReset, reason: this.#peerReset ?? undefined })
    this.#readWake.wake() // buffered data stays readable; EOF/error after it drains
    this.#drainWake.wake()
    this.#maybeRetire()
  }

  // ---- outbound ----

  async #writeChunk(chunk: Uint8Array): Promise<void> {
    let off = 0
    while (off < chunk.length) {
      this.#checkWritable()
      // Credit BEFORE the frame may exist (§6.5). The grant may be partial —
      // frames split at the credit boundary as well as at MAX_FRAME_PAYLOAD.
      const want = Math.min(chunk.length - off, MAX_FRAME_PAYLOAD)
      const granted = await this.#sf.reserve(want)
      const slice = chunk.subarray(off, off + granted)
      try {
        await this.#drainLocal()
        this.#checkWritable()
        this.#ch.send(encodeData(slice))
      } catch (e) {
        this.#sf.release(granted)
        throw e
      }
      this.#sf.commit(granted)
      off += granted
    }
  }

  #checkWritable(): void {
    if (this.#peerStop) throw streamError(this.#peerStop)
    if (this.#localTerminal) throw streamError({ code: 'closed', message: 'kps: write half closed' })
    if (this.#channelClosed || !this.#ch.isOpen()) throw new Error('kps: stream is closed')
  }

  // Local send-queue bound only — flow control is the credit reservation above.
  async #drainLocal(): Promise<void> {
    while (this.#ch.isOpen() && this.#ch.bufferedAmount() >= LOCAL_SEND_BUFFER_LOW) {
      await this.#drainWake.wait()
    }
  }

  // ---- public stream operations ----

  /** Gracefully finish the local write half; the peer observes EOF after all written bytes. */
  async closeWrite(): Promise<void> {
    if (this.#localTerminal) return
    this.#localTerminal = 'fin'
    this.#sf.failSend(streamError({ code: 'closed', message: 'kps: write half closed' }))
    if (this.#ch.isOpen()) this.#ch.send(encodeFin())
    this.#maybeRetire()
  }

  /** Stop wanting inbound bytes (not EOF); the peer is told to stop sending. */
  async cancelRead(reason?: KpsReason): Promise<void> {
    if (this.#readCancelled) return
    this.#readCancelled = true
    this.#readError = reason ?? { code: 'cancelled' }
    this.#sf.markCancelled() // no further stream credit; discards still free MAX_DATA
    this.#discardInbuf()
    if (!this.#peerFin && !this.#peerReset && this.#ch.isOpen()) {
      this.#ch.send(encodeCode(FRAME_STOP_SENDING, codeToNum(reason?.code ?? 'cancelled')))
    }
    this.#readWake.wake()
    this.#maybeRetire()
  }

  /** Abort the local write half; the peer observes a stream error rather than EOF. */
  async resetWrite(reason?: KpsReason): Promise<void> {
    if (this.#localTerminal) return
    this.#localTerminal = 'reset'
    this.#sf.failSend(streamError(reason ?? { code: 'reset' }))
    if (this.#ch.isOpen()) this.#ch.send(encodeCode(FRAME_RESET, codeToNum(reason?.code ?? 'reset')))
    this.#maybeRetire()
  }

  /**
   * Tear down both halves. The channel itself closes at retirement — once the
   * peer's terminal frame (a conforming peer answers STOP_SENDING with RESET)
   * has arrived — because closing it earlier is a §6.5 protocol violation.
   */
  async close(reason?: KpsReason): Promise<void> {
    try { await this.closeWrite() } catch { /* ignore */ }
    try { await this.cancelRead(reason ?? { code: 'closed' }) } catch { /* ignore */ }
  }

  /** Connection teardown: discard state, fail waiters, no wire activity. */
  destroy(reason?: KpsReason): void {
    this.#discardInbuf()
    this.#readCancelled = true
    this.#readError = reason ?? { code: 'closed', message: 'kps: connection closed' }
    this.#sf.failSend(streamError(reason ?? { code: 'closed', message: 'kps: connection closed' }))
    this.#settleOpen(new Error('kps: connection closed'))
    this.#settle(reason ? { ok: false, reason } : { ok: true })
    this.#readWake.wake()
    this.#drainWake.wake()
  }

  // ---- lifecycle ----

  #discardInbuf(): void {
    if (this.#inbuf.length === 0) return
    let n = 0
    for (const c of this.#inbuf) n += c.length
    this.#inbuf = []
    this.#sf.onConsumed(n) // explicit discard is consumption (§6.5)
  }

  #maybeRetire(): void {
    const wireComplete = this.#localTerminal !== null && (this.#peerFin || this.#peerReset !== null)
    if (!wireComplete) return
    const drained = this.#inbuf.length === 0
    if (!drained) return
    if (!this.#channelClosed) {
      // Wire-complete and locally drained: we MUST initiate the close so
      // retirement always makes progress (§6.5).
      this.#ch.close()
      return
    }
    if (!this.#retiredFired) {
      this.#retiredFired = true
      this.#hooks.retired()
    }
  }

  #settle(info: StreamCloseInfo): void {
    if (this.#closeSettled) return
    this.#closeSettled = true
    this.#closeResolve(info)
  }

  #settleOpen(err: Error | null): void {
    if (this.#openSettled) return
    this.#openSettled = true
    if (err) this.#openReject(err)
    else this.#openResolve()
  }
}
