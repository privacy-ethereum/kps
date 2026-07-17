// ConnCore — the shared WebRTC connection engine (SPEC §8 control channel +
// HELLO establishment, §6.5 connection-level flow control, §7 datagrams) over
// ChannelLike adapters. @kpstreams/webrtc-client and @kpstreams/server wrap
// this with their transport-specific setup (SDP/ICE synthesis vs answering)
// and forward peer-connection state changes into it.

import { numToCode } from './framing.js'
import {
  WIRE_VERSION, encodeHello, encodeConnClose, encodeMaxData, encodeMaxStreams, decodeControl,
} from './control.js'
import { ConnFlow, resolveLimits, type FlowLimits } from './flow.js'
import { KpsStream } from './stream-core.js'
import { streamError, type KpsReason } from './errors.js'
import type { ConnCloseInfo, OpenStreamOptions, AcceptStreamOptions } from './types.js'
import type { ChannelLike } from './channel.js'

// Sub-MTU cap so each datagram travels as a single unreliable SCTP message
// (fragmenting an unreliable message multiplies its loss); the limit surfaces
// via the send error (SPEC §7).
export const WEBRTC_MAX_DATAGRAM = 1200

// Reserved channels (SPEC §8).
export const CONTROL_LABEL = '_kps_control'
export const CONTROL_ID = 0
export const DATAGRAM_LABEL = '_kps_datagrams'
export const DATAGRAM_ID = 1

// The pre-HELLO state must be bounded (SPEC §8). Counted from construction, so
// it also covers transport establishment on the accept side; dialers usually
// fail faster via their dial signal.
const DEFAULT_HELLO_TIMEOUT_MS = 15_000

const MAX_DATAGRAM_QUEUE = 256

export interface ConnCoreHost {
  /** The reserved control channel (negotiated, ID 0), already created. */
  control: ChannelLike
  /** The reserved datagram channel (negotiated, ID 1), already created. */
  datagram: ChannelLike
  /** Create an application data channel (DCEP, non-negotiated). */
  openChannel(label: string): ChannelLike
  /** Tear down the peer connection. */
  closeTransport(): void
  limits?: Partial<FlowLimits>
  helloTimeoutMs?: number
}

interface Waiter<T> {
  resolve: (v: T) => void
  reject: (e: Error) => void
}

function raceAbort<T>(p: Promise<T>, signal: AbortSignal | undefined, message: string): Promise<T> {
  if (!signal) return p
  if (signal.aborted) return Promise.reject(new Error(message))
  return new Promise<T>((resolve, reject) => {
    const onAbort = () => reject(new Error(message))
    signal.addEventListener('abort', onAbort, { once: true })
    p.then(
      v => { signal.removeEventListener('abort', onAbort); resolve(v) },
      e => { signal.removeEventListener('abort', onAbort); reject(e) }
    )
  })
}

export class ConnCore {
  readonly closed: Promise<ConnCloseInfo>
  /** Resolves at mutual HELLO; rejects if the connection dies first. */
  readonly established: Promise<void>
  readonly flow: ConnFlow

  #host: ConnCoreHost
  #state: 'connecting' | 'open' | 'closed' = 'connecting'
  #tearingDown = false

  #helloSent = false
  #peerHello: FlowLimits | null = null
  #establishedDone = false
  #establishResolve!: () => void
  #establishReject!: (e: Error) => void
  #helloTimer: ReturnType<typeof setTimeout>

  #seq = 0
  #streams = new Set<KpsStream>()
  #staged: KpsStream[] = []
  #incoming: KpsStream[] = []
  #acceptWaiters: Waiter<KpsStream>[] = []

  #dgQueue: Uint8Array[] = []
  #dgWaiters: Waiter<Uint8Array>[] = []

  #closeResolve!: (info: ConnCloseInfo) => void
  #closeFired = false

  constructor(host: ConnCoreHost) {
    this.#host = host
    this.closed = new Promise<ConnCloseInfo>(res => { this.#closeResolve = res })
    this.established = new Promise<void>((res, rej) => {
      this.#establishResolve = res
      this.#establishReject = rej
    })
    this.established.catch(() => { /* surfaced via dial/ready when awaited */ })

    // Connection-level credit advertisements travel on the control channel.
    this.flow = new ConnFlow(resolveLimits(host.limits), {
      sendMaxData: v => this.#trySendControl(encodeMaxData(v)),
      sendMaxStreams: v => this.#trySendControl(encodeMaxStreams(v))
    })

    host.control.onOpen(() => this.#sendHello())
    host.control.onMessage(d => this.#onControl(d))
    host.control.onClose(() => this.#reservedChannelLost('control'))
    host.control.onError(() => this.#reservedChannelLost('control'))
    if (host.control.isOpen()) this.#sendHello()

    host.datagram.onMessage(d => this.#onDatagram(d))
    host.datagram.onClose(() => this.#reservedChannelLost('datagram'))

    this.#helloTimer = setTimeout(
      () => this.fatal({ code: 'timeout', message: 'kps: HELLO timeout' }),
      host.helloTimeoutMs ?? DEFAULT_HELLO_TIMEOUT_MS
    )
    // Don't hold a Node event loop open for it.
    ;(this.#helloTimer as { unref?: () => void }).unref?.()
  }

  get state(): 'connecting' | 'open' | 'closed' {
    return this.#state
  }

  // ---- control channel ----

  #sendHello(): void {
    if (this.#helloSent || this.#closeFired) return
    this.#helloSent = true
    this.#trySendControl(encodeHello(this.flow.local))
    this.#checkEstablished()
  }

  #onControl(data: Uint8Array): void {
    let m
    try {
      m = decodeControl(data)
    } catch (e) {
      this.fatal({ code: 'protocol-error', message: (e as Error).message })
      return
    }
    switch (m.t) {
      case 'hello': {
        if (this.#peerHello) {
          this.fatal({ code: 'protocol-error', message: 'duplicate HELLO' })
          return
        }
        if (m.version !== WIRE_VERSION) {
          this.#trySendControl(encodeConnClose('unsupported'))
          this.#teardown({
            ok: false,
            reason: { code: 'unsupported', message: `kps: peer wire version ${m.version} (want ${WIRE_VERSION})` }
          })
          return
        }
        this.#peerHello = m.limits
        this.flow.onPeerHello(m.limits)
        this.#checkEstablished()
        return
      }
      case 'close': {
        // Valid at any time — before HELLO it is a handshake rejection (§8).
        const reason = m.code === 0 ? undefined : { code: numToCode(m.code) ?? ('internal-error' as const) }
        this.#teardown({ ok: m.code === 0, reason })
        return
      }
      case 'max-data':
        if (!this.#peerHello) {
          this.fatal({ code: 'protocol-error', message: 'control message before HELLO' })
          return
        }
        this.flow.onPeerMaxData(m.value)
        return
      case 'max-streams':
        if (!this.#peerHello) {
          this.fatal({ code: 'protocol-error', message: 'control message before HELLO' })
          return
        }
        this.flow.onPeerMaxStreams(m.value)
        return
    }
  }

  #checkEstablished(): void {
    if (this.#establishedDone || this.#closeFired) return
    if (!this.#helloSent || !this.#peerHello) return
    this.#establishedDone = true
    this.#state = 'open'
    clearTimeout(this.#helloTimer)
    this.#establishResolve()
    // Surface streams staged while waiting for the peer's HELLO.
    const staged = this.#staged
    this.#staged = []
    for (const s of staged) this.#enqueueIncoming(s)
  }

  #trySendControl(msg: Uint8Array): void {
    try {
      if (this.#host.control.isOpen()) this.#host.control.send(msg)
    } catch { /* teardown races are fine; close delivery is best-effort */ }
  }

  #reservedChannelLost(which: string): void {
    if (this.#tearingDown || this.#closeFired) return
    // Loss of a reserved channel while the connection is healthy is fatal (§8).
    this.fatal({ code: 'protocol-error', message: `kps: reserved ${which} channel lost` })
  }

  // ---- streams ----

  /** The wrapper calls this for every incoming (DCEP) application channel. */
  handleIncomingChannel(ch: ChannelLike): void {
    try {
      this.flow.peerStreamOpened() // counts a slot even unaccepted/pre-HELLO
    } catch (e) {
      this.fatal({ code: 'protocol-error', message: (e as Error).message })
      return
    }
    const stream = this.#makeStream(ch, true)
    if (this.#establishedDone) this.#enqueueIncoming(stream)
    else this.#staged.push(stream) // staged until mutual HELLO (§8)
  }

  #makeStream(ch: ChannelLike, peerInitiated: boolean): KpsStream {
    const stream = new KpsStream(ch, this.flow, {
      fatal: r => this.fatal(r),
      retired: () => {
        this.#streams.delete(stream)
        // Only peer-initiated streams return MAX_STREAMS credit (§6.5).
        if (peerInitiated) this.flow.peerStreamRetired()
      },
      isTeardown: () => this.#tearingDown
    })
    this.#streams.add(stream)
    return stream
  }

  #enqueueIncoming(stream: KpsStream): void {
    const w = this.#acceptWaiters.shift()
    if (w) w.resolve(stream)
    else this.#incoming.push(stream)
  }

  async openStream(opts: OpenStreamOptions = {}): Promise<KpsStream> {
    if (opts.signal?.aborted) throw new Error('kps: openStream aborted')
    if (this.#state !== 'open') throw new Error(`kps: connection is ${this.#state}`)
    // Stream-count credit: reserve a slot (waits at the peer's limit), commit
    // on successful channel creation.
    await this.flow.reserveStreamSlot(opts.signal)
    let ch: ChannelLike
    try {
      ch = this.#host.openChannel(`kps-${++this.#seq}`)
    } catch (e) {
      this.flow.releaseStreamSlot()
      throw e
    }
    this.flow.commitStreamSlot()
    const stream = this.#makeStream(ch, false)
    try {
      await raceAbort(stream.opened, opts.signal, 'kps: openStream aborted')
    } catch (e) {
      // Abandon via the stream lifecycle (RESET + STOP_SENDING once open, then
      // retirement closes the channel) — closing a non-wire-complete channel
      // directly would be a §6.5 protocol violation against the peer.
      stream.opened
        .then(() => { void stream.resetWrite({ code: 'cancelled' }); void stream.cancelRead({ code: 'cancelled' }) })
        .catch(() => { /* channel never opened; connection-level cleanup applies */ })
      throw e
    }
    return stream
  }

  acceptStream(opts: AcceptStreamOptions = {}): Promise<KpsStream> {
    const ready = this.#incoming.shift()
    if (ready) return Promise.resolve(ready)
    if (opts.signal?.aborted) return Promise.reject(new Error('kps: acceptStream aborted'))
    if (this.#state === 'closed') return Promise.reject(new Error('kps: connection is closed'))
    const signal = opts.signal
    return new Promise<KpsStream>((resolve, reject) => {
      const waiter: Waiter<KpsStream> = {
        resolve: s => { signal?.removeEventListener('abort', onAbort); resolve(s) },
        reject: e => { signal?.removeEventListener('abort', onAbort); reject(e) }
      }
      const onAbort = () => {
        const i = this.#acceptWaiters.indexOf(waiter)
        if (i >= 0) this.#acceptWaiters.splice(i, 1)
        reject(new Error('kps: acceptStream aborted'))
      }
      this.#acceptWaiters.push(waiter)
      signal?.addEventListener('abort', onAbort, { once: true })
    })
  }

  // ---- datagrams (SPEC §7) ----

  #onDatagram(data: Uint8Array): void {
    const w = this.#dgWaiters.shift()
    if (w) {
      w.resolve(data)
      return
    }
    this.#dgQueue.push(data)
    if (this.#dgQueue.length > MAX_DATAGRAM_QUEUE) this.#dgQueue.shift() // drop-oldest
  }

  async sendDatagram(data: Uint8Array, opts?: { signal?: AbortSignal }): Promise<void> {
    if (opts?.signal?.aborted) throw new Error('kps: sendDatagram aborted')
    if (data.length > WEBRTC_MAX_DATAGRAM) {
      const e = new Error(`kps: datagram exceeds limit (max ${WEBRTC_MAX_DATAGRAM} bytes)`)
      Object.assign(e, { code: 'too-large', maxDatagramPayloadSize: WEBRTC_MAX_DATAGRAM })
      throw e
    }
    if (!this.#host.datagram.isOpen()) throw new Error('kps: datagram channel not open')
    this.#host.datagram.send(data)
  }

  receiveDatagram(opts?: { signal?: AbortSignal }): Promise<Uint8Array> {
    const next = this.#dgQueue.shift()
    if (next) return Promise.resolve(next)
    if (this.#state === 'closed') return Promise.reject(new Error('kps: connection closed'))
    if (opts?.signal?.aborted) return Promise.reject(new Error('kps: receiveDatagram aborted'))
    const signal = opts?.signal
    return new Promise<Uint8Array>((resolve, reject) => {
      const waiter: Waiter<Uint8Array> = {
        resolve: v => { signal?.removeEventListener('abort', onAbort); resolve(v) },
        reject: e => { signal?.removeEventListener('abort', onAbort); reject(e) }
      }
      const onAbort = () => {
        const i = this.#dgWaiters.indexOf(waiter)
        if (i >= 0) this.#dgWaiters.splice(i, 1)
        reject(new Error('kps: receiveDatagram aborted'))
      }
      this.#dgWaiters.push(waiter)
      signal?.addEventListener('abort', onAbort, { once: true })
    })
  }

  // ---- close paths ----

  /** Graceful local close: best-effort CONNECTION_CLOSE, then teardown. */
  close(reason?: KpsReason): void {
    if (this.#closeFired) return
    this.#tearingDown = true
    this.#trySendControl(encodeConnClose(reason?.code))
    this.#teardown({ ok: true, reason })
  }

  /** A peer wire violation or local fatal condition: convey a code, tear down. */
  fatal(reason: KpsReason): void {
    if (this.#closeFired) return
    this.#tearingDown = true
    this.#trySendControl(encodeConnClose(reason.code ?? 'protocol-error'))
    this.#teardown({ ok: false, reason })
  }

  /** Transport-layer state changes, forwarded by the wrapper. */
  onTransportFailed(message = 'peer connection failed'): void {
    this.#teardown({ ok: false, reason: { code: 'network-error', message } })
  }

  onTransportClosed(): void {
    this.#teardown({ ok: this.#state !== 'connecting' })
  }

  #teardown(info: ConnCloseInfo): void {
    if (this.#closeFired) return
    this.#closeFired = true
    this.#tearingDown = true
    this.#state = 'closed'
    clearTimeout(this.#helloTimer)
    const err = info.reason ? streamError(info.reason) : new Error('kps: connection closed')
    if (!this.#establishedDone) this.#establishReject(err)
    this.flow.fail(err)
    for (const s of [...this.#streams, ...this.#staged]) s.destroy(info.reason)
    this.#streams.clear()
    this.#staged = []
    for (const w of this.#acceptWaiters) w.reject(new Error('kps: connection closed'))
    this.#acceptWaiters = []
    for (const w of this.#dgWaiters) w.reject(new Error('kps: connection closed'))
    this.#dgWaiters = []
    try { this.#host.closeTransport() } catch { /* ignore */ }
    this.#closeResolve(info)
  }
}
