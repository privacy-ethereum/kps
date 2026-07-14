// Server-side Connection: wraps a node-datachannel PeerConnection for one peer
// and presents the transport-neutral core Connection. Application streams arrive
// as data channels (onDataChannel); the negotiated control/datagram channels
// never surface as streams. Mirrors the browser webrtc-client Connection.

import { PeerConnection, type DataChannel } from 'node-datachannel'
import { encodeConnClose, readConnCloseCode, numToCode } from '@kpstreams/core/webrtc'
import { Stream } from './stream.js'
import type {
  Connection as CoreConnection, ConnCloseInfo,
  KpsReason, OpenStreamOptions, AcceptStreamOptions,
} from '@kpstreams/core'

const DATAGRAM_LABEL = '_kps_datagrams'
const DATAGRAM_ID = 1
// Reserved control channel (SPEC §8): negotiated, reliable, ordered, fixed ID 0.
// Forces the SCTP m-line (client side) and carries the CONNECTION_CLOSE message.
const CONTROL_LABEL = '_kps_control'
const CONTROL_ID = 0
// Sub-MTU cap so each datagram is one unreliable SCTP message (matches the Go
// and webrtc-client limits); the limit surfaces via the send error.
const WEBRTC_MAX_DATAGRAM = 1200

// Backs the flat sendDatagram/receiveDatagram API with the reserved unreliable
// channel; `receive` is pull-based (one datagram per call), mirroring Go.
function makeDatagramChannel(dc: DataChannel) {
  const MAXQ = 256
  const queue: Uint8Array[] = []
  const waiters: Array<(v: Uint8Array) => void> = []
  const rejecters: Array<(e: Error) => void> = []
  let closedErr: Error | null = null
  dc.onMessage((msg) => {
    const data = typeof msg === 'string'
      ? new TextEncoder().encode(msg)
      : (msg instanceof Uint8Array ? msg : new Uint8Array(msg))
    const w = waiters.shift()
    if (w) { rejecters.shift(); w(data); return }
    queue.push(data)
    if (queue.length > MAXQ) queue.shift() // drop-oldest
  })
  return {
    async send(data: Uint8Array, opts?: { signal?: AbortSignal }) {
      if (opts?.signal?.aborted) throw new Error('kps: sendDatagram aborted')
      if (data.length > WEBRTC_MAX_DATAGRAM) {
        const e = new Error(`kps: datagram exceeds limit (max ${WEBRTC_MAX_DATAGRAM} bytes)`)
        Object.assign(e, { code: 'too-large', maxDatagramPayloadSize: WEBRTC_MAX_DATAGRAM })
        throw e
      }
      if (!dc.isOpen()) throw new Error('kps: datagram channel not open')
      dc.sendMessageBinary(Buffer.from(data))
    },
    receive(opts?: { signal?: AbortSignal }): Promise<Uint8Array> {
      const next = queue.shift()
      if (next) return Promise.resolve(next)
      if (closedErr) return Promise.reject(closedErr)
      if (opts?.signal?.aborted) return Promise.reject(new Error('kps: receiveDatagram aborted'))
      return new Promise<Uint8Array>((resolve, reject) => {
        const w = (v: Uint8Array) => { opts?.signal?.removeEventListener('abort', onAbort); resolve(v) }
        const rej = (e: Error) => { opts?.signal?.removeEventListener('abort', onAbort); reject(e) }
        const onAbort = () => {
          const i = waiters.indexOf(w)
          if (i >= 0) { waiters.splice(i, 1); rejecters.splice(i, 1) }
          reject(new Error('kps: receiveDatagram aborted'))
        }
        waiters.push(w); rejecters.push(rej)
        opts?.signal?.addEventListener('abort', onAbort, { once: true })
      })
    },
    close(err: Error) {
      closedErr = err
      while (rejecters.length) { waiters.shift(); rejecters.shift()!(err) }
    },
  }
}

export class Connection implements CoreConnection {
  readonly closed: Promise<ConnCloseInfo>
  /** Resolves once the peer connection reaches 'connected'; rejects if it fails first. */
  readonly ready: Promise<void>

  // The client's first STUN source (see the core Connection.remoteAddress doc).
  readonly remoteAddress: { ip: string; port: number }

  #pc: PeerConnection
  #control!: DataChannel
  #dg: ReturnType<typeof makeDatagramChannel>
  #state: 'connecting' | 'open' | 'closed' = 'connecting'
  #seq = 0
  #incoming: Stream[] = []
  #waiters: Array<{ resolve: (s: Stream) => void; reject: (e: Error) => void }> = []
  #closeResolve!: (info: ConnCloseInfo) => void
  #readyResolve!: () => void
  #readyReject!: (e: Error) => void
  #closeFired = false
  #readySettled = false

  constructor(pc: PeerConnection, remote: { ip: string; port: number }) {
    this.#pc = pc
    this.remoteAddress = remote
    this.closed = new Promise<ConnCloseInfo>(res => { this.#closeResolve = res })
    this.ready = new Promise<void>((res, rej) => { this.#readyResolve = res; this.#readyReject = rej })

    // Reserved control channel (SPEC §8): negotiated, reliable. A message on it
    // is a CONNECTION_CLOSE — record the peer's code and tear down.
    this.#control = pc.createDataChannel(CONTROL_LABEL, { negotiated: true, id: CONTROL_ID })
    this.#control.onMessage((msg) => {
      const data = typeof msg === 'string'
        ? new TextEncoder().encode(msg)
        : (msg instanceof Uint8Array ? msg : new Uint8Array(msg))
      const n = readConnCloseCode(data)
      const reason = n === 0 ? undefined : { code: numToCode(n) ?? 'internal-error' as const }
      this.#fireClose({ ok: n === 0, reason })
      try { this.#pc.close() } catch { /* ignore */ }
    })

    // Reserved datagram channel — negotiated on both sides (no DCEP), so it
    // never surfaces as an application stream.
    this.#dg = makeDatagramChannel(pc.createDataChannel(DATAGRAM_LABEL, {
      negotiated: true, id: DATAGRAM_ID, unordered: true, maxRetransmits: 0,
    }))

    pc.onStateChange((s) => {
      if (s === 'connected' && this.#state === 'connecting') {
        this.#state = 'open'
        this.#settleReady(null)
      } else if (s === 'failed') {
        this.#settleReady(new Error('kps: peer connection failed'))
        this.#fireClose({ ok: false, reason: { code: 'network-error', message: 'peer connection failed' } })
      } else if (s === 'closed') {
        this.#settleReady(new Error('kps: peer connection closed'))
        this.#fireClose({ ok: this.#state !== 'connecting' })
      }
      // 'disconnected' is transient; let it recover or escalate to 'failed'.
    })

    pc.onDataChannel((dc) => {
      const label = dc.getLabel()
      if (label === CONTROL_LABEL || label === DATAGRAM_LABEL) return
      this.#enqueueIncoming(new Stream(dc))
    })
  }

  async openStream(opts: OpenStreamOptions = {}): Promise<Stream> {
    if (opts.signal?.aborted) throw new Error('kps: openStream aborted')
    if (this.#state !== 'open') throw new Error(`kps: connection is ${this.#state}`)
    const dc = this.#pc.createDataChannel(`kps-s-${++this.#seq}`)
    const stream = new Stream(dc) // registers handlers immediately
    if (dc.isOpen()) return stream
    return await new Promise<Stream>((resolve, reject) => {
      const onAbort = () => { try { dc.close() } catch {} ; reject(new Error('kps: openStream aborted')) }
      dc.onOpen(() => { opts.signal?.removeEventListener('abort', onAbort); resolve(stream) })
      dc.onError((e) => { opts.signal?.removeEventListener('abort', onAbort); reject(new Error(e)) })
      opts.signal?.addEventListener('abort', onAbort, { once: true })
    })
  }

  acceptStream(opts: AcceptStreamOptions = {}): Promise<Stream> {
    const ready = this.#incoming.shift()
    if (ready) return Promise.resolve(ready)
    if (opts.signal?.aborted) return Promise.reject(new Error('kps: acceptStream aborted'))
    if (this.#state === 'closed') return Promise.reject(new Error('kps: connection is closed'))
    const signal = opts.signal
    return new Promise<Stream>((resolve, reject) => {
      const waiter = {
        resolve: (s: Stream) => { signal?.removeEventListener('abort', onAbort); resolve(s) },
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

  async close(reason?: KpsReason): Promise<void> {
    if (this.#state === 'closed') return
    // Best-effort CONNECTION_CLOSE to the peer before teardown (SPEC §8).
    try {
      if (this.#control.isOpen()) this.#control.sendMessageBinary(Buffer.from(encodeConnClose(reason?.code)))
    } catch { /* ignore */ }
    try { this.#pc.close() } catch { /* ignore */ }
    this.#fireClose({ ok: true, reason })
  }

  // Datagrams (SPEC §7) — unreliable, unordered, best-effort.
  sendDatagram(data: Uint8Array, opts?: { signal?: AbortSignal }): Promise<void> {
    return this.#dg.send(data, opts)
  }

  receiveDatagram(opts?: { signal?: AbortSignal }): Promise<Uint8Array> {
    return this.#dg.receive(opts)
  }

  #enqueueIncoming(stream: Stream): void {
    const w = this.#waiters.shift()
    if (w) w.resolve(stream)
    else this.#incoming.push(stream)
  }

  #settleReady(err: Error | null): void {
    if (this.#readySettled) return
    this.#readySettled = true
    if (err) this.#readyReject(err); else this.#readyResolve()
  }

  #fireClose(info: ConnCloseInfo): void {
    if (this.#closeFired) return
    this.#closeFired = true
    this.#state = 'closed'
    this.#dg.close(new Error('kps: connection closed'))
    for (const w of this.#waiters) w.reject(new Error('kps: connection closed'))
    this.#waiters = []
    this.#closeResolve(info)
  }
}
