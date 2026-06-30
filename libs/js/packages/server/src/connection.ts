// Server-side Connection: wraps a node-datachannel PeerConnection for one peer
// and presents the transport-neutral core Connection. Application streams arrive
// as data channels (onDataChannel); the negotiated bootstrap/datagram channels
// never surface as streams. Mirrors the browser webrtc-client Connection.

import { PeerConnection, type DataChannel } from 'node-datachannel'
import { Stream } from './stream.js'
import type {
  Connection as CoreConnection, ConnCloseInfo, Datagrams,
  KpsReason, OpenStreamOptions, AcceptStreamOptions,
} from '@kpstreams/core'

const BOOTSTRAP_LABEL = '_kps_bootstrap'
const DATAGRAM_LABEL = '_kps_datagrams'
const DATAGRAM_ID = 1
// Sub-MTU cap so each datagram is one unreliable SCTP message (matches the Go
// and webrtc-client limits); the limit surfaces via the send error.
const WEBRTC_MAX_DATAGRAM = 1200

function makeDatagrams(dc: DataChannel): Datagrams {
  const MAXQ = 256
  const queue: Uint8Array[] = []
  let waiter: ((v: Uint8Array) => void) | null = null
  dc.onMessage((msg) => {
    const data = typeof msg === 'string'
      ? new TextEncoder().encode(msg)
      : (msg instanceof Uint8Array ? msg : new Uint8Array(msg))
    if (waiter) { const w = waiter; waiter = null; w(data); return }
    queue.push(data)
    if (queue.length > MAXQ) queue.shift() // drop-oldest
  })
  const incoming = new ReadableStream<Uint8Array>({
    pull(controller) {
      const next = queue.shift()
      if (next) { controller.enqueue(next); return }
      return new Promise<void>(resolve => { waiter = (v) => { controller.enqueue(v); resolve() } })
    },
  })
  return {
    async send(data: Uint8Array) {
      if (data.length > WEBRTC_MAX_DATAGRAM) {
        const e = new Error(`kps: datagram exceeds limit (max ${WEBRTC_MAX_DATAGRAM} bytes)`)
        Object.assign(e, { code: 'too-large', maxDatagramPayloadSize: WEBRTC_MAX_DATAGRAM })
        throw e
      }
      if (!dc.isOpen()) throw new Error('kps: datagram channel not open')
      dc.sendMessageBinary(data)
    },
    incoming,
  }
}

export class Connection implements CoreConnection {
  readonly closed: Promise<ConnCloseInfo>
  readonly datagrams: Datagrams
  /** Resolves once the peer connection reaches 'connected'; rejects if it fails first. */
  readonly ready: Promise<void>
  state: 'connecting' | 'open' | 'closed' = 'connecting'

  #pc: PeerConnection
  #seq = 0
  #incoming: Stream[] = []
  #waiters: Array<{ resolve: (s: Stream) => void; reject: (e: Error) => void }> = []
  #closeResolve!: (info: ConnCloseInfo) => void
  #readyResolve!: () => void
  #readyReject!: (e: Error) => void
  #closeFired = false
  #readySettled = false

  constructor(pc: PeerConnection) {
    this.#pc = pc
    this.closed = new Promise<ConnCloseInfo>(res => { this.#closeResolve = res })
    this.ready = new Promise<void>((res, rej) => { this.#readyResolve = res; this.#readyReject = rej })

    // Reserved datagram channel — negotiated on both sides (no DCEP), so it
    // never surfaces as an application stream.
    this.datagrams = makeDatagrams(pc.createDataChannel(DATAGRAM_LABEL, {
      negotiated: true, id: DATAGRAM_ID, unordered: true, maxRetransmits: 0,
    }))

    pc.onStateChange((s) => {
      if (s === 'connected' && this.state === 'connecting') {
        this.state = 'open'
        this.#settleReady(null)
      } else if (s === 'failed') {
        this.#settleReady(new Error('kps: peer connection failed'))
        this.#fireClose({ ok: false, reason: { code: 'network-error', message: 'peer connection failed' } })
      } else if (s === 'closed' || s === 'disconnected') {
        this.#settleReady(new Error('kps: peer connection closed'))
        this.#fireClose({ ok: this.state !== 'connecting' })
      }
    })

    pc.onDataChannel((dc) => {
      const label = dc.getLabel()
      if (label === BOOTSTRAP_LABEL || label === DATAGRAM_LABEL) return
      this.#enqueueIncoming(new Stream(dc))
    })
  }

  async openStream(opts: OpenStreamOptions = {}): Promise<Stream> {
    if (this.state !== 'open') throw new Error(`kps: connection is ${this.state}`)
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
    if (this.state === 'closed') return Promise.reject(new Error('kps: connection is closed'))
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
    if (this.state === 'closed') return
    try { this.#pc.close() } catch { /* ignore */ }
    this.#fireClose({ ok: true, reason })
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
    this.state = 'closed'
    for (const w of this.#waiters) w.reject(new Error('kps: connection closed'))
    this.#waiters = []
    this.#closeResolve(info)
  }
}
