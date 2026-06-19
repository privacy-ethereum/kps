// Connection — a kps session to a single server (SPEC §4). Holds an
// RTCPeerConnection and exposes openStream() / acceptStream() / close(). Streams
// are unnamed; the data-channel label is a non-semantic implementation detail.

import { parseAddress } from './address.js'
import { decodeCerthash } from './certhash.js'
import { generateUfrag, deriveICEPwd, rewriteOfferUfrag, synthesizeAnswer } from './sdp.js'
import { Stream } from './stream.js'
import type { KpsReason } from './framing.js'

export interface DialOptions {
  signal?: AbortSignal
  timeoutMs?: number
}

export interface ConnCloseInfo {
  ok: boolean
  reason?: KpsReason
}

// Datagrams (SPEC §7) — capability gated. Always present; unsupported in v0 over
// WebRTC, signalled by maxSize 0 and a send() that rejects.
export interface Datagrams {
  // Send one unreliable, unordered datagram. There is a per-connection size
  // limit (transport/path dependent); an oversized send rejects with an error
  // carrying `code: 'too-large'` and `maxDatagramPayloadSize`. Payloads up to
  // ~1100 bytes are safe on every connection.
  send(data: Uint8Array, opts?: { signal?: AbortSignal }): Promise<void>
  readonly incoming: ReadableStream<Uint8Array>
}

const DEFAULT_TIMEOUT = 15_000
// Bootstrap channel: negotiated on both sides (no DCEP), so it never surfaces as
// a server-side stream. Its only job is to force the SCTP m-line into the offer.
const BOOTSTRAP_LABEL = '_kps_bootstrap'
const BOOTSTRAP_ID = 0
// Reserved datagram channel (SPEC §7/§8): negotiated, unreliable, unordered.
const DATAGRAM_LABEL = '_kps_datagrams'
const DATAGRAM_ID = 1
// Cap WebRTC datagrams to a sub-MTU size so each travels as a single unreliable
// SCTP message (matches the Go webrtcMaxDatagram). The limit surfaces via the
// send error; ~1100 bytes is safe on any connection.
const WEBRTC_MAX_DATAGRAM = 1200

function bytesToArrayBuffer(u8: Uint8Array): ArrayBuffer {
  return u8.buffer.slice(u8.byteOffset, u8.byteOffset + u8.byteLength) as ArrayBuffer
}

// makeDatagrams backs the Datagrams API with the reserved unreliable channel.
// Inbound datagrams use a bounded buffer (drop-oldest when full); delivery is
// best-effort.
function makeDatagrams(dg: RTCDataChannel): Datagrams {
  dg.binaryType = 'arraybuffer'
  const MAXQ = 256
  const queue: Uint8Array[] = []
  let waiter: ((v: Uint8Array) => void) | null = null
  dg.addEventListener('message', (e) => {
    const raw = (e as MessageEvent).data as ArrayBuffer | string
    const data = typeof raw === 'string' ? new TextEncoder().encode(raw) : new Uint8Array(raw)
    if (waiter) { const w = waiter; waiter = null; w(data); return }
    queue.push(data)
    if (queue.length > MAXQ) queue.shift() // drop-oldest
  })
  const incoming = new ReadableStream<Uint8Array>({
    pull(controller) {
      const next = queue.shift()
      if (next) { controller.enqueue(next); return }
      return new Promise<void>(resolve => {
        waiter = (v) => { controller.enqueue(v); resolve() }
      })
    }
  })
  return {
    async send(data: Uint8Array) {
      if (data.length > WEBRTC_MAX_DATAGRAM) {
        const e = new Error(`kps: datagram exceeds limit (max ${WEBRTC_MAX_DATAGRAM} bytes)`)
        Object.assign(e, { code: 'too-large', maxDatagramPayloadSize: WEBRTC_MAX_DATAGRAM })
        throw e
      }
      if (dg.readyState !== 'open') throw new Error('kps: datagram channel not open')
      dg.send(bytesToArrayBuffer(data))
    },
    incoming
  }
}

export class Connection {
  readonly closed: Promise<ConnCloseInfo>
  readonly datagrams: Datagrams
  state: 'connecting' | 'open' | 'closed' = 'connecting'

  #pc: RTCPeerConnection
  #streamSeq = 0
  #incoming: Stream[] = []
  #acceptWaiters: Array<{ resolve: (s: Stream) => void; reject: (e: Error) => void }> = []
  #closeResolve!: (info: ConnCloseInfo) => void
  #closeFired = false

  private constructor(pc: RTCPeerConnection) {
    this.#pc = pc
    this.closed = new Promise<ConnCloseInfo>(res => { this.#closeResolve = res })

    // Reserved datagram channel — negotiated on both sides, so it carries
    // datagrams without DCEP and never surfaces as an application stream.
    this.datagrams = makeDatagrams(pc.createDataChannel(DATAGRAM_LABEL, {
      negotiated: true, id: DATAGRAM_ID, ordered: false, maxRetransmits: 0
    }))

    pc.addEventListener('connectionstatechange', () => {
      const s = pc.connectionState
      if (s === 'connected' && this.state === 'connecting') {
        this.state = 'open'
      } else if (s === 'failed') {
        this.#fireClose({ ok: false, reason: { code: 'network-error', message: 'peer connection failed' } })
      } else if (s === 'closed' || s === 'disconnected') {
        this.#fireClose({ ok: this.state !== 'connecting' })
      }
    })

    pc.addEventListener('datachannel', (e: RTCDataChannelEvent) => {
      const channel = e.channel
      if (channel.label === BOOTSTRAP_LABEL) return
      this.#enqueueIncoming(new Stream(channel))
    })
  }

  static async dial(addrStr: string, opts: DialOptions = {}): Promise<Connection> {
    const addr = parseAddress(addrStr)
    const digest = decodeCerthash(addr.certhash)
    const pc = new RTCPeerConnection({})

    // Pre-allocate the negotiated bootstrap channel so the offer carries the
    // application m-line and SCTP comes up.
    pc.createDataChannel(BOOTSTRAP_LABEL, { negotiated: true, id: BOOTSTRAP_ID })

    const offer = await pc.createOffer()
    const ufrag = generateUfrag()
    const pwd = await deriveICEPwd(digest, ufrag)
    await pc.setLocalDescription({ type: offer.type, sdp: rewriteOfferUfrag(offer.sdp ?? '', ufrag, pwd) })
    await pc.setRemoteDescription({ type: 'answer', sdp: synthesizeAnswer(addr, ufrag, pwd) })

    const conn = new Connection(pc)
    await conn.#waitForOpen(opts.timeoutMs ?? DEFAULT_TIMEOUT, opts.signal)
    return conn
  }

  // Open a new unnamed bidirectional byte stream.
  async openStream(opts: { signal?: AbortSignal } = {}): Promise<Stream> {
    if (this.state !== 'open') throw new Error(`kps: connection is ${this.state}`)
    const label = `kps-${++this.#streamSeq}`
    const channel = this.#pc.createDataChannel(label)
    return await new Promise<Stream>((resolve, reject) => {
      const onAbort = () => { cleanup(); try { channel.close() } catch {} ; reject(new Error('kps: openStream aborted')) }
      const onOpen = () => { cleanup(); resolve(new Stream(channel)) }
      const onError = (e: Event) => { cleanup(); reject((e as RTCErrorEvent).error ?? new Error('kps: openStream failed')) }
      const cleanup = () => {
        channel.removeEventListener('open', onOpen)
        channel.removeEventListener('error', onError)
        opts.signal?.removeEventListener('abort', onAbort)
      }
      channel.addEventListener('open', onOpen, { once: true })
      channel.addEventListener('error', onError, { once: true })
      opts.signal?.addEventListener('abort', onAbort, { once: true })
    })
  }

  // Accept the next stream opened by the peer (pull-based, symmetric with Go's
  // AcceptStream).
  acceptStream(opts: { signal?: AbortSignal } = {}): Promise<Stream> {
    const ready = this.#incoming.shift()
    if (ready) return Promise.resolve(ready)
    if (this.state === 'closed') return Promise.reject(new Error('kps: connection is closed'))
    const signal = opts.signal
    return new Promise<Stream>((resolve, reject) => {
      let waiter: { resolve: (s: Stream) => void; reject: (e: Error) => void }
      const onAbort = () => {
        const i = this.#acceptWaiters.indexOf(waiter)
        if (i >= 0) this.#acceptWaiters.splice(i, 1)
        signal?.removeEventListener('abort', onAbort)
        reject(new Error('kps: acceptStream aborted'))
      }
      waiter = {
        resolve: (s: Stream) => { signal?.removeEventListener('abort', onAbort); resolve(s) },
        reject: (e: Error) => { signal?.removeEventListener('abort', onAbort); reject(e) }
      }
      this.#acceptWaiters.push(waiter)
      signal?.addEventListener('abort', onAbort, { once: true })
    })
  }

  async close(reason?: KpsReason): Promise<void> {
    if (this.state === 'closed') return
    this.#pc.close()
    this.#fireClose({ ok: true, reason })
  }

  #enqueueIncoming(stream: Stream): void {
    const w = this.#acceptWaiters.shift()
    if (w) w.resolve(stream)
    else this.#incoming.push(stream)
  }

  #waitForOpen(timeoutMs: number, signal?: AbortSignal): Promise<void> {
    return new Promise((resolve, reject) => {
      if (this.state === 'open') return resolve()
      const timer = setTimeout(() => {
        cleanup(); try { this.#pc.close() } catch {}
        reject(new Error(`kps: dial timed out after ${timeoutMs}ms`))
      }, timeoutMs)
      const onState = () => {
        if (this.#pc.connectionState === 'connected') { cleanup(); resolve() }
      }
      const onAbort = () => { cleanup(); try { this.#pc.close() } catch {} ; reject(new Error('kps: dial aborted')) }
      const cleanup = () => {
        clearTimeout(timer)
        this.#pc.removeEventListener('connectionstatechange', onState)
        signal?.removeEventListener('abort', onAbort)
      }
      this.#pc.addEventListener('connectionstatechange', onState)
      this.closed.then(() => { cleanup(); reject(new Error('kps: connection closed during dial')) }).catch(() => {})
      signal?.addEventListener('abort', onAbort, { once: true })
    })
  }

  #fireClose(info: ConnCloseInfo): void {
    if (this.#closeFired) return
    this.#closeFired = true
    this.state = 'closed'
    for (const w of this.#acceptWaiters) w.reject(new Error('kps: connection closed'))
    this.#acceptWaiters = []
    this.#closeResolve(info)
  }
}

export function dial(addr: string, opts?: DialOptions): Promise<Connection> {
  return Connection.dial(addr, opts)
}
