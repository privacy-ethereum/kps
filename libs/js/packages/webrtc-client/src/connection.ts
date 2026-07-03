// Connection — a kps session to a single server (SPEC §4). Holds an
// RTCPeerConnection and exposes openStream() / acceptStream() / close(). Streams
// are unnamed; the data-channel label is a non-semantic implementation detail.

import { parseAddress, decodeCerthash } from '@kpstreams/core'
import {
  generateUfrag, deriveICEPwd, rewriteOfferUfrag, synthesizeAnswer,
  encodeConnClose, readConnCloseCode, numToCode,
} from '@kpstreams/core/webrtc'
import { Stream } from './stream.js'
import type {
  KpsReason, DialOptions, ConnCloseInfo,
  Connection as CoreConnection,
} from '@kpstreams/core'

const DEFAULT_TIMEOUT = 15_000
// Reserved control channel (SPEC §8): negotiated, reliable, ordered, fixed ID 0.
// Created before the offer so the offer carries the SCTP m-line, and also carries
// the CONNECTION_CLOSE message. Never surfaces as an application stream.
const CONTROL_LABEL = '_kps_control'
const CONTROL_ID = 0
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

// An aborted dial signal — distinguish a timeout (AbortSignal.timeout →
// DOMException "TimeoutError") from an explicit cancel for a clearer message.
function dialAbortError(signal: AbortSignal): Error {
  const reason = signal.reason as { name?: string } | undefined
  return new Error(reason?.name === 'TimeoutError' ? 'kps: dial timed out' : 'kps: dial aborted')
}

// makeDatagramChannel backs the flat sendDatagram/receiveDatagram API with the
// reserved unreliable channel. Inbound datagrams use a bounded buffer (drop-
// oldest when full); delivery is best-effort. `receive` is pull-based (one
// datagram per call), mirroring Go's ReceiveDatagram(ctx).
function makeDatagramChannel(dg: RTCDataChannel) {
  dg.binaryType = 'arraybuffer'
  const MAXQ = 256
  const queue: Uint8Array[] = []
  const waiters: Array<(v: Uint8Array) => void> = []
  let closedErr: Error | null = null
  const rejecters: Array<(e: Error) => void> = []
  dg.addEventListener('message', (e) => {
    const raw = (e as MessageEvent).data as ArrayBuffer | string
    const data = typeof raw === 'string' ? new TextEncoder().encode(raw) : new Uint8Array(raw)
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
      if (dg.readyState !== 'open') throw new Error('kps: datagram channel not open')
      dg.send(bytesToArrayBuffer(data))
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
    // Fail any pending receivers when the connection goes away.
    close(err: Error) {
      closedErr = err
      while (rejecters.length) { waiters.shift(); rejecters.shift()!(err) }
    },
  }
}

export class Connection implements CoreConnection {
  readonly closed: Promise<ConnCloseInfo>

  #pc: RTCPeerConnection
  #control!: RTCDataChannel
  #dg: ReturnType<typeof makeDatagramChannel>
  #state: 'connecting' | 'open' | 'closed' = 'connecting'
  #streamSeq = 0
  #incoming: Stream[] = []
  #acceptWaiters: Array<{ resolve: (s: Stream) => void; reject: (e: Error) => void }> = []
  #closeResolve!: (info: ConnCloseInfo) => void
  #closeFired = false

  // `control` is the reserved reliable channel (ID 0) dial() created before the
  // offer (to force the SCTP m-line); it also carries CONNECTION_CLOSE.
  private constructor(pc: RTCPeerConnection, control: RTCDataChannel) {
    this.#pc = pc
    this.closed = new Promise<ConnCloseInfo>(res => { this.#closeResolve = res })

    // A message on the control channel is a CONNECTION_CLOSE — record the peer's
    // code as the close reason, then tear down.
    this.#control = control
    this.#control.binaryType = 'arraybuffer'
    this.#control.addEventListener('message', (e) => {
      const data = new Uint8Array((e as MessageEvent).data as ArrayBuffer)
      const n = readConnCloseCode(data)
      const reason = n === 0 ? undefined : { code: numToCode(n) ?? 'internal-error' as const }
      this.#fireClose({ ok: n === 0, reason })
      try { this.#pc.close() } catch { /* ignore */ }
    })

    // Reserved datagram channel — negotiated on both sides, so it carries
    // datagrams without DCEP and never surfaces as an application stream.
    this.#dg = makeDatagramChannel(pc.createDataChannel(DATAGRAM_LABEL, {
      negotiated: true, id: DATAGRAM_ID, ordered: false, maxRetransmits: 0
    }))

    pc.addEventListener('connectionstatechange', () => {
      const s = pc.connectionState
      if (s === 'connected' && this.#state === 'connecting') {
        this.#state = 'open'
      } else if (s === 'failed') {
        this.#fireClose({ ok: false, reason: { code: 'network-error', message: 'peer connection failed' } })
      } else if (s === 'closed') {
        this.#fireClose({ ok: this.#state !== 'connecting' })
      }
      // 'disconnected' is transient (a packet-loss blip that often recovers to
      // 'connected'); don't tear down. If it doesn't recover, the state machine
      // escalates to 'failed' on its own, which we handle above.
    })

    pc.addEventListener('datachannel', (e: RTCDataChannelEvent) => {
      const channel = e.channel
      if (channel.label === CONTROL_LABEL) return
      this.#enqueueIncoming(new Stream(channel))
    })
  }

  static async dial(addrStr: string, opts: DialOptions = {}): Promise<Connection> {
    // Timeout is expressed via the signal (SPEC/DialOptions): a caller-supplied
    // signal owns the deadline; otherwise apply the default timeout so a dial
    // can't hang forever.
    const signal = opts.signal ?? AbortSignal.timeout(DEFAULT_TIMEOUT)
    if (signal.aborted) throw dialAbortError(signal)
    const addr = parseAddress(addrStr)
    const digest = decodeCerthash(addr.certhash)
    const pc = new RTCPeerConnection({})

    // Pre-allocate the negotiated control channel (ID 0) before the offer so the
    // offer carries the application m-line and SCTP comes up; it also carries
    // CONNECTION_CLOSE (SPEC §8). Retain the handle for the Connection.
    const control = pc.createDataChannel(CONTROL_LABEL, { negotiated: true, id: CONTROL_ID })

    const offer = await pc.createOffer()
    const ufrag = generateUfrag()
    const pwd = await deriveICEPwd(digest, ufrag)
    await pc.setLocalDescription({ type: offer.type, sdp: rewriteOfferUfrag(offer.sdp ?? '', ufrag, pwd) })
    await pc.setRemoteDescription({ type: 'answer', sdp: synthesizeAnswer(addr, ufrag, pwd) })

    const conn = new Connection(pc, control)
    await conn.#waitForOpen(signal)
    return conn
  }

  // Open a new unnamed bidirectional byte stream.
  async openStream(opts: { signal?: AbortSignal } = {}): Promise<Stream> {
    if (opts.signal?.aborted) throw new Error('kps: openStream aborted')
    if (this.#state !== 'open') throw new Error(`kps: connection is ${this.#state}`)
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
    if (opts.signal?.aborted) return Promise.reject(new Error('kps: acceptStream aborted'))
    if (this.#state === 'closed') return Promise.reject(new Error('kps: connection is closed'))
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
    if (this.#state === 'closed') return
    // Best-effort CONNECTION_CLOSE to the peer before teardown (SPEC §8). The
    // control channel opens asynchronously, so a close right after dial can beat
    // it; wait briefly for it to open, then send.
    await this.#sendConnClose(reason)
    this.#pc.close()
    this.#fireClose({ ok: true, reason })
  }

  async #sendConnClose(reason?: KpsReason): Promise<void> {
    const c = this.#control
    if (c.readyState === 'connecting') {
      await new Promise<void>((resolve) => {
        const done = () => { c.removeEventListener('open', done); clearTimeout(t); resolve() }
        const t = setTimeout(done, 250)
        c.addEventListener('open', done, { once: true })
      })
    }
    if (c.readyState === 'open') {
      try { c.send(bytesToArrayBuffer(encodeConnClose(reason?.code))) } catch { /* ignore */ }
    }
  }

  // Datagrams (SPEC §7) — unreliable, unordered, best-effort.
  sendDatagram(data: Uint8Array, opts?: { signal?: AbortSignal }): Promise<void> {
    return this.#dg.send(data, opts)
  }

  receiveDatagram(opts?: { signal?: AbortSignal }): Promise<Uint8Array> {
    return this.#dg.receive(opts)
  }

  #enqueueIncoming(stream: Stream): void {
    const w = this.#acceptWaiters.shift()
    if (w) w.resolve(stream)
    else this.#incoming.push(stream)
  }

  // Resolve when the peer connection opens; reject when the signal fires
  // (caller cancel or the default/at-caller timeout) or the connection closes.
  #waitForOpen(signal: AbortSignal): Promise<void> {
    return new Promise((resolve, reject) => {
      if (this.#state === 'open') return resolve()
      const onState = () => {
        if (this.#pc.connectionState === 'connected') { cleanup(); resolve() }
      }
      const onAbort = () => { cleanup(); try { this.#pc.close() } catch {} ; reject(dialAbortError(signal)) }
      const cleanup = () => {
        this.#pc.removeEventListener('connectionstatechange', onState)
        signal.removeEventListener('abort', onAbort)
      }
      this.#pc.addEventListener('connectionstatechange', onState)
      this.closed.then(() => { cleanup(); reject(new Error('kps: connection closed during dial')) }).catch(() => {})
      signal.addEventListener('abort', onAbort, { once: true })
    })
  }

  #fireClose(info: ConnCloseInfo): void {
    if (this.#closeFired) return
    this.#closeFired = true
    this.#state = 'closed'
    this.#dg.close(new Error('kps: connection closed'))
    for (const w of this.#acceptWaiters) w.reject(new Error('kps: connection closed'))
    this.#acceptWaiters = []
    this.#closeResolve(info)
  }
}

export function dial(addr: string, opts?: DialOptions): Promise<Connection> {
  return Connection.dial(addr, opts)
}
