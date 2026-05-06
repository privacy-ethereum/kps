// Connection — a kps session to a single server.
// Holds an RTCPeerConnection and exposes openStream() / onstream / close.

import { parseAddress } from './address.js'
import { generateUfrag, rewriteOfferUfrag, synthesizeAnswer } from './sdp.js'
import { Stream, type CloseInfo } from './stream.js'

export interface DialOptions {
  signal?: AbortSignal
  timeoutMs?: number
}

const DEFAULT_TIMEOUT = 15_000
// Fixed stream id for the bootstrap channel — declared on both sides as
// `negotiated:true` so it doesn't surface as a server-side ondatachannel
// event. Its only job is to ensure the offer SDP includes the application
// m-line.
const BOOTSTRAP_LABEL = '_kps_bootstrap'
const BOOTSTRAP_ID = 0

export class Connection extends EventTarget {
  readonly closed: Promise<CloseInfo>
  state: 'connecting' | 'open' | 'closed' = 'connecting'
  onstream: ((stream: Stream) => void) | null = null

  #pc: RTCPeerConnection
  #closeResolve!: (info: CloseInfo) => void
  #closeFired = false

  private constructor(pc: RTCPeerConnection) {
    super()
    this.#pc = pc

    this.closed = new Promise<CloseInfo>(res => { this.#closeResolve = res })

    pc.addEventListener('connectionstatechange', () => {
      const s = pc.connectionState
      if (s === 'connected' && this.state === 'connecting') {
        this.state = 'open'
        this.dispatchEvent(new Event('open'))
      } else if (s === 'failed') {
        this.#fireClose({ reason: 'error', error: new Error('kps: peer connection failed') })
      } else if (s === 'closed' || s === 'disconnected') {
        this.#fireClose({ reason: this.state === 'connecting' ? 'error' : 'remote' })
      }
    })

    pc.addEventListener('datachannel', (e: RTCDataChannelEvent) => {
      const channel = e.channel
      // Bootstrap channel is negotiated:true on both sides, so it should
      // not surface here. If it does (e.g. server didn't pre-allocate it),
      // ignore it.
      if (channel.label === BOOTSTRAP_LABEL) return
      const stream = new Stream(channel)
      if (this.onstream) this.onstream(stream)
    })
  }

  static async dial(addrStr: string, opts: DialOptions = {}): Promise<Connection> {
    const addr = parseAddress(addrStr)
    const pc = new RTCPeerConnection({})

    // Pre-allocated bootstrap channel: forces the offer SDP to contain an
    // application m-line so SCTP gets set up. negotiated:true means no DCEP
    // exchange; the server doesn't need to mirror it.
    pc.createDataChannel(BOOTSTRAP_LABEL, { negotiated: true, id: BOOTSTRAP_ID })

    const offer = await pc.createOffer()
    const ufrag = generateUfrag()
    const rewrittenOffer = { type: offer.type, sdp: rewriteOfferUfrag(offer.sdp ?? '', ufrag) }
    await pc.setLocalDescription(rewrittenOffer)
    const answer = synthesizeAnswer(addr, ufrag)
    await pc.setRemoteDescription({ type: 'answer', sdp: answer })

    const conn = new Connection(pc)
    await conn.#waitForOpen(opts.timeoutMs ?? DEFAULT_TIMEOUT, opts.signal)
    return conn
  }

  async openStream(name: string, opts: { signal?: AbortSignal } = {}): Promise<Stream> {
    if (this.state !== 'open') throw new Error(`kps: connection is ${this.state}`)
    if (name === BOOTSTRAP_LABEL) throw new Error(`kps: '${BOOTSTRAP_LABEL}' is reserved`)
    const channel = this.#pc.createDataChannel(name)
    return await new Promise<Stream>((resolve, reject) => {
      const onAbort = () => {
        cleanup()
        try { channel.close() } catch {}
        reject(new Error('kps: openStream aborted'))
      }
      const onOpen = () => {
        cleanup()
        resolve(new Stream(channel))
      }
      const onError = (e: Event) => {
        cleanup()
        reject((e as RTCErrorEvent).error ?? new Error('kps: openStream failed'))
      }
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

  async close(): Promise<void> {
    if (this.state === 'closed') return
    this.#pc.close()
    this.#fireClose({ reason: 'local' })
  }

  #waitForOpen(timeoutMs: number, signal?: AbortSignal): Promise<void> {
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        cleanup()
        try { this.#pc.close() } catch {}
        reject(new Error(`kps: dial timed out after ${timeoutMs}ms`))
      }, timeoutMs)
      const onOpen = () => { cleanup(); resolve() }
      const onClose = () => { cleanup(); reject(new Error('kps: connection closed during dial')) }
      const onAbort = () => {
        cleanup()
        try { this.#pc.close() } catch {}
        reject(new Error('kps: dial aborted'))
      }
      const cleanup = () => {
        clearTimeout(timer)
        this.removeEventListener('open', onOpen)
        signal?.removeEventListener('abort', onAbort)
        this.closed.finally(() => {}).catch(() => {})
      }
      this.addEventListener('open', onOpen, { once: true })
      this.closed.then(onClose)
      signal?.addEventListener('abort', onAbort, { once: true })
    })
  }

  #fireClose(info: CloseInfo): void {
    if (this.#closeFired) return
    this.#closeFired = true
    this.state = 'closed'
    this.#closeResolve(info)
    this.dispatchEvent(new Event('close'))
  }
}

export function dial(addr: string, opts?: DialOptions): Promise<Connection> {
  return Connection.dial(addr, opts)
}
