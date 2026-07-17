// Connection — a kps session to a single server (SPEC §4) over a browser
// RTCPeerConnection. All protocol logic (framing, §6.5 flow control, §8 typed
// control channel + HELLO, datagrams) lives in the shared @kpstreams/core
// engine (ConnCore/KpsStream); this file supplies the RTCDataChannel adapter,
// the SDP/ICE dial, and transport-state forwarding.

import { parseAddress, decodeCerthash } from '@kpstreams/core'
import {
  generateUfrag, deriveICEPwd, rewriteOfferUfrag, synthesizeAnswer,
  ConnCore, CONTROL_LABEL, CONTROL_ID, DATAGRAM_LABEL, DATAGRAM_ID,
  type ChannelLike,
} from '@kpstreams/core/webrtc'
import type {
  KpsReason, DialOptions, ConnCloseInfo, Stream,
  OpenStreamOptions, AcceptStreamOptions,
  Connection as CoreConnection,
} from '@kpstreams/core'

const DEFAULT_TIMEOUT = 15_000

// RTCDataChannel.send wants an ArrayBuffer-backed view; copy to a fresh,
// exactly-sized ArrayBuffer (also detaches from any SharedArrayBuffer typing).
function toArrayBuffer(u8: Uint8Array): ArrayBuffer {
  return u8.buffer.slice(u8.byteOffset, u8.byteOffset + u8.byteLength) as ArrayBuffer
}

// An aborted dial signal — distinguish a timeout (AbortSignal.timeout →
// DOMException "TimeoutError") from an explicit cancel for a clearer message.
function dialAbortError(signal: AbortSignal): Error {
  const reason = signal.reason as { name?: string } | undefined
  return new Error(reason?.name === 'TimeoutError' ? 'kps: dial timed out' : 'kps: dial aborted')
}

// The ChannelLike adapter over a browser RTCDataChannel. DOM listeners are
// registered once; the current single handler is a swappable field.
class RTCChannelAdapter implements ChannelLike {
  #dc: RTCDataChannel
  #message: ((data: Uint8Array) => void) | null = null
  #open: (() => void) | null = null
  #close: (() => void) | null = null
  #error: ((message: string) => void) | null = null
  #bal: (() => void) | null = null

  constructor(dc: RTCDataChannel) {
    this.#dc = dc
    dc.binaryType = 'arraybuffer'
    dc.addEventListener('message', (e) => {
      const raw = (e as MessageEvent).data as ArrayBuffer | string
      const data = typeof raw === 'string' ? new TextEncoder().encode(raw) : new Uint8Array(raw)
      this.#message?.(data)
    })
    dc.addEventListener('open', () => this.#open?.())
    dc.addEventListener('close', () => this.#close?.())
    dc.addEventListener('error', (e) => {
      this.#error?.((e as RTCErrorEvent).error?.message ?? 'data channel error')
    })
    dc.addEventListener('bufferedamountlow', () => this.#bal?.())
  }

  isOpen(): boolean { return this.#dc.readyState === 'open' }
  send(data: Uint8Array): void { this.#dc.send(toArrayBuffer(data)) }
  bufferedAmount(): number { return this.#dc.bufferedAmount }
  setBufferedAmountLowThreshold(bytes: number): void { this.#dc.bufferedAmountLowThreshold = bytes }
  onBufferedAmountLow(cb: () => void): void { this.#bal = cb }
  onMessage(cb: (data: Uint8Array) => void): void { this.#message = cb }
  onOpen(cb: () => void): void { this.#open = cb }
  onClose(cb: () => void): void { this.#close = cb }
  onError(cb: (message: string) => void): void { this.#error = cb }
  close(): void { try { this.#dc.close() } catch { /* already closed */ } }
}

export class Connection implements CoreConnection {
  // The dialed endpoint (see the core Connection.remoteAddress doc).
  readonly remoteAddress: { ip: string; port: number }

  #pc: RTCPeerConnection
  #core: ConnCore

  // `control` is the reserved reliable channel (ID 0) dial() created before the
  // offer (to force the SCTP m-line).
  private constructor(pc: RTCPeerConnection, control: RTCDataChannel, remote: { ip: string; port: number }) {
    this.#pc = pc
    this.remoteAddress = remote

    // Reserved datagram channel (SPEC §7/§8): negotiated, unreliable, unordered.
    const dg = pc.createDataChannel(DATAGRAM_LABEL, {
      negotiated: true, id: DATAGRAM_ID, ordered: false, maxRetransmits: 0
    })

    this.#core = new ConnCore({
      control: new RTCChannelAdapter(control),
      datagram: new RTCChannelAdapter(dg),
      openChannel: (label) => new RTCChannelAdapter(pc.createDataChannel(label)),
      closeTransport: () => { try { pc.close() } catch { /* ignore */ } }
    })

    pc.addEventListener('connectionstatechange', () => {
      const s = pc.connectionState
      if (s === 'failed') this.#core.onTransportFailed()
      else if (s === 'closed') this.#core.onTransportClosed()
      // 'disconnected' is transient (a packet-loss blip that often recovers);
      // the state machine escalates to 'failed' on its own if it doesn't.
    })

    pc.addEventListener('datachannel', (e: RTCDataChannelEvent) => {
      const channel = e.channel
      // Negotiated reserved channels never fire here; guard anyway.
      if (channel.label === CONTROL_LABEL || channel.label === DATAGRAM_LABEL) return
      this.#core.handleIncomingChannel(new RTCChannelAdapter(channel))
    })
  }

  get closed(): Promise<ConnCloseInfo> {
    return this.#core.closed
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
    // offer carries the application m-line and SCTP comes up; it carries the §8
    // typed control messages (HELLO, CONNECTION_CLOSE, credit).
    const control = pc.createDataChannel(CONTROL_LABEL, { negotiated: true, id: CONTROL_ID })

    const offer = await pc.createOffer()
    const ufrag = generateUfrag()
    const pwd = await deriveICEPwd(digest, ufrag)
    await pc.setLocalDescription({ type: offer.type, sdp: rewriteOfferUfrag(offer.sdp ?? '', ufrag, pwd) })
    await pc.setRemoteDescription({ type: 'answer', sdp: synthesizeAnswer(addr, ufrag, pwd) })

    const conn = new Connection(pc, control, { ip: addr.ip, port: addr.port })
    // Established = transport up AND mutual HELLO (SPEC §8): dial MUST NOT
    // complete before the HELLO exchange.
    await conn.#waitEstablished(signal)
    return conn
  }

  #waitEstablished(signal: AbortSignal): Promise<void> {
    return new Promise((resolve, reject) => {
      const onAbort = () => {
        try { this.#pc.close() } catch { /* ignore */ }
        reject(dialAbortError(signal))
      }
      signal.addEventListener('abort', onAbort, { once: true })
      this.#core.established.then(
        () => { signal.removeEventListener('abort', onAbort); resolve() },
        (e) => { signal.removeEventListener('abort', onAbort); reject(e) }
      )
    })
  }

  openStream(opts: OpenStreamOptions = {}): Promise<Stream> {
    return this.#core.openStream(opts)
  }

  acceptStream(opts: AcceptStreamOptions = {}): Promise<Stream> {
    return this.#core.acceptStream(opts)
  }

  async close(reason?: KpsReason): Promise<void> {
    this.#core.close(reason)
  }

  // Datagrams (SPEC §7) — unreliable, unordered, best-effort.
  sendDatagram(data: Uint8Array, opts?: { signal?: AbortSignal }): Promise<void> {
    return this.#core.sendDatagram(data, opts)
  }

  receiveDatagram(opts?: { signal?: AbortSignal }): Promise<Uint8Array> {
    return this.#core.receiveDatagram(opts)
  }
}

export function dial(addr: string, opts?: DialOptions): Promise<Connection> {
  return Connection.dial(addr, opts)
}
