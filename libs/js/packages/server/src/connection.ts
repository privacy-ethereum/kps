// Server-side Connection: wraps a node-datachannel PeerConnection for one peer
// and presents the transport-neutral core Connection. All protocol logic
// (framing, §6.5 flow control, §8 typed control channel + HELLO, datagrams)
// lives in the shared @kpstreams/core engine (ConnCore/KpsStream); this file
// supplies the node-datachannel ChannelLike adapter and transport-state
// forwarding. Mirrors the browser webrtc-client Connection.

import { PeerConnection, type DataChannel } from 'node-datachannel'
import {
  ConnCore, CONTROL_LABEL, CONTROL_ID, DATAGRAM_LABEL, DATAGRAM_ID,
  type ChannelLike,
} from '@kpstreams/core/webrtc'
import type {
  Connection as CoreConnection, ConnCloseInfo, Stream,
  KpsReason, OpenStreamOptions, AcceptStreamOptions,
} from '@kpstreams/core'

function toBytes(msg: string | Buffer | ArrayBuffer): Uint8Array {
  if (typeof msg === 'string') return new TextEncoder().encode(msg)
  return msg instanceof Uint8Array ? msg : new Uint8Array(msg)
}

// The ChannelLike adapter over a node-datachannel DataChannel (whose callback
// registration is already single-handler).
class NdcChannelAdapter implements ChannelLike {
  #dc: DataChannel
  constructor(dc: DataChannel) {
    this.#dc = dc
  }
  isOpen(): boolean { return this.#dc.isOpen() }
  send(data: Uint8Array): void { this.#dc.sendMessageBinary(Buffer.from(data)) }
  bufferedAmount(): number { return this.#dc.bufferedAmount() }
  setBufferedAmountLowThreshold(bytes: number): void { this.#dc.setBufferedAmountLowThreshold(bytes) }
  onBufferedAmountLow(cb: () => void): void { this.#dc.onBufferedAmountLow(cb) }
  onMessage(cb: (data: Uint8Array) => void): void {
    // Strings are converted, not dropped: a text message on a KPS channel is a
    // wire violation the engine must see (and fail the connection on).
    this.#dc.onMessage((msg) => cb(toBytes(msg)))
  }
  onOpen(cb: () => void): void { this.#dc.onOpen(cb) }
  onClose(cb: () => void): void { this.#dc.onClosed(cb) }
  onError(cb: (message: string) => void): void { this.#dc.onError(cb) }
  close(): void { try { this.#dc.close() } catch { /* already closed */ } }
}

export class Connection implements CoreConnection {
  /** Resolves at mutual HELLO (SPEC §8) — accept MUST NOT surface before it. */
  readonly ready: Promise<void>

  // The client's first STUN source (see the core Connection.remoteAddress doc).
  readonly remoteAddress: { ip: string; port: number }

  #pc: PeerConnection
  #core: ConnCore

  constructor(pc: PeerConnection, remote: { ip: string; port: number }) {
    this.#pc = pc
    this.remoteAddress = remote

    // Reserved channels (SPEC §8): negotiated on both sides, never surfaced as
    // application streams. Control (ID 0) carries the typed §8 messages;
    // datagram (ID 1) is unreliable, unordered.
    const control = pc.createDataChannel(CONTROL_LABEL, { negotiated: true, id: CONTROL_ID })
    const dg = pc.createDataChannel(DATAGRAM_LABEL, {
      negotiated: true, id: DATAGRAM_ID, unordered: true, maxRetransmits: 0
    })

    this.#core = new ConnCore({
      control: new NdcChannelAdapter(control),
      datagram: new NdcChannelAdapter(dg),
      openChannel: (label) => new NdcChannelAdapter(pc.createDataChannel(label)),
      closeTransport: () => { try { pc.close() } catch { /* ignore */ } }
    })
    this.ready = this.#core.established

    pc.onStateChange((s) => {
      if (s === 'failed') this.#core.onTransportFailed()
      else if (s === 'closed') this.#core.onTransportClosed()
      // 'disconnected' is transient; let it recover or escalate to 'failed'.
    })

    pc.onDataChannel((dc) => {
      const label = dc.getLabel()
      // Negotiated reserved channels never fire here; guard anyway.
      if (label === CONTROL_LABEL || label === DATAGRAM_LABEL) return
      this.#core.handleIncomingChannel(new NdcChannelAdapter(dc))
    })
  }

  get closed(): Promise<ConnCloseInfo> {
    return this.#core.closed
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
