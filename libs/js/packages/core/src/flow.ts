// End-to-end flow control for the WebRTC mapping (SPEC §6.5): the credit
// engine, pure of any channel type. One ConnFlow per connection tracks both
// directions of connection-level credit plus stream-count credit; one
// StreamFlow per stream tracks that stream's credit. All arithmetic is BigInt
// (counters are unbounded cumulative offsets with a 2^62−1 ceiling).
//
// Sender side: `sent + reserved + n ≤ peerMax` at both levels, checked and
// reserved atomically (trivially so — JS is single-threaded) BEFORE a DATA
// frame may be sent; a blocked reservation waits for peer credit.
// Receiver side: `received + n ≤ localMax` enforced before buffering;
// consumption (read fulfilled or explicit discard) advances counters and
// re-advertises credit once half a window is consumed-but-unadvertised.

import { MAX_OFFSET, ProtocolViolation } from './framing.js'

// Recommended initial windows (SPEC §6.5) — receiver policy, not protocol
// constants.
export const DEFAULT_INITIAL_MAX_STREAM_DATA = 1n << 20n // 1 MiB
export const DEFAULT_INITIAL_MAX_DATA = 8n << 20n // 8 MiB
export const DEFAULT_INITIAL_MAX_STREAMS = 100n

export interface FlowLimits {
  initialMaxStreamData: bigint
  initialMaxData: bigint
  initialMaxStreams: bigint
}

export function resolveLimits(partial?: Partial<FlowLimits>): FlowLimits {
  return {
    initialMaxStreamData: partial?.initialMaxStreamData ?? DEFAULT_INITIAL_MAX_STREAM_DATA,
    initialMaxData: partial?.initialMaxData ?? DEFAULT_INITIAL_MAX_DATA,
    initialMaxStreams: partial?.initialMaxStreams ?? DEFAULT_INITIAL_MAX_STREAMS
  }
}

function saturate(v: bigint): bigint {
  return v > MAX_OFFSET ? MAX_OFFSET : v
}

// All credit-blocked operations wait on one connection-wide wakeable and
// re-check; wakes happen on any credit grant, reservation release, or failure.
class Wakeable {
  #resolvers: Array<() => void> = []
  wake(): void {
    const rs = this.#resolvers
    this.#resolvers = []
    for (const r of rs) r()
  }
  wait(signal?: AbortSignal): Promise<void> {
    return new Promise((resolve, reject) => {
      if (signal) {
        const onAbort = () => reject(new Error('kps: aborted'))
        signal.addEventListener('abort', onAbort, { once: true })
        this.#resolvers.push(() => {
          signal.removeEventListener('abort', onAbort)
          resolve()
        })
      } else {
        this.#resolvers.push(resolve)
      }
    })
  }
}

// Callbacks through which the engine emits credit advertisements; the owner
// wires them to the control channel (conn-level) or stream channel
// (stream-level). Values are absolute and the channels reliable+ordered, so
// sending immediately is always safe; the local enforcement limit advances at
// this call (commit-to-send), before the peer ever sees the update (§6.5).
export interface ConnFlowSink {
  sendMaxData(value: bigint): void
  sendMaxStreams(value: bigint): void
}

export class ConnFlow {
  // ---- our receive policy (what we grant the peer) ----
  readonly local: FlowLimits

  // ---- sender side (peer-granted, all zero until the peer's HELLO) ----
  #peerMaxStreamDataInitial = 0n // seeds each new StreamFlow's send window
  #peerMaxData = 0n
  #peerMaxStreams = 0n
  #connSent = 0n
  #connReserved = 0n
  #streamsOpened = 0n
  #streamsReserved = 0n

  // ---- receiver side ----
  #localMaxData: bigint // enforcement limit (advances at commit-to-send)
  #connReceived = 0n
  #connConsumed = 0n
  #connAdvertisedAt = 0n // connConsumed value at the last advertisement
  #peerOpenedStreams = 0n
  #peerRetiredStreams = 0n
  #advertisedMaxStreams: bigint

  #credit = new Wakeable()
  #failure: Error | null = null
  #sink: ConnFlowSink

  constructor(local: FlowLimits, sink: ConnFlowSink) {
    this.local = local
    this.#localMaxData = local.initialMaxData
    this.#advertisedMaxStreams = local.initialMaxStreams
    this.#sink = sink
  }

  /** The peer's HELLO: seed every send-side limit. */
  onPeerHello(limits: FlowLimits): void {
    this.#peerMaxStreamDataInitial = limits.initialMaxStreamData
    this.#peerMaxData = limits.initialMaxData
    this.#peerMaxStreams = limits.initialMaxStreams
    this.#credit.wake()
  }

  /** Peer raised the connection data limit (MAX_DATA). Decreases are ignored. */
  onPeerMaxData(value: bigint): void {
    if (value > this.#peerMaxData) {
      this.#peerMaxData = value
      this.#credit.wake()
    }
  }

  /** Peer raised the stream-count limit (MAX_STREAMS). Decreases are ignored. */
  onPeerMaxStreams(value: bigint): void {
    if (value > this.#peerMaxStreams) {
      this.#peerMaxStreams = value
      this.#credit.wake()
    }
  }

  /** Fail every pending and future credit wait (connection teardown). */
  fail(err: Error): void {
    if (this.#failure) return
    this.#failure = err
    this.#credit.wake()
  }

  /** Wake blocked reservations so they re-check (stream credit or failure). */
  wake(): void {
    this.#credit.wake()
  }

  get failed(): Error | null {
    return this.#failure
  }

  /**
   * The peer's per-stream initial window. A getter (not copied into
   * StreamFlow) so streams staged before the peer's HELLO see the window the
   * moment it arrives.
   */
  get peerInitialMaxStreamData(): bigint {
    return this.#peerMaxStreamDataInitial
  }

  newStream(sendMaxStreamData: (value: bigint) => void): StreamFlow {
    return new StreamFlow(this, this.local.initialMaxStreamData, sendMaxStreamData)
  }

  // ---- sender: byte credit (called via StreamFlow) ----

  /**
   * Reserve up to `n` DATA payload bytes at both levels, waiting until at
   * least one byte of credit is available (a writer larger than the whole
   * window must split at the window boundary, like a QUIC sender — an
   * all-or-nothing reservation would deadlock). Returns the granted amount
   * (1..n). Rejects if the stream's write half fails (STOP_SENDING, reset,
   * close), the connection fails, or `signal` aborts.
   */
  async reserveData(sf: StreamFlow, n: bigint, signal?: AbortSignal): Promise<bigint> {
    for (;;) {
      if (this.#failure) throw this.#failure
      const sfErr = sf.sendFailed
      if (sfErr) throw sfErr
      if (signal?.aborted) throw new Error('kps: aborted')
      const streamAvail = sf.peerMaxStreamData - sf.sendSent - sf.sendReserved
      const connAvail = this.#peerMaxData - this.#connSent - this.#connReserved
      let grant = streamAvail < connAvail ? streamAvail : connAvail
      if (grant > n) grant = n
      if (grant >= 1n) {
        sf.sendReserved += grant
        this.#connReserved += grant
        return grant
      }
      await this.#credit.wait(signal)
    }
  }

  /** Bytes passed to the transport: reserved → sent, both levels. */
  commitData(sf: StreamFlow, n: bigint): void {
    sf.sendReserved -= n
    sf.sendSent += n
    this.#connReserved -= n
    this.#connSent += n
  }

  /** A reserved-but-unsent frame was discarded: release its reservation. */
  releaseData(sf: StreamFlow, n: bigint): void {
    sf.sendReserved -= n
    this.#connReserved -= n
    this.#credit.wake()
  }

  // ---- sender: stream slots ----

  /** Reserve a slot to open one stream, waiting at the limit. */
  async reserveStreamSlot(signal?: AbortSignal): Promise<void> {
    for (;;) {
      if (this.#failure) throw this.#failure
      if (signal?.aborted) throw new Error('kps: aborted')
      if (this.#streamsOpened + this.#streamsReserved < this.#peerMaxStreams) {
        this.#streamsReserved += 1n
        return
      }
      await this.#credit.wait(signal)
    }
  }

  /** Channel creation succeeded: the cumulative count never decreases. */
  commitStreamSlot(): void {
    this.#streamsReserved -= 1n
    this.#streamsOpened += 1n
  }

  /** Channel creation failed synchronously: release the slot. */
  releaseStreamSlot(): void {
    this.#streamsReserved -= 1n
    this.#credit.wake()
  }

  // ---- receiver: byte credit (called via StreamFlow) ----

  /** @throws ProtocolViolation when the peer exceeds the connection window. */
  connDataReceived(n: bigint): void {
    if (this.#connReceived + n > this.#localMaxData) {
      throw new ProtocolViolation('peer exceeded MAX_DATA')
    }
    this.#connReceived += n
  }

  connDataConsumed(n: bigint): void {
    this.#connConsumed += n
    const window = this.local.initialMaxData
    if (this.#connConsumed - this.#connAdvertisedAt >= window / 2n) {
      this.#connAdvertisedAt = this.#connConsumed
      this.#localMaxData = saturate(this.#connConsumed + window)
      this.#sink.sendMaxData(this.#localMaxData)
    }
  }

  // ---- receiver: stream count ----

  /**
   * A peer-initiated stream was observed (it consumes a slot immediately, even
   * unaccepted or pre-HELLO).
   * @throws ProtocolViolation when the peer exceeds MAX_STREAMS.
   */
  peerStreamOpened(): void {
    if (this.#peerOpenedStreams >= this.#advertisedMaxStreams) {
      throw new ProtocolViolation('peer exceeded MAX_STREAMS')
    }
    this.#peerOpenedStreams += 1n
  }

  /** A peer-initiated stream retired: grant a replacement slot. */
  peerStreamRetired(): void {
    this.#peerRetiredStreams += 1n
    this.#advertisedMaxStreams = saturate(this.local.initialMaxStreams + this.#peerRetiredStreams)
    this.#sink.sendMaxStreams(this.#advertisedMaxStreams)
  }
}

export class StreamFlow {
  // sender side
  sendSent = 0n
  sendReserved = 0n
  #peerMaxExplicit = 0n // largest MAX_STREAM_DATA received on this stream
  #sendFailure: Error | null = null

  // receiver side
  #localMaxStreamData: bigint // enforcement limit
  #received = 0n
  #consumed = 0n
  #advertisedAt = 0n
  #cancelled = false // local cancelRead: no further stream credit

  #conn: ConnFlow
  #sendMaxStreamData: (value: bigint) => void

  constructor(conn: ConnFlow, localMaxStreamData: bigint, sendMaxStreamData: (value: bigint) => void) {
    this.#conn = conn
    this.#localMaxStreamData = localMaxStreamData
    this.#sendMaxStreamData = sendMaxStreamData
  }

  /** Effective peer window: explicit updates never lower it below the HELLO initial. */
  get peerMaxStreamData(): bigint {
    const initial = this.#conn.peerInitialMaxStreamData
    return this.#peerMaxExplicit > initial ? this.#peerMaxExplicit : initial
  }

  // ---- sender ----

  /** Reserve up to `n` bytes; resolves with the granted amount (1..n). */
  async reserve(n: number, signal?: AbortSignal): Promise<number> {
    return Number(await this.#conn.reserveData(this, BigInt(n), signal))
  }

  commit(n: number): void {
    this.#conn.commitData(this, BigInt(n))
  }

  release(n: number): void {
    this.#conn.releaseData(this, BigInt(n))
  }

  /** Fail pending and future reservations (STOP_SENDING, reset, close). */
  failSend(err: Error): void {
    if (this.#sendFailure) return
    this.#sendFailure = err
    // Conn-wide wake is fine: waiters re-check their own state.
    this.#conn.wake()
  }

  get sendFailed(): Error | null {
    return this.#sendFailure
  }

  /** MAX_STREAM_DATA from the peer. Decreases are ignored. */
  onPeerMaxStreamData(value: bigint): void {
    if (value > this.#peerMaxExplicit) {
      this.#peerMaxExplicit = value
      this.#conn.wake()
    }
  }

  // ---- receiver ----

  /**
   * `n` inbound DATA payload bytes arrived; enforce both windows atomically
   * (single-threaded: check both, then count both).
   * @throws ProtocolViolation when the peer exceeds either window.
   */
  onDataReceived(n: number): void {
    const bn = BigInt(n)
    if (this.#received + bn > this.#localMaxStreamData) {
      throw new ProtocolViolation('peer exceeded MAX_STREAM_DATA')
    }
    this.#conn.connDataReceived(bn) // throws before counting if over
    this.#received += bn
  }

  /**
   * `n` bytes were consumed — read-fulfilled to the application or explicitly
   * discarded. Advertises replacement credit past the half-window threshold
   * (stream credit is withheld after cancelRead; connection credit always
   * flows so a discarded stream cannot starve unrelated streams).
   */
  onConsumed(n: number): void {
    const bn = BigInt(n)
    this.#consumed += bn
    const window = this.#conn.local.initialMaxStreamData
    if (!this.#cancelled && this.#consumed - this.#advertisedAt >= window / 2n) {
      this.#advertisedAt = this.#consumed
      this.#localMaxStreamData = saturate(this.#consumed + window)
      this.#sendMaxStreamData(this.#localMaxStreamData)
    }
    this.#conn.connDataConsumed(bn)
  }

  /** Local cancelRead: stop granting stream credit; discards still free MAX_DATA. */
  markCancelled(): void {
    this.#cancelled = true
  }
}
