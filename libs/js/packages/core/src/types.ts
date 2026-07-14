// The transport-neutral contract. Each transport (webrtc-client, quic-client,
// server) exports concrete classes implementing these interfaces, so callers
// program against the same shape regardless of how the connection is carried.

import type { KpsReason } from './errors.js'

export interface DialOptions {
  // Abort the dial — cancellation or timeout, whichever the signal expresses.
  // With no signal, dial applies a ~15s default timeout so it can't hang. Pass a
  // signal to own the deadline: `AbortSignal.timeout(ms)` for a custom timeout,
  // or `AbortSignal.any([mySignal, AbortSignal.timeout(ms)])` to combine cancel +
  // timeout. (The analogue of Go's ctx; there's no separate timeout option.)
  signal?: AbortSignal
}

export interface OpenStreamOptions {
  signal?: AbortSignal
}

export interface AcceptStreamOptions {
  signal?: AbortSignal
}

export interface ConnCloseInfo {
  ok: boolean
  reason?: KpsReason
}

export interface StreamCloseInfo {
  ok: boolean
  reason?: KpsReason
}

// Stream — an unnamed, bidirectional, reliable, ordered byte stream (SPEC §6).
export interface Stream {
  readonly readable: ReadableStream<Uint8Array>
  readonly writable: WritableStream<Uint8Array>
  readonly closed: Promise<StreamCloseInfo>
  /** Gracefully finish the local write half; the peer observes EOF. */
  closeWrite(): Promise<void>
  /** Stop wanting inbound bytes (not EOF); the peer is told to stop sending. */
  cancelRead(reason?: KpsReason): Promise<void>
  /** Abort the local write half; the peer observes a stream error, not EOF. */
  resetWrite(reason?: KpsReason): Promise<void>
  /** Tear down both halves of the stream. */
  close(reason?: KpsReason): Promise<void>
}

// Connection — a kps session to a single server (SPEC §4).
export interface Connection {
  readonly closed: Promise<ConnCloseInfo>

  // The peer's UDP endpoint (e.g. for per-IP policy such as rate limiting).
  // Reflects the endpoint observed at connection establishment and MAY change
  // over the connection's life (QUIC path migration, ICE renomination); on the
  // dial side it is the dialed endpoint. Mirrors Go's RemoteAddr / Rust's
  // remote_addr.
  readonly remoteAddress: { ip: string; port: number }

  openStream(opts?: OpenStreamOptions): Promise<Stream>
  acceptStream(opts?: AcceptStreamOptions): Promise<Stream>
  close(reason?: KpsReason): Promise<void>

  // Datagrams (SPEC §7) — always available. Unreliable, unordered, best-effort.
  // `sendDatagram` rejects an oversized payload with an error carrying
  // `code: 'too-large'` and `maxDatagramPayloadSize`; payloads up to ~1100 bytes
  // are safe on every connection. `receiveDatagram` resolves with the next
  // inbound datagram (mirrors Go's SendDatagram/ReceiveDatagram).
  sendDatagram(data: Uint8Array, opts?: { signal?: AbortSignal }): Promise<void>
  receiveDatagram(opts?: { signal?: AbortSignal }): Promise<Uint8Array>
}
