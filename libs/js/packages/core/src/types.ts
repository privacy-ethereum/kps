// The transport-neutral contract. Each transport (webrtc-client, quic-client,
// server) exports concrete classes implementing these interfaces, so callers
// program against the same shape regardless of how the connection is carried.

import type { KpsReason } from './errors.js'

export interface DialOptions {
  signal?: AbortSignal
  timeoutMs?: number
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

// Datagrams (SPEC §7) — always present on a connection. Unreliable, unordered;
// there is a per-connection size limit (oversized send rejects with an error
// carrying `code: 'too-large'` and `maxDatagramPayloadSize`). Payloads up to
// ~1100 bytes are safe on every connection.
export interface Datagrams {
  send(data: Uint8Array, opts?: { signal?: AbortSignal }): Promise<void>
  readonly incoming: ReadableStream<Uint8Array>
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
  readonly datagrams: Datagrams
  readonly state: 'connecting' | 'open' | 'closed'
  openStream(opts?: OpenStreamOptions): Promise<Stream>
  acceptStream(opts?: AcceptStreamOptions): Promise<Stream>
  close(reason?: KpsReason): Promise<void>
}
