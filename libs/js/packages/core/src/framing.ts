// Internal stream framing (SPEC §6.2, wire version 1). Each WebRTC data-channel
// message is one frame: a 1-byte type then a type-specific payload. This makes a
// reliable, ordered, message-oriented data channel present as a byte stream with
// QUIC-like lifecycle plus per-stream flow-control credit (§6.5). The framing is
// internal to KPS; applications see bytes.

export const FRAME_DATA = 0x00
export const FRAME_FIN = 0x01
export const FRAME_RESET = 0x02
export const FRAME_STOP_SENDING = 0x03
export const FRAME_MAX_STREAM_DATA = 0x04

// A frame (type byte + payload) never exceeds MAX_WEBRTC_FRAME_SIZE: an SCTP
// user message is reassembled before KPS can inspect it, so credit alone cannot
// bound one message. DATA therefore carries 1..MAX_FRAME_PAYLOAD bytes; larger
// writes are split, empty writes produce no frame.
export const MAX_WEBRTC_FRAME_SIZE = 16_384
export const MAX_FRAME_PAYLOAD = MAX_WEBRTC_FRAME_SIZE - 1

// Ceiling for every offset, limit, and count (QUIC's integer range, §6.5).
// JavaScript MUST do this arithmetic in BigInt.
export const MAX_OFFSET = (1n << 62n) - 1n

// A wire-rule violation by the peer. Callers catch it and close the connection
// with `protocol-error` (§6.2/§8): within a wire version, malformed input is
// never tolerated or read as something weaker.
export class ProtocolViolation extends Error {}

// The error-code set and reason shape live in ./errors (transport-neutral); the
// framing only needs the type to map codes to the wire uint32 below.
import type { KpsErrorCode } from './errors.js'

const CODE_TO_NUM: Record<KpsErrorCode, number> = {
  cancelled: 1,
  closed: 2,
  reset: 3,
  timeout: 4,
  'network-error': 5,
  'protocol-error': 6,
  unsupported: 7,
  'too-large': 8,
  'queue-full': 9,
  'permission-denied': 10,
  'internal-error': 11
}

const NUM_TO_CODE: Record<number, KpsErrorCode> = Object.fromEntries(
  Object.entries(CODE_TO_NUM).map(([k, v]) => [v, k])
) as Record<number, KpsErrorCode>

export function codeToNum(code?: KpsErrorCode): number {
  return code ? (CODE_TO_NUM[code] ?? 0) : 0
}

export function numToCode(n: number): KpsErrorCode | undefined {
  return n === 0 ? undefined : (NUM_TO_CODE[n] ?? 'internal-error')
}

export function encodeData(payload: Uint8Array): Uint8Array {
  const out = new Uint8Array(1 + payload.length)
  out[0] = FRAME_DATA
  out.set(payload, 1)
  return out
}

export function encodeFin(): Uint8Array {
  return new Uint8Array([FRAME_FIN])
}

export function encodeCode(type: number, code: number): Uint8Array {
  const out = new Uint8Array(5)
  out[0] = type
  new DataView(out.buffer).setUint32(1, code >>> 0, false) // big-endian
  return out
}

export function encodeMaxStreamData(value: bigint): Uint8Array {
  const out = new Uint8Array(9)
  out[0] = FRAME_MAX_STREAM_DATA
  new DataView(out.buffer).setBigUint64(1, value, false)
  return out
}

export type ParsedFrame =
  | { type: 'data'; payload: Uint8Array }
  | { type: 'fin' }
  | { type: 'reset'; code: number }
  | { type: 'stop-sending'; code: number }
  | { type: 'max-stream-data'; value: bigint }

// Parse one data-channel message as a frame, enforcing the wire-version-1 rules
// strictly: unknown types, wrong payload lengths, empty or oversized DATA, and
// out-of-range credit are all ProtocolViolations (connection-fatal), not
// tolerated input.
export function parseFrame(data: Uint8Array): ParsedFrame {
  if (data.length === 0) throw new ProtocolViolation('empty data-channel message')
  if (data.length > MAX_WEBRTC_FRAME_SIZE) {
    throw new ProtocolViolation(`frame exceeds ${MAX_WEBRTC_FRAME_SIZE} bytes (${data.length})`)
  }
  const type = data[0]
  const payload = data.subarray(1)
  const view = () => new DataView(payload.buffer, payload.byteOffset, payload.byteLength)
  switch (type) {
    case FRAME_DATA:
      if (payload.length === 0) throw new ProtocolViolation('empty DATA frame')
      return { type: 'data', payload }
    case FRAME_FIN:
      if (payload.length !== 0) throw new ProtocolViolation('FIN with payload')
      return { type: 'fin' }
    case FRAME_RESET:
      if (payload.length !== 4) throw new ProtocolViolation('RESET payload must be 4 bytes')
      return { type: 'reset', code: view().getUint32(0, false) }
    case FRAME_STOP_SENDING:
      if (payload.length !== 4) throw new ProtocolViolation('STOP_SENDING payload must be 4 bytes')
      return { type: 'stop-sending', code: view().getUint32(0, false) }
    case FRAME_MAX_STREAM_DATA: {
      if (payload.length !== 8) throw new ProtocolViolation('MAX_STREAM_DATA payload must be 8 bytes')
      const value = view().getBigUint64(0, false)
      if (value > MAX_OFFSET) throw new ProtocolViolation('MAX_STREAM_DATA above MAX_OFFSET')
      return { type: 'max-stream-data', value }
    }
    default:
      throw new ProtocolViolation(`unknown frame type 0x${type.toString(16)}`)
  }
}
