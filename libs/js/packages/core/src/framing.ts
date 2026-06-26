// Internal stream framing (SPEC §6.2). Each WebRTC data-channel message is one
// frame: a 1-byte type then a type-specific payload. This makes a reliable,
// ordered, message-oriented data channel present as a byte stream with
// QUIC-like lifecycle. The framing is internal to KPS; applications see bytes.

export const FRAME_DATA = 0x00
export const FRAME_FIN = 0x01
export const FRAME_RESET = 0x02
export const FRAME_STOP_SENDING = 0x03

// Largest stream payload carried in one DATA frame; larger writes are split.
export const MAX_FRAME_PAYLOAD = 16 * 1024

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

export interface Frame {
  type: number
  payload: Uint8Array
  code: number
}

export function decodeFrame(data: Uint8Array): Frame {
  const type = data[0]
  const payload = data.subarray(1)
  let code = 0
  if ((type === FRAME_RESET || type === FRAME_STOP_SENDING) && payload.length >= 4) {
    code = new DataView(payload.buffer, payload.byteOffset, payload.byteLength).getUint32(0, false)
  }
  return { type, payload, code }
}
