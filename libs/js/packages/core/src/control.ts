// The reserved control channel's typed messages (SPEC §8, wire version 1).
// Each message is a 1-byte type + exact-length payload, integers big-endian:
//
//   0x00 CONNECTION_CLOSE  uint32 code                          5 bytes
//   0x01 HELLO             uint8 version + 3× uint64 credits   26 bytes
//   0x02 MAX_DATA          uint64 absolute limit                9 bytes
//   0x03 MAX_STREAMS       uint64 absolute limit                9 bytes
//
// Anything else — unknown type, wrong length, credit above MAX_OFFSET — is a
// ProtocolViolation: malformed input is never read as a clean close.

import { MAX_OFFSET, ProtocolViolation, codeToNum } from './framing.js'
import type { KpsErrorCode } from './errors.js'

export const CTRL_CONNECTION_CLOSE = 0x00
export const CTRL_HELLO = 0x01
export const CTRL_MAX_DATA = 0x02
export const CTRL_MAX_STREAMS = 0x03

export const WIRE_VERSION = 1

export interface HelloLimits {
  initialMaxStreamData: bigint
  initialMaxData: bigint
  initialMaxStreams: bigint
}

export function encodeConnClose(code?: KpsErrorCode): Uint8Array {
  const out = new Uint8Array(5)
  out[0] = CTRL_CONNECTION_CLOSE
  new DataView(out.buffer).setUint32(1, codeToNum(code), false)
  return out
}

export function encodeHello(limits: HelloLimits, version = WIRE_VERSION): Uint8Array {
  const out = new Uint8Array(26)
  const v = new DataView(out.buffer)
  out[0] = CTRL_HELLO
  out[1] = version
  v.setBigUint64(2, limits.initialMaxStreamData, false)
  v.setBigUint64(10, limits.initialMaxData, false)
  v.setBigUint64(18, limits.initialMaxStreams, false)
  return out
}

export function encodeMaxData(value: bigint): Uint8Array {
  const out = new Uint8Array(9)
  out[0] = CTRL_MAX_DATA
  new DataView(out.buffer).setBigUint64(1, value, false)
  return out
}

export function encodeMaxStreams(value: bigint): Uint8Array {
  const out = new Uint8Array(9)
  out[0] = CTRL_MAX_STREAMS
  new DataView(out.buffer).setBigUint64(1, value, false)
  return out
}

export type ControlMsg =
  | { t: 'close'; code: number }
  | { t: 'hello'; version: number; limits: HelloLimits }
  | { t: 'max-data'; value: bigint }
  | { t: 'max-streams'; value: bigint }

export function decodeControl(data: Uint8Array): ControlMsg {
  if (data.length === 0) throw new ProtocolViolation('empty control message')
  const type = data[0]
  const view = new DataView(data.buffer, data.byteOffset, data.byteLength)
  switch (type) {
    case CTRL_CONNECTION_CLOSE:
      if (data.length !== 5) throw new ProtocolViolation('CONNECTION_CLOSE must be 5 bytes')
      return { t: 'close', code: view.getUint32(1, false) }
    case CTRL_HELLO: {
      if (data.length !== 26) throw new ProtocolViolation('HELLO must be 26 bytes')
      const limits: HelloLimits = {
        initialMaxStreamData: view.getBigUint64(2, false),
        initialMaxData: view.getBigUint64(10, false),
        initialMaxStreams: view.getBigUint64(18, false)
      }
      for (const v of Object.values(limits)) {
        if (v > MAX_OFFSET) throw new ProtocolViolation('HELLO credit above MAX_OFFSET')
      }
      return { t: 'hello', version: data[1], limits }
    }
    case CTRL_MAX_DATA: {
      if (data.length !== 9) throw new ProtocolViolation('MAX_DATA must be 9 bytes')
      const value = view.getBigUint64(1, false)
      if (value > MAX_OFFSET) throw new ProtocolViolation('MAX_DATA above MAX_OFFSET')
      return { t: 'max-data', value }
    }
    case CTRL_MAX_STREAMS: {
      if (data.length !== 9) throw new ProtocolViolation('MAX_STREAMS must be 9 bytes')
      const value = view.getBigUint64(1, false)
      if (value > MAX_OFFSET) throw new ProtocolViolation('MAX_STREAMS above MAX_OFFSET')
      return { t: 'max-streams', value }
    }
    default:
      throw new ProtocolViolation(`unknown control message type 0x${type.toString(16)}`)
  }
}
