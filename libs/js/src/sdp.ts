// SDP synthesis for kps over WebRTC.
//
// The browser creates a real local offer via RTCPeerConnection.createOffer(),
// then feeds it a fabricated "answer" describing the server. The server never
// exchanges SDP — it learns the connection's ufrag from the first inbound STUN
// binding's USERNAME attribute and derives the ICE password from the pinned
// certhash (SPEC §5.2). Both ends compute the same password; it is never on the
// wire, which removes the libp2p `ufrag == pwd` fingerprint and gates DTLS
// behind certhash possession.

import { decodeCerthash, digestToSdpFingerprint } from './certhash.js'
import type { Address } from './address.js'

export function extractUfragFromLocalOffer(sdp: string): string {
  const m = sdp.match(/^a=ice-ufrag:(\S+)/m)
  if (!m) throw new Error('kps: no a=ice-ufrag in local offer SDP')
  return m[1]
}

// A random connection-demux ufrag of normal WebRTC length (~72 bits). It no
// longer doubles as the password, so it need not be pwd-length.
export function generateUfrag(): string {
  const bytes = new Uint8Array(9)
  crypto.getRandomValues(bytes)
  let bin = ''
  for (const b of bytes) bin += String.fromCharCode(b)
  return btoa(bin).replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '')
}

// deriveICEPwd computes the ICE password from the pinned certhash digest and the
// ufrag (SPEC §5.2). HMAC-SHA256, base64 standard alphabet, no padding (within
// the ICE ice-char set). Identical to the Go server's deriveICEPwd.
export async function deriveICEPwd(certhashDigest: Uint8Array, ufrag: string): Promise<string> {
  const key = await crypto.subtle.importKey(
    'raw', toArrayBuffer(certhashDigest), { name: 'HMAC', hash: 'SHA-256' }, false, ['sign']
  )
  const msg = new TextEncoder().encode('kps-ice-pwd-v1:' + ufrag)
  const sig = new Uint8Array(await crypto.subtle.sign('HMAC', key, toArrayBuffer(msg)))
  let bin = ''
  for (const b of sig) bin += String.fromCharCode(b)
  return btoa(bin).replace(/=+$/, '')
}

function toArrayBuffer(u8: Uint8Array): ArrayBuffer {
  return u8.buffer.slice(u8.byteOffset, u8.byteOffset + u8.byteLength) as ArrayBuffer
}

export function rewriteOfferUfrag(sdp: string, ufrag: string, pwd: string): string {
  const lines = sdp.split(/\r\n|\n/).map(line => {
    if (line.startsWith('a=ice-ufrag:')) return `a=ice-ufrag:${ufrag}`
    if (line.startsWith('a=ice-pwd:')) return `a=ice-pwd:${pwd}`
    return line
  })
  return lines.join('\r\n')
}

export function synthesizeAnswer(addr: Address, ufrag: string, pwd: string): string {
  const fingerprint = digestToSdpFingerprint(decodeCerthash(addr.certhash))
  const lines = [
    'v=0',
    'o=- 0 0 IN IP4 0.0.0.0',
    's=-',
    't=0 0',
    'a=ice-lite',
    `m=application ${addr.port} UDP/DTLS/SCTP webrtc-datachannel`,
    `c=IN IP4 ${addr.ip}`,
    'a=mid:0',
    `a=ice-ufrag:${ufrag}`,
    `a=ice-pwd:${pwd}`,
    `a=fingerprint:sha-256 ${fingerprint}`,
    'a=setup:passive',
    'a=sctp-port:5000',
    'a=max-message-size:1048576',
    `a=candidate:1 1 UDP 1 ${addr.ip} ${addr.port} typ host`
  ]
  return lines.join('\r\n') + '\r\n'
}
