// SDP synthesis for kps over WebRTC.
//
// The browser creates a real local offer via RTCPeerConnection.createOffer(),
// then feeds it a fabricated "answer" describing the server. The server
// never exchanges SDP — it learns the connection's ufrag from the first
// inbound STUN binding's USERNAME attribute and uses that ufrag as the ICE
// password too (the kps convention, inherited from libp2p webrtc-direct).
//
// So: the synthesized answer must claim ice-ufrag = ice-pwd = the same
// ufrag the browser already chose for its local offer. We extract that
// ufrag from the local SDP and reuse it.

import { decodeCerthash, digestToSdpFingerprint } from './certhash.js'
import type { Address } from './address.js'

export function extractUfragFromLocalOffer(sdp: string): string {
  const m = sdp.match(/^a=ice-ufrag:(\S+)/m)
  if (!m) throw new Error('kps: no a=ice-ufrag in local offer SDP')
  return m[1]
}

// Replace the offer's ICE ufrag/pwd with values we control. Browsers
// auto-generate a 4-char ufrag and a separate 22+ char pwd; we need
// ufrag to also be long enough to use as pwd (the kps convention has
// pwd = ufrag so the server can derive both from the STUN USERNAME it
// observes).
export function generateUfrag(): string {
  const bytes = new Uint8Array(18)
  crypto.getRandomValues(bytes)
  // base64url, no padding — yields 24 chars from 18 bytes.
  let bin = ''
  for (const b of bytes) bin += String.fromCharCode(b)
  return btoa(bin).replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '')
}

export function rewriteOfferUfrag(sdp: string, ufrag: string): string {
  const lines = sdp.split(/\r\n|\n/).map(line => {
    if (line.startsWith('a=ice-ufrag:')) return `a=ice-ufrag:${ufrag}`
    if (line.startsWith('a=ice-pwd:')) return `a=ice-pwd:${ufrag}`
    return line
  })
  return lines.join('\r\n')
}

export function synthesizeAnswer(addr: Address, ufrag: string): string {
  const fingerprint = digestToSdpFingerprint(decodeCerthash(addr.certhash))
  // Order matters in SDP. WebRTC implementations are forgiving about some
  // attribute orderings within the m= section, but keep the canonical layout.
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
    `a=ice-pwd:${ufrag}`,
    `a=fingerprint:sha-256 ${fingerprint}`,
    'a=setup:passive',
    'a=sctp-port:5000',
    'a=max-message-size:1048576',
    `a=candidate:1 1 UDP 1 ${addr.ip} ${addr.port} typ host`
  ]
  // SDP must end with CRLF after the last line.
  return lines.join('\r\n') + '\r\n'
}
