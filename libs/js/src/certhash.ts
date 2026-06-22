// The wire format for a pinned key: a SHA-256 multihash, multibase-encoded.
// We accept the same `uEi...` form the libp2p/webrtc-direct ecosystem uses
// (multibase prefix `u` = base64url-no-pad; multihash prefix 0x12 0x20 =
// sha2-256 / 32 bytes). 32 raw digest bytes come out the back.

const MULTIBASE_BASE64URL_NOPAD = 'u'
const MULTIHASH_SHA256_CODE = 0x12
const MULTIHASH_SHA256_LEN = 0x20

export function decodeCerthash(s: string): Uint8Array {
  if (!s.startsWith(MULTIBASE_BASE64URL_NOPAD)) {
    throw new Error(`certhash: expected multibase prefix '${MULTIBASE_BASE64URL_NOPAD}', got '${s[0] ?? ''}'`)
  }
  const bytes = base64urlDecode(s.slice(1))
  if (bytes.length !== 2 + MULTIHASH_SHA256_LEN) {
    throw new Error(`certhash: expected ${2 + MULTIHASH_SHA256_LEN} bytes, got ${bytes.length}`)
  }
  if (bytes[0] !== MULTIHASH_SHA256_CODE || bytes[1] !== MULTIHASH_SHA256_LEN) {
    throw new Error(`certhash: not a sha2-256 multihash (prefix ${bytes[0].toString(16)} ${bytes[1].toString(16)})`)
  }
  return bytes.slice(2)
}

// Format raw 32 bytes as `AA:BB:...` for SDP a=fingerprint lines.
export function digestToSdpFingerprint(digest: Uint8Array): string {
  return Array.from(digest, b => b.toString(16).padStart(2, '0').toUpperCase()).join(':')
}

function base64urlDecode(s: string): Uint8Array {
  // Restore padding and translate alphabet to standard base64.
  const pad = (4 - (s.length % 4)) % 4
  const b64 = s.replace(/-/g, '+').replace(/_/g, '/') + '='.repeat(pad)
  const bin = atob(b64)
  const out = new Uint8Array(bin.length)
  for (let i = 0; i < bin.length; i++) out[i] = bin.charCodeAt(i)
  return out
}
