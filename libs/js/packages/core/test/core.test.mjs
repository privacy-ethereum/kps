// Unit tests for the pure, dependency-free core surface. Run against the built
// dist (node --test) — no test-runner dependency. See the root `npm test`.
import { test } from 'node:test'
import assert from 'node:assert/strict'
import { parseAddress, formatAddress, decodeCerthash, encodeCerthash } from '../dist/index.js'
import {
  deriveICEPwd, parseFrame, encodeData, encodeFin, encodeCode,
  FRAME_RESET, numToCode,
} from '../dist/webrtc.js'

test('parseAddress/formatAddress round-trip (v4 + bracketed v6)', () => {
  for (const a of [
    '203.0.113.5:41108:uEiABC',
    '[2001:db8::1]:4242:uEiABC',
    '[::1]:60949:uEiABC',
  ]) {
    assert.equal(formatAddress(parseAddress(a)), a)
  }
})

test('parseAddress rejects malformed addresses', () => {
  for (const bad of [
    '1.2.3.4',              // no port/certhash
    '1.2.3.4:0x1bb:uEi',    // hex port
    '1.2.3.4:1e3:uEi',      // exponent port
    '1.2.3.4: 42 :uEi',     // whitespace
    '1.2.3.4:0:uEi',        // port 0
    '1.2.3.4:99999:uEi',    // port > 65535
    '[2001:db8::1]4242:uEi',// missing ']:'
  ]) {
    assert.throws(() => parseAddress(bad), `should reject ${bad}`)
  }
})

test('certhash encode/decode round-trip', () => {
  const digest = Uint8Array.from({ length: 32 }, (_, i) => i)
  const ch = encodeCerthash(digest)
  assert.match(ch, /^u/) // multibase base64url-nopad
  assert.deepEqual(decodeCerthash(ch), digest)
})

test('deriveICEPwd known vector (must match the Go server)', async () => {
  const pwd = await deriveICEPwd(new Uint8Array(32), 'test')
  assert.equal(pwd, 'dn9pWWCBP6fiDbQLjNcQVFAYWVSdPNzYf5JQ8JzVSso')
})

test('parseFrame: DATA / FIN / RESET(code)', () => {
  const data = parseFrame(encodeData(new Uint8Array([1, 2, 3])))
  assert.equal(data.type, 'data')
  assert.deepEqual(new Uint8Array(data.payload), new Uint8Array([1, 2, 3]))

  assert.equal(parseFrame(encodeFin()).type, 'fin')

  const reset = parseFrame(encodeCode(FRAME_RESET, 3))
  assert.equal(reset.type, 'reset')
  assert.equal(reset.code, 3)
  assert.equal(numToCode(3), 'reset')
})
