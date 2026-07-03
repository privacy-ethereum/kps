// Single public port coexistence: one @kpstreams/server address ("ip:port:
// certhash") serving BOTH transports through the demux relay. A JS quic-client,
// a Go QUIC client, and a Go WebRTC client all connect to the SAME port
// concurrently and echo — proving the relay routes STUN → WebRTC / else → QUIC.
// Mirrors Go's TestBothTransportsSamePort. Needs `go` + real UDP.

import { test, before, after } from 'node:test'
import assert from 'node:assert/strict'
import { dial } from '@kpstreams/quic-client'
import {
  startJsServer, spawnGoClient, echoRoundTrip, goAvailable,
} from './helpers.mjs'

const enc = (s) => new TextEncoder().encode(s)
const dec = (b) => new TextDecoder().decode(b)

let server
before(async () => { server = await startJsServer() }) // default: webrtc + quic
after(async () => { await server?.close() })

test('JS QUIC + Go QUIC + Go WebRTC all echo on one port concurrently', { skip: goAvailable() ? false : 'requires the `go` toolchain' }, async () => {
  const jsQuic = (async () => {
    const conn = await dial(server.address)
    try { return dec(await echoRoundTrip(conn, enc('js-quic'))) }
    finally { await conn.close() }
  })()
  const goQuic = spawnGoClient({ addr: server.address, transport: 'quic', message: 'go-quic' })
  const goWebrtc = spawnGoClient({ addr: server.address, transport: 'webrtc', message: 'go-webrtc' })

  const [js, gq, gw] = await Promise.all([jsQuic, goQuic, goWebrtc])
  assert.equal(js, 'js-quic')
  assert.equal(gq.code, 0, `go quic failed: ${gq.err}`)
  assert.equal(gq.out, 'go-quic')
  assert.equal(gw.code, 0, `go webrtc failed: ${gw.err}`)
  assert.equal(gw.out, 'go-webrtc')
})
