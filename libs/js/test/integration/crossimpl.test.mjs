// Cross-implementation interop (JS ↔ Go). Fills the pairs the Go-only suite
// can't: JS quic-client → Go server, and Go client (both transports) → JS
// server. Go↔Go is covered in libs/go/*_test.go. Requires the `go` toolchain
// and real UDP; skips cleanly if `go` is absent.

import { test, before, after, describe } from 'node:test'
import assert from 'node:assert/strict'
import { dial } from '@kpstreams/quic-client'
import {
  startJsServer, spawnGoServer, spawnGoClient, echoRoundTrip, goAvailable,
} from './helpers.mjs'

// Exactly 24 bytes; × 65536 = 1.5 MiB for the flow-control rows.
const MSG_24B = 'flow-credit-0123456789ab'

const enc = (s) => new TextEncoder().encode(s)
const dec = (b) => new TextDecoder().decode(b)
const skipNoGo = goAvailable() ? false : 'requires the `go` toolchain'

describe('JS quic-client → Go server', { skip: skipNoGo }, () => {
  let go
  before(async () => { go = await spawnGoServer() })
  after(async () => { await go?.kill() })

  test('stream echo (byte-exact)', async () => {
    const conn = await dial(go.address)
    try {
      const echoed = await echoRoundTrip(conn, enc('js-quic-to-go'))
      assert.equal(dec(echoed), 'js-quic-to-go')
    } finally { await conn.close() }
  })

  test('large payload round-trips byte-exact', async () => {
    const conn = await dial(go.address)
    try {
      const size = 1024 * 1024
      const payload = Buffer.allocUnsafe(size)
      for (let i = 0; i < size; i++) payload[i] = (i * 2654435761) & 0xff
      const echoed = await echoRoundTrip(conn, payload)
      assert.ok(echoed.equals(payload))
    } finally { await conn.close() }
  })
})

describe('Go client → JS server', { skip: skipNoGo }, () => {
  let server
  before(async () => { server = await startJsServer() }) // both transports
  after(async () => { await server?.close() })

  test('QUIC transport echoes', async () => {
    const { code, out, err } = await spawnGoClient({ addr: server.address, transport: 'quic', message: 'go-quic-to-js' })
    assert.equal(code, 0, `go dial failed: ${err}`)
    assert.equal(out, 'go-quic-to-js')
  })

  test('WebRTC transport echoes', async () => {
    const { code, out, err } = await spawnGoClient({ addr: server.address, transport: 'webrtc', message: 'go-webrtc-to-js' })
    assert.equal(code, 0, `go dial failed: ${err}`)
    assert.equal(out, 'go-webrtc-to-js')
  })

  test('WebRTC large payload exercises cross-impl flow control (§6.5)', async () => {
    // 24 B × 65536 = 1.5 MiB: crosses the 1 MiB stream window, so the echo
    // only completes if MAX_STREAM_DATA / MAX_DATA credit flows both ways
    // between the implementations. Stays under the ~2 MiB write-all-then-read
    // deadlock threshold of the dial CLI.
    const { code, out, err } = await spawnGoClient({
      addr: server.address, transport: 'webrtc', message: MSG_24B, repeat: 65536, timeoutMs: 30_000,
    })
    assert.equal(code, 0, `go dial failed: ${err}`)
    assert.equal(out, `echoed ${MSG_24B.length * 65536} bytes OK`)
  })

  // WebRTC datagram symmetry: the QUIC datagram round-trip is covered in
  // quic.test.mjs; the WebRTC path can't run headless JS↔JS (juice loopback), so
  // exercise it cross-impl with the Go webrtc client against the JS server's
  // datagram echo.
  test('WebRTC datagram round-trip', async () => {
    const { code, out, err } = await spawnGoClient({ addr: server.address, transport: 'webrtc', message: 'go-webrtc-dgram', datagram: true })
    assert.equal(code, 0, `go datagram dial failed: ${err}`)
    assert.equal(out, 'go-webrtc-dgram')
  })
})
