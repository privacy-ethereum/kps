// Cross-implementation interop rows involving Rust (libs/rust). Mirrors
// crossimpl.test.mjs (JS ↔ Go): JS quic-client → Rust server, Rust client
// (both transports) → JS server, and Rust ↔ Go via the spawned binaries.
// Requires the `cargo` toolchain (plus `go` for the Rust↔Go rows) and real
// UDP; skips cleanly when a toolchain is absent.

import { test, before, after, describe } from 'node:test'
import assert from 'node:assert/strict'
import { dial } from '@kpstreams/quic-client'
import {
  startJsServer, spawnGoServer, spawnRustServer, spawnRustClient, spawnGoClient,
  echoRoundTrip, goAvailable, rustAvailable,
} from './helpers.mjs'

// Exactly 24 bytes; × 65536 = 1.5 MiB for the flow-control rows.
const MSG_24B = 'flow-credit-0123456789ab'

const enc = (s) => new TextEncoder().encode(s)
const dec = (b) => new TextDecoder().decode(b)
const skipNoRust = rustAvailable() ? false : 'requires the `cargo` toolchain'
const skipNoBoth = !rustAvailable() ? 'requires the `cargo` toolchain'
  : !goAvailable() ? 'requires the `go` toolchain' : false

describe('JS quic-client → Rust server', { skip: skipNoRust }, () => {
  let rust
  before(async () => { rust = await spawnRustServer() })
  after(async () => { await rust?.kill() })

  test('stream echo (byte-exact)', async () => {
    const conn = await dial(rust.address)
    try {
      const echoed = await echoRoundTrip(conn, enc('js-quic-to-rust'))
      assert.equal(dec(echoed), 'js-quic-to-rust')
    } finally { await conn.close() }
  })

  test('large payload round-trips byte-exact', async () => {
    const conn = await dial(rust.address)
    try {
      const size = 1024 * 1024
      const payload = Buffer.allocUnsafe(size)
      for (let i = 0; i < size; i++) payload[i] = (i * 2654435761) & 0xff
      const echoed = await echoRoundTrip(conn, payload)
      assert.ok(echoed.equals(payload))
    } finally { await conn.close() }
  })

  test('datagram round-trip', async () => {
    const conn = await dial(rust.address)
    try {
      const payload = enc('js-dgram-to-rust')
      // Best-effort: retry until the echo arrives.
      for (let i = 0; i < 10; i++) {
        await conn.sendDatagram(payload)
        const got = await Promise.race([
          conn.receiveDatagram(),
          new Promise((res) => setTimeout(res, 500)),
        ])
        if (got) { assert.equal(dec(got), 'js-dgram-to-rust'); return }
      }
      assert.fail('no datagram echoed after 10 attempts')
    } finally { await conn.close() }
  })
})

describe('Rust client → JS server', { skip: skipNoRust }, () => {
  let server
  before(async () => { server = await startJsServer() }) // both transports
  after(async () => { await server?.close() })

  test('QUIC transport echoes', async () => {
    const { code, out, err } = await spawnRustClient({ addr: server.address, transport: 'quic', message: 'rust-quic-to-js' })
    assert.equal(code, 0, `rust dial failed: ${err}`)
    assert.equal(out, 'rust-quic-to-js')
  })

  test('WebRTC transport echoes', async () => {
    const { code, out, err } = await spawnRustClient({ addr: server.address, transport: 'webrtc', message: 'rust-webrtc-to-js' })
    assert.equal(code, 0, `rust dial failed: ${err}`)
    assert.equal(out, 'rust-webrtc-to-js')
  })

  test('WebRTC large payload exercises cross-impl flow control (§6.5)', async () => {
    // 24 B × 65536 = 1.5 MiB: crosses the 1 MiB stream window, so the echo
    // only completes if MAX_STREAM_DATA / MAX_DATA credit flows both ways
    // between the implementations. Stays under the ~2 MiB write-all-then-read
    // deadlock threshold of the dial CLI.
    const { code, out, err } = await spawnRustClient({
      addr: server.address, transport: 'webrtc', message: MSG_24B, repeat: 65536, timeoutMs: 30_000,
    })
    assert.equal(code, 0, `rust dial failed: ${err}`)
    assert.equal(out, `echoed ${MSG_24B.length * 65536} bytes OK`)
  })

  test('WebRTC datagram round-trip', async () => {
    const { code, out, err } = await spawnRustClient({ addr: server.address, transport: 'webrtc', message: 'rust-webrtc-dgram', datagram: true })
    assert.equal(code, 0, `rust datagram dial failed: ${err}`)
    assert.equal(out, 'rust-webrtc-dgram')
  })
})

// Rust ↔ Go rows, spawning both binaries — completes the three-implementation
// matrix in one executable place (Go↔Go lives in libs/go, Rust↔Rust in
// libs/rust/kps/tests).
describe('Rust client → Go server', { skip: skipNoBoth }, () => {
  let go
  before(async () => { go = await spawnGoServer() })
  after(async () => { await go?.kill() })

  test('QUIC transport echoes', async () => {
    const { code, out, err } = await spawnRustClient({ addr: go.address, transport: 'quic', message: 'rust-quic-to-go' })
    assert.equal(code, 0, `rust dial failed: ${err}`)
    assert.equal(out, 'rust-quic-to-go')
  })

  test('WebRTC transport echoes', async () => {
    const { code, out, err } = await spawnRustClient({ addr: go.address, transport: 'webrtc', message: 'rust-webrtc-to-go' })
    assert.equal(code, 0, `rust dial failed: ${err}`)
    assert.equal(out, 'rust-webrtc-to-go')
  })

  test('WebRTC large payload exercises cross-impl flow control (§6.5)', async () => {
    // 24 B × 65536 = 1.5 MiB: crosses the 1 MiB stream window, so the echo
    // only completes if MAX_STREAM_DATA / MAX_DATA credit flows both ways
    // between the implementations. Stays under the ~2 MiB write-all-then-read
    // deadlock threshold of the dial CLI.
    const { code, out, err } = await spawnRustClient({
      addr: go.address, transport: 'webrtc', message: MSG_24B, repeat: 65536, timeoutMs: 30_000,
    })
    assert.equal(code, 0, `rust dial failed: ${err}`)
    assert.equal(out, `echoed ${MSG_24B.length * 65536} bytes OK`)
  })
})

describe('Go client → Rust server', { skip: skipNoBoth }, () => {
  let rust
  before(async () => { rust = await spawnRustServer() })
  after(async () => { await rust?.kill() })

  test('QUIC transport echoes', async () => {
    const { code, out, err } = await spawnGoClient({ addr: rust.address, transport: 'quic', message: 'go-quic-to-rust' })
    assert.equal(code, 0, `go dial failed: ${err}`)
    assert.equal(out, 'go-quic-to-rust')
  })

  test('WebRTC transport echoes', async () => {
    const { code, out, err } = await spawnGoClient({ addr: rust.address, transport: 'webrtc', message: 'go-webrtc-to-rust' })
    assert.equal(code, 0, `go dial failed: ${err}`)
    assert.equal(out, 'go-webrtc-to-rust')
  })

  test('WebRTC large payload exercises cross-impl flow control (§6.5)', async () => {
    // 24 B × 65536 = 1.5 MiB: crosses the 1 MiB stream window, so the echo
    // only completes if MAX_STREAM_DATA / MAX_DATA credit flows both ways
    // between the implementations. Stays under the ~2 MiB write-all-then-read
    // deadlock threshold of the dial CLI.
    const { code, out, err } = await spawnGoClient({
      addr: rust.address, transport: 'webrtc', message: MSG_24B, repeat: 65536, timeoutMs: 30_000,
    })
    assert.equal(code, 0, `go dial failed: ${err}`)
    assert.equal(out, `echoed ${MSG_24B.length * 65536} bytes OK`)
  })

  test('WebRTC datagram round-trip', async () => {
    const { code, out, err } = await spawnGoClient({ addr: rust.address, transport: 'webrtc', message: 'go-webrtc-dgram-rust', datagram: true })
    assert.equal(code, 0, `go datagram dial failed: ${err}`)
    assert.equal(out, 'go-webrtc-dgram-rust')
  })
})
