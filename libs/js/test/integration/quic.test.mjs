// JS quic-client ↔ @kpstreams/server (QUIC transport) integration.
// Happy path + edge cases: multi-stream, large-payload backpressure, datagrams,
// certhash pinning rejection, and abort. Native UDP is required (no restrictive
// seccomp). Run via the root `npm run test:integration` (builds first).

import { test, before, after } from 'node:test'
import assert from 'node:assert/strict'
import { parseAddress, formatAddress, decodeCerthash, encodeCerthash } from '@kpstreams/core'
import { dial } from '@kpstreams/quic-client'
import { startJsServer, echoRoundTrip } from './helpers.mjs'

let server
before(async () => { server = await startJsServer() })
after(async () => { await server?.close() })

const enc = (s) => new TextEncoder().encode(s)
const dec = (b) => new TextDecoder().decode(b)

test('dial + stream echo', async () => {
  const conn = await dial(server.address)
  try {
    const echoed = await echoRoundTrip(conn, enc('hello-quic'))
    assert.equal(dec(echoed), 'hello-quic')
  } finally { await conn.close() }
})

test('many concurrent streams, each echoes its own payload', async () => {
  const conn = await dial(server.address)
  try {
    const N = 16
    const results = await Promise.all(
      Array.from({ length: N }, (_, i) => echoRoundTrip(conn, enc(`stream-${i}-payload`)))
    )
    results.forEach((r, i) => assert.equal(dec(r), `stream-${i}-payload`))
  } finally { await conn.close() }
})

test('large payload round-trips byte-exact (backpressure)', async () => {
  const conn = await dial(server.address)
  try {
    // 4 MiB of pseudo-random-ish bytes; forces flow control / backpressure.
    const size = 4 * 1024 * 1024
    const payload = Buffer.allocUnsafe(size)
    for (let i = 0; i < size; i++) payload[i] = (i * 2654435761) & 0xff
    const echoed = await echoRoundTrip(conn, payload)
    assert.equal(echoed.length, size)
    assert.ok(echoed.equals(payload), 'echoed bytes must match exactly')
  } finally { await conn.close() }
})

test('datagram round-trip', async () => {
  const conn = await dial(server.address)
  try {
    const payload = enc('ping-datagram')
    // Best-effort transport: retry a few times to avoid a flaky drop.
    let got
    for (let attempt = 0; attempt < 5 && !got; attempt++) {
      await conn.sendDatagram(payload)
      const ac = new AbortController()
      const timer = setTimeout(() => ac.abort(), 500)
      try { got = await conn.receiveDatagram({ signal: ac.signal }) }
      catch { /* timed out this attempt */ }
      finally { clearTimeout(timer) }
    }
    assert.ok(got, 'expected an echoed datagram within retries')
    assert.equal(dec(got), 'ping-datagram')
  } finally { await conn.close() }
})

test('pending receiveDatagram rejects when the connection closes', async () => {
  const conn = await dial(server.address)
  const pending = conn.receiveDatagram() // nothing will ever arrive
  const assertion = assert.rejects(pending) // attach handler before closing
  await conn.close()
  await assertion
})

test('rejects a corrupted certhash (pinning)', async () => {
  const a = parseAddress(server.address)
  const digest = Uint8Array.from(decodeCerthash(a.certhash))
  digest[0] ^= 0xff // valid encoding, wrong pin
  const badAddr = formatAddress({ ...a, certhash: encodeCerthash(digest) })
  await assert.rejects(() => dial(badAddr, { timeoutMs: 5_000 }))
})

test('rejects a pre-aborted dial signal', async () => {
  await assert.rejects(() => dial(server.address, { signal: AbortSignal.abort() }))
})

test('rejects acceptStream with a pre-aborted signal', async () => {
  const conn = await dial(server.address)
  try {
    await assert.rejects(
      () => conn.acceptStream({ signal: AbortSignal.abort() }),
      /acceptStream aborted/,
    )
  } finally { await conn.close() }
})
