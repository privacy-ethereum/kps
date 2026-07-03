// Closing a connection must not deadlock a writer that's blocked on flow-control
// backpressure. Uses a server that accepts streams but NEVER reads, so the QUIC
// flow-control window fills and writes park; closing the connection must settle
// them (reject), not hang. (Same failure family as the full-duplex echo deadlock
// fixed earlier.) QUIC JS↔JS — no webrtc juice-loopback issues.

import { test, before, after } from 'node:test'
import assert from 'node:assert/strict'
import { mkdtempSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { dial } from '@kpstreams/quic-client'
import { freeUdpPort } from './helpers.mjs'

let srv, address
before(async () => {
  const { listen } = await import('@kpstreams/server')
  const port = await freeUdpPort()
  const idDir = mkdtempSync(join(tmpdir(), 'kps-bp-'))
  srv = await listen({ port, address: '127.0.0.1', certPath: join(idDir, 'cert.pem'), keyPath: join(idDir, 'key.pem') })
  address = srv.address('127.0.0.1')
  // Accept connections and streams, but never read from them (build backpressure).
  ;(async () => {
    for (;;) {
      let conn
      try { conn = await srv.accept() } catch { return }
      ;(async () => { for (;;) { try { await conn.acceptStream() } catch { return } } })()
    }
  })()
})
after(async () => { await srv?.close() })

test('closing the connection unblocks a backpressured write', async () => {
  const conn = await dial(address)
  const stream = await conn.openStream()
  const writer = stream.writable.getWriter()

  // Far more than any flow-control window; the server never reads, so most of
  // these will park unresolved.
  const big = new Uint8Array(1 << 20) // 1 MiB
  let resolved = 0
  const writes = []
  for (let i = 0; i < 128; i++) writes.push(writer.write(big).then(() => { resolved++ }, () => {}))

  await new Promise((r) => setTimeout(r, 200)) // let backpressure build
  assert.ok(resolved < 128, `expected backpressure to park writes (resolved=${resolved}/128)`)

  // Closing must settle the parked writes rather than hanging.
  await conn.close()
  await Promise.race([
    Promise.allSettled(writes),
    new Promise((_, rej) => setTimeout(() => rej(new Error('backpressured writes did not settle after close')), 8000)),
  ])
})
