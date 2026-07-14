// Connection.remoteAddress (per-IP policy, e.g. rate limits): the dial side
// sees the dialed endpoint; the accept side sees the client's UDP source.
// QUIC is covered JS↔JS; the server's WebRTC side is covered with the Go
// webrtc client (JS↔JS WebRTC can't run headless).

import { test, before, after, describe } from 'node:test'
import assert from 'node:assert/strict'
import { join } from 'node:path'
import { mkdtempSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { dial } from '@kpstreams/quic-client'
import { freeUdpPort, spawnGoClient, goAvailable } from './helpers.mjs'

const skipNoGo = goAvailable() ? false : 'requires the `go` toolchain'

function assertConcrete(remote, label) {
  assert.ok(remote, `${label} remoteAddress missing`)
  assert.equal(typeof remote.ip, 'string')
  assert.notEqual(remote.ip, '')
  assert.notEqual(remote.ip, '0.0.0.0')
  assert.notEqual(remote.ip, '::')
  assert.ok(Number.isInteger(remote.port) && remote.port > 0, `${label} port ${remote.port}`)
}

describe('Connection.remoteAddress', () => {
  let srv, port, accepted
  before(async () => {
    const { listen } = await import('@kpstreams/server')
    port = await freeUdpPort()
    const idDir = mkdtempSync(join(tmpdir(), 'kps-it-ra-'))
    srv = await listen({
      port, address: '127.0.0.1',
      certPath: join(idDir, 'cert.pem'), keyPath: join(idDir, 'key.pem'),
    })
    accepted = []
    ;(async () => {
      for (;;) {
        let conn
        try { conn = await srv.accept() } catch { return }
        accepted.push(conn)
        // Echo streams so the Go client's round-trip completes.
        ;(async () => {
          for (;;) {
            let stream
            try { stream = await conn.acceptStream() } catch { return }
            ;(async () => {
              const reader = stream.readable.getReader()
              const writer = stream.writable.getWriter()
              try {
                for (;;) {
                  const { value, done } = await reader.read()
                  if (done) break
                  if (value && value.length) await writer.write(value)
                }
                await writer.close()
              } catch { /* peer gone */ }
            })()
          }
        })()
      }
    })()
  })
  after(async () => { await srv?.close().catch(() => {}) })

  test('QUIC: dial side sees the dialed endpoint, accept side the source', async () => {
    const conn = await dial(srv.address('127.0.0.1'))
    try {
      assert.deepEqual(conn.remoteAddress, { ip: '127.0.0.1', port })
      // Wait for the accept side to surface.
      for (let i = 0; accepted.length === 0 && i < 100; i++) await new Promise(r => setTimeout(r, 20))
      assert.ok(accepted.length > 0, 'server accepted no connection')
      assertConcrete(accepted.at(-1).remoteAddress, 'server-side (quic)')
    } finally { await conn.close() }
  })

  test('WebRTC accept side sees the client STUN source', { skip: skipNoGo }, async () => {
    const seen = accepted.length
    const { code, err } = await spawnGoClient({ addr: srv.address('127.0.0.1'), transport: 'webrtc', message: 'ra-webrtc' })
    assert.equal(code, 0, `go dial failed: ${err}`)
    for (let i = 0; accepted.length === seen && i < 100; i++) await new Promise(r => setTimeout(r, 20))
    assert.ok(accepted.length > seen, 'server accepted no webrtc connection')
    assertConcrete(accepted.at(-1).remoteAddress, 'server-side (webrtc)')
  })
})
