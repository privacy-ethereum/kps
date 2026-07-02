// Standalone @kpstreams/server runner for the Playwright interop test. Run as a
// SUBPROCESS (not in-process in global-setup): the node-datachannel WebRTC
// backend can hold native threads that block a clean exit, so isolating it in
// its own process keeps the Playwright runner unaffected — global-setup just
// SIGKILLs it on teardown. Binds a free loopback port, prints its dial address
// ("127.0.0.1:<port>:<certhash>") on stdout, and echoes every stream.
import { listen } from '../../libs/js/packages/server/dist/index.js'
import dgram from 'node:dgram'
import { mkdtempSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'

function freeUdpPort() {
  return new Promise((res, rej) => {
    const s = dgram.createSocket('udp4')
    s.once('error', rej)
    s.bind(0, '127.0.0.1', () => { const p = s.address().port; s.close(() => res(p)) })
  })
}

const port = await freeUdpPort()
const idDir = mkdtempSync(join(tmpdir(), 'kps-it-id-'))
const srv = await listen({
  port, address: '127.0.0.1', transports: ['webrtc'],
  certPath: join(idDir, 'cert.pem'), keyPath: join(idDir, 'key.pem'),
})
process.stdout.write(srv.address('127.0.0.1') + '\n')

;(async () => {
  for (;;) {
    let conn
    try { conn = await srv.accept() } catch { return }
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

const bye = () => process.exit(0)
process.on('SIGTERM', bye)
process.on('SIGINT', bye)
