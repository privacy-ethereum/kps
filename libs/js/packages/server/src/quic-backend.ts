// QUIC accept backend: an @infisical/quic QUICServer bound on a loopback port
// behind the demux relay, presenting the pinned identity cert over TLS 1.3 with
// ALPN "h3" (non-identifying) and datagrams enabled. Connected peers surface via
// onConnection as core Connections. quiche interops with the Go quic-go client.

// @infisical/quic is CommonJS with non-statically-analyzable exports, so ESM
// named imports fail at runtime — default-import the module and destructure.
import quicPkg from '@infisical/quic'
import { randomBytes, createHmac, timingSafeEqual } from 'node:crypto'
import { readFileSync } from 'node:fs'
import type { Connection as CoreConnection } from '@kpstreams/core'
import { QuicConnection } from './quic-connection.js'
import type { Identity } from './identity.js'

const { QUICServer, events: quicEvents } = quicPkg as unknown as {
  QUICServer: new (opts: unknown) => {
    addEventListener: (type: string, cb: (e: Event) => void) => void
    start: (o: { host: string; port: number }) => Promise<void>
    stop: (o?: unknown) => Promise<void>
  }
  events: { EventQUICServerConnection: { name: string } }
}

export interface QUICBackend {
  close(): Promise<void>
}

// Server token signing (retry/address validation): an HMAC over an ephemeral key.
const serverCrypto = {
  key: randomBytes(32).buffer,
  ops: {
    async sign(key: ArrayBuffer, data: ArrayBuffer): Promise<ArrayBuffer> {
      const d = createHmac('sha256', Buffer.from(key)).update(Buffer.from(data)).digest()
      return d.buffer.slice(d.byteOffset, d.byteOffset + d.byteLength)
    },
    async verify(key: ArrayBuffer, data: ArrayBuffer, sig: ArrayBuffer): Promise<boolean> {
      const d = createHmac('sha256', Buffer.from(key)).update(Buffer.from(data)).digest()
      return d.byteLength === sig.byteLength && timingSafeEqual(d, Buffer.from(sig))
    },
  },
}

export async function startQUICBackend(args: {
  identity: Identity
  host: string
  port: number
  onConnection: (conn: CoreConnection) => void
}): Promise<QUICBackend> {
  const { identity, host, port, onConnection } = args
  const certPem = readFileSync(identity.certPath, 'utf8')
  const keyPem = readFileSync(identity.keyPath, 'utf8')

  // @infisical/quic uses opaque/branded types for crypto/config; build the
  // options loosely (the runtime shape is what matters).
  const serverOpts = {
    crypto: serverCrypto,
    config: {
      key: keyPem,
      cert: certPem,
      applicationProtos: ['h3'],
      verifyPeer: false, // the server does not pin the client (trust is by certhash, client-side)
      enableDgram: [true, 1000, 1000],
    },
  } as unknown as ConstructorParameters<typeof QUICServer>[0]
  const server = new QUICServer(serverOpts)

  server.addEventListener(quicEvents.EventQUICServerConnection.name, (e: Event) => {
    const qc = (e as unknown as { detail: ConstructorParameters<typeof QuicConnection>[0] }).detail
    onConnection(new QuicConnection(qc))
  })

  await (server as unknown as { start: (o: { host: string; port: number }) => Promise<void> })
    .start({ host, port })

  return {
    async close() {
      try { await (server as unknown as { stop: (o?: unknown) => Promise<void> }).stop({ force: true }) } catch { /* ignore */ }
    },
  }
}
