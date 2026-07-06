// QUIC accept backend: an @infisical/quic QUICServer bound on a loopback port
// behind the demux relay, presenting the pinned identity cert over TLS 1.3 with
// ALPN "h3" (non-identifying) and datagrams enabled. Connected peers surface via
// onConnection as core Connections. quiche interops with the Go quic-go client.

// @infisical/quic is CommonJS with non-statically-analyzable exports, so ESM
// named imports fail at runtime — default-import the module and destructure.
import quicPkg from '@infisical/quic'
import { createRequire } from 'node:module'
import { randomBytes, createHmac, timingSafeEqual } from 'node:crypto'
import { readFileSync } from 'node:fs'
import type { Connection as CoreConnection } from '@kpstreams/core'
import { QuicConnection } from './quic-connection.js'
import type { Identity } from './identity.js'

// @matrixai/quic logs at INFO by default (straight to the app's console). A
// networking library shouldn't spam the host process, so run it at WARN.
// @matrixai/logger is CJS; its ESM default-import shape varies across runtimes
// (Node/Bun/Deno), so load it via createRequire for deterministic CJS semantics.
const loggerPkg = createRequire(import.meta.url)('@matrixai/logger') as {
  default: new (name: string, level: number) => unknown
  LogLevel: { WARN: number }
}
function quietLogger(name: string): unknown { return new loggerPkg.default(name, loggerPkg.LogLevel.WARN) }

const { QUICServer, events: quicEvents } = quicPkg as unknown as {
  QUICServer: new (opts: unknown) => {
    addEventListener: (type: string, cb: (e: Event) => void) => void
    start: (o: { host: string; port: number }) => Promise<void>
    stop: (o?: unknown) => Promise<void>
    // QUICSocket is protected; accessible at runtime for the bound port.
    socket: { readonly port: number; readonly host: string }
  }
  events: { EventQUICServerConnection: { name: string } }
}

export interface QUICBackend {
  /** The loopback port the QUIC server actually bound (it self-assigns). */
  readonly port: number
  close(): Promise<void>
}

// Server token signing (retry/address validation): an HMAC over an ephemeral key.
// Slice the exact 32 bytes — Buffer's backing ArrayBuffer can be a larger pool.
const keyBytes = randomBytes(32)
const serverCrypto = {
  key: keyBytes.buffer.slice(keyBytes.byteOffset, keyBytes.byteOffset + keyBytes.byteLength),
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
  onConnection: (conn: CoreConnection) => void
}): Promise<QUICBackend> {
  const { identity, host, onConnection } = args
  const certPem = readFileSync(identity.certPath, 'utf8')
  const keyPem = readFileSync(identity.keyPath, 'utf8')

  // @infisical/quic uses opaque/branded types for crypto/config; build the
  // options loosely (the runtime shape is what matters).
  const serverOpts = {
    crypto: serverCrypto,
    logger: quietLogger('@kpstreams/server:quic'),
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

  // Bind an ephemeral loopback port (0) and read back the one it chose — the
  // QUIC server self-assigns, so there's no probe-then-rebind race. (node-
  // datachannel's mux can't do this, so the WebRTC backend still pre-picks.)
  await server.start({ host, port: 0 })

  return {
    port: server.socket.port,
    async close() {
      try { await server.stop({ force: true }) } catch { /* ignore */ }
    },
  }
}
