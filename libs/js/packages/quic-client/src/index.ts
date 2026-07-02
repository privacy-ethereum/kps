// @kpstreams/quic-client — native QUIC client for KPS. Dials ip:port:certhash
// over QUIC (TLS 1.3, ALPN "h3", datagrams) and pins the server's certificate
// against the certhash — no CA. Returns the transport-neutral core Connection.
//
// QUIC carries FIN/RESET/STOP_SENDING + datagrams natively, so this does NOT use
// @kpstreams/core/webrtc. The QUICStream/QUICConnection adapters are shared with
// @kpstreams/server (copied for now; a shared package could dedupe them).

import quicPkg from '@infisical/quic'
import loggerPkg from '@matrixai/logger'
import { randomFillSync, createHash, timingSafeEqual } from 'node:crypto'
import { parseAddress, decodeCerthash, type Connection, type Stream, type DialOptions } from '@kpstreams/core'
import { QuicConnection } from './quic-connection.js'

// @matrixai/quic logs at INFO by default (straight to the app's console); run it
// at WARN so dialing doesn't spam the host process.
const Logger = (loggerPkg as { default: new (name: string, level: number) => unknown }).default
const LogLevel = (loggerPkg as unknown as { LogLevel: { WARN: number } }).LogLevel

// Convenience re-exports so callers don't also need to import @kpstreams/core.
// (Kept identical to @kpstreams/webrtc-client — same job, same surface.)
export { parseAddress, formatAddress } from '@kpstreams/core'
export type {
  Address, Connection, Stream,
  DialOptions, OpenStreamOptions,
  ConnCloseInfo, StreamCloseInfo,
  KpsErrorCode, KpsReason,
} from '@kpstreams/core'

const { QUICClient } = quicPkg as unknown as {
  QUICClient: {
    createQUICClient(opts: unknown, ctx?: unknown): Promise<{
      connection: ConstructorParameters<typeof QuicConnection>[0]
      destroy(o?: unknown): Promise<void>
    }>
  }
}

// CryptoError.BadCertificate — returned from the TLS verify callback to reject a
// server whose certificate doesn't match the pinned certhash.
const BAD_CERTIFICATE = 298

const clientCrypto = {
  ops: {
    async randomBytes(data: ArrayBuffer): Promise<void> {
      randomFillSync(new Uint8Array(data))
    },
  },
}

export async function dial(addr: string, opts: DialOptions = {}): Promise<Connection> {
  if (opts.signal?.aborted) throw new Error('kps: dial aborted')
  const a = parseAddress(addr)
  const digest = Buffer.from(decodeCerthash(a.certhash))

  const client = await QUICClient.createQUICClient(
    {
      host: a.ip,
      port: a.port,
      crypto: clientCrypto,
      logger: new Logger('@kpstreams/quic-client', LogLevel.WARN),
      config: {
        applicationProtos: ['h3'],
        verifyPeer: true,
        // Trust is by certhash, not PKI: accept iff sha256(leaf cert) == digest.
        verifyCallback: async (certs: Uint8Array[]) => {
          const leaf = certs?.[0]
          if (!leaf) return BAD_CERTIFICATE
          const d = createHash('sha256').update(leaf).digest()
          return d.length === digest.length && timingSafeEqual(d, digest) ? undefined : BAD_CERTIFICATE
        },
        enableDgram: [true, 1000, 1000],
      },
    },
    { timer: opts.timeoutMs ?? 15_000, signal: opts.signal },
  )

  const conn = new QuicConnection(client.connection)
  // Tearing down the connection must also close the client's UDP socket.
  conn.closed.finally(() => { client.destroy({ force: true }).catch(() => {}) }).catch(() => {})
  return conn
}

// One-shot convenience over dial → openStream. The returned stream owns the
// hidden connection: closing the stream closes the connection. Mirrors
// @kpstreams/webrtc-client's openStream.
export async function openStream(addr: string, opts?: DialOptions): Promise<Stream> {
  const conn = await dial(addr, opts)
  try {
    const stream = await conn.openStream({ signal: opts?.signal })
    void stream.closed.finally(() => { void conn.close() })
    return stream
  } catch (err) {
    await conn.close()
    throw err
  }
}
