// @kpstreams/quic-client — native QUIC client for KPS. Dials ip:port:certhash
// over QUIC (TLS 1.3, ALPN "h3", datagrams) and pins the server's certificate
// against the certhash — no CA. Returns the transport-neutral core Connection.
//
// QUIC carries FIN/RESET/STOP_SENDING + datagrams natively, so this does NOT use
// @kpstreams/core/webrtc. The QUICStream/QUICConnection adapters are shared with
// @kpstreams/server (copied for now; a shared package could dedupe them).

import quicPkg from '@infisical/quic'
import { randomFillSync, createHash, timingSafeEqual } from 'node:crypto'
import { parseAddress, decodeCerthash, type Connection, type DialOptions } from '@kpstreams/core'
import { QuicConnection } from './quic-connection.js'

export { parseAddress, formatAddress } from '@kpstreams/core'
export type { Address, Connection, Stream, DialOptions } from '@kpstreams/core'

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
  const a = parseAddress(addr)
  const digest = Buffer.from(decodeCerthash(a.certhash))

  const client = await QUICClient.createQUICClient(
    {
      host: a.ip,
      port: a.port,
      crypto: clientCrypto,
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
    { timer: opts.timeoutMs ?? 15_000 },
  )

  const conn = new QuicConnection(client.connection)
  // Tearing down the connection must also close the client's UDP socket.
  conn.closed.finally(() => { client.destroy({ force: true }).catch(() => {}) }).catch(() => {})
  return conn
}
