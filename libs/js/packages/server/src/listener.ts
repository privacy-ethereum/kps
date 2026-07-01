// Unified listener: one public UDP port serving BOTH transports. node-datachannel
// (WebRTC) and @infisical/quic (QUIC) each bind a private loopback port; a demux
// relay on the public port routes each client to the right backend (STUN →
// WebRTC, else → QUIC) and NATs responses back out the single port. So the server
// advertises ONE address ("ip:port:certhash") for both transports — the Go
// single-port model, recovered in Node where the libs can't share a socket.

import dgram from 'node:dgram'
import { formatAddress, type Connection as CoreConnection } from '@kpstreams/core'
import { loadOrCreateIdentity } from './identity.js'
import { startWebRTCBackend, type WebRTCBackend } from './webrtc-backend.js'
import { startQUICBackend, type QUICBackend } from './quic-backend.js'
import { startDemux } from './demux.js'

export interface ListenOptions {
  port: number
  /** Public bind address; defaults to '0.0.0.0' (dual-stack wildcard). */
  address?: string
  /** Persisted self-signed cert/key paths (created on first run). */
  certPath?: string
  keyPath?: string
  /** Transports to accept; defaults to both. */
  transports?: Array<'webrtc' | 'quic'>
}

export interface Listener {
  readonly certhash: string
  readonly port: number
  /** Format a dial address "ip:port:certhash" (IPv6 hosts are bracketed). */
  address(ip: string): string
  /** Resolve the next connected peer (either transport). */
  accept(opts?: { signal?: AbortSignal }): Promise<CoreConnection>
  close(): Promise<void>
}

function freeUdpPort(): Promise<number> {
  return new Promise((resolve, reject) => {
    const s = dgram.createSocket('udp4')
    s.once('error', reject)
    s.bind(0, '127.0.0.1', () => {
      const addr = s.address()
      const port = typeof addr === 'object' && addr ? addr.port : 0
      s.close(() => resolve(port))
    })
  })
}

export async function listen(opts: ListenOptions): Promise<Listener> {
  const id = await loadOrCreateIdentity(opts.certPath, opts.keyPath)
  const publicHost = opts.address ?? '0.0.0.0'
  const transports = new Set(opts.transports ?? ['webrtc', 'quic'])

  const ready: CoreConnection[] = []
  const waiters: Array<{ resolve: (c: CoreConnection) => void; reject: (e: Error) => void }> = []
  let closed = false

  const onConnection = (conn: CoreConnection) => {
    if (closed) { conn.close().catch(() => {}); return }
    const w = waiters.shift()
    if (w) w.resolve(conn)
    else ready.push(conn)
  }

  // Backends live on loopback behind the relay. QUIC self-assigns its port
  // (bind :0, report it) so there's no race; node-datachannel's mux needs a
  // real port up front, so WebRTC pre-picks a free one (tiny loopback window).
  let webrtc: WebRTCBackend | undefined
  let quic: QUICBackend | undefined
  const webrtcPort = transports.has('webrtc') ? await freeUdpPort() : 0
  if (transports.has('webrtc')) {
    webrtc = startWebRTCBackend({ identity: id, host: '127.0.0.1', port: webrtcPort, onConnection })
  }
  if (transports.has('quic')) {
    quic = await startQUICBackend({ identity: id, host: '127.0.0.1', onConnection })
  }
  // With QUIC disabled, stray non-STUN harmlessly routes to the WebRTC backend
  // (which ignores it).
  const quicPort = quic?.port ?? webrtcPort
  const demux = await startDemux({ host: publicHost, port: opts.port, webrtcPort, quicPort })

  return {
    certhash: id.certhash,
    port: opts.port,
    address(ip: string) {
      return formatAddress({ ip, port: opts.port, certhash: id.certhash })
    },
    accept(o = {}) {
      const r = ready.shift()
      if (r) return Promise.resolve(r)
      if (closed) return Promise.reject(new Error('kps: listener closed'))
      if (o.signal?.aborted) return Promise.reject(new Error('kps: accept aborted'))
      return new Promise<CoreConnection>((resolve, reject) => {
        const onAbort = () => {
          const i = waiters.indexOf(waiter)
          if (i >= 0) waiters.splice(i, 1)
          reject(new Error('kps: accept aborted'))
        }
        const waiter = {
          resolve: (c: CoreConnection) => { o.signal?.removeEventListener('abort', onAbort); resolve(c) },
          reject: (e: Error) => { o.signal?.removeEventListener('abort', onAbort); reject(e) },
        }
        waiters.push(waiter)
        o.signal?.addEventListener('abort', onAbort, { once: true })
      })
    },
    async close() {
      closed = true
      demux.close()
      webrtc?.close()
      if (quic) await quic.close()
      // Close connections that were accepted by a backend but never consumed.
      while (ready.length) await ready.shift()!.close().catch(() => {})
      while (waiters.length) waiters.shift()!.reject(new Error('kps: listener closed'))
    },
  }
}
