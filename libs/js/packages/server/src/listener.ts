// Listener: one IceUdpMuxListener owns the UDP port; when a client whose ufrag
// we don't know in advance sends its first STUN, onUnhandledStunRequest hands us
// the ufrag and we lazily create a per-client PeerConnection (ICE-lite answerer,
// our pinned cert, ICE password derived from the certhash). This is the Go
// listener's spawnPC, expressed with node-datachannel.

import { PeerConnection, IceUdpMuxListener, type IceUdpMuxRequest } from 'node-datachannel'
import { deriveICEPwd, buildClientOffer } from '@kpstreams/core/webrtc'
import { formatAddress, type Connection as CoreConnection } from '@kpstreams/core'
import { Connection } from './connection.js'
import { loadOrCreateIdentity } from './identity.js'

export interface ListenOptions {
  port: number
  /** Bind address; defaults to '127.0.0.1'. Use '0.0.0.0'/'::' for public binds. */
  address?: string
  /** Persisted self-signed cert/key paths (created on first run). */
  certPath?: string
  keyPath?: string
}

export interface Listener {
  /** The certhash advertised in the address. */
  readonly certhash: string
  readonly port: number
  /** Format a dial address "ip:port:certhash" (IPv6 hosts are bracketed). */
  address(ip: string): string
  /** Resolve the next connected peer. */
  accept(opts?: { signal?: AbortSignal }): Promise<CoreConnection>
  close(): Promise<void>
}

export async function listen(opts: ListenOptions): Promise<Listener> {
  const id = await loadOrCreateIdentity(opts.certPath, opts.keyPath)
  const bind = opts.address ?? '127.0.0.1'
  const mux = new IceUdpMuxListener(opts.port, bind)

  const peers = new Map<string, PeerConnection>()
  const ready: Connection[] = []
  const waiters: Array<{ resolve: (c: CoreConnection) => void; reject: (e: Error) => void }> = []
  let closed = false

  const deliver = (conn: Connection) => {
    const w = waiters.shift()
    if (w) w.resolve(conn)
    else ready.push(conn)
  }

  mux.onUnhandledStunRequest(async (req: IceUdpMuxRequest) => {
    if (closed || !req.ufrag || peers.has(req.ufrag)) return
    const ufrag = req.ufrag
    const pwd = await deriveICEPwd(id.digest, ufrag)

    const pc = new PeerConnection(`kps-${ufrag}`, {
      iceServers: [],
      disableAutoNegotiation: true,
      enableIceUdpMux: true,
      bindAddress: bind,
      portRangeBegin: opts.port,
      portRangeEnd: opts.port,
      certificatePemFile: id.certPath,
      keyPemFile: id.keyPath,
      disableFingerprintVerification: true, // we don't pin the client
    })
    peers.set(ufrag, pc)

    const conn = new Connection(pc)
    conn.ready.then(() => { if (!closed) deliver(conn) }).catch(() => { /* failed before open */ })
    conn.closed.then(() => peers.delete(ufrag)).catch(() => peers.delete(ufrag))

    try {
      // Adopt the client's ufrag + derived pwd as our local ICE credentials and
      // answer the fabricated offer. node-datachannel generates the real answer
      // (our cert fingerprint, host candidate) which the client never sees — it
      // has already synthesized the same answer from the address.
      pc.setRemoteDescription(buildClientOffer(ufrag, pwd), 'offer')
      pc.setLocalDescription('answer', { iceUfrag: ufrag, icePwd: pwd })
    } catch {
      peers.delete(ufrag)
      try { pc.close() } catch { /* ignore */ }
    }
  })

  return {
    certhash: id.certhash,
    port: opts.port,
    address(ip: string) {
      return formatAddress({ ip, port: opts.port, certhash: id.certhash })
    },
    accept(o = {}) {
      const r = ready.shift()
      if (r) return Promise.resolve(r as CoreConnection)
      if (closed) return Promise.reject(new Error('kps: listener closed'))
      return new Promise<CoreConnection>((resolve, reject) => {
        const waiter = { resolve, reject }
        waiters.push(waiter)
        o.signal?.addEventListener('abort', () => {
          const i = waiters.indexOf(waiter)
          if (i >= 0) waiters.splice(i, 1)
          reject(new Error('kps: accept aborted'))
        }, { once: true })
      })
    },
    async close() {
      closed = true
      try { mux.stop() } catch { /* ignore */ }
      for (const pc of peers.values()) try { pc.close() } catch { /* ignore */ }
      peers.clear()
      while (waiters.length) waiters.shift()!.reject(new Error('kps: listener closed'))
    },
  }
}
