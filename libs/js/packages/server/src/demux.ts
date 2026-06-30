// Single-port UDP demux relay. node-datachannel and @infisical/quic each own
// their UDP socket and can't be handed an external one (unlike pion/quic-go,
// which is how the Go server shares a port). So we put a userspace relay in
// front: one public socket, and a per-client loopback "NAT" socket that forwards
// to whichever backend the client's first packet selects (STUN → WebRTC, else →
// QUIC). Each client gets its own forwarding socket so the backends see distinct
// peer addresses (node-datachannel routes post-handshake packets by source
// 4-tuple). The client only ever talks to the public address, so ICE/QUIC are
// unaffected — and because the backends live on loopback, they see clean v4
// peers regardless of the client's family.

import dgram from 'node:dgram'

export interface DemuxOptions {
  /** Public bind host: '' / '0.0.0.0' / '::' → dual-stack wildcard; else a literal. */
  host: string
  port: number
  webrtcPort: number // loopback backend
  quicPort: number // loopback backend
  /** Drop a client's NAT socket after this much inactivity. */
  idleMs?: number
}

export interface Demux {
  close(): void
}

// STUN binding requests carry the magic cookie 0x2112A442 at bytes 4..8; QUIC
// (and anything else) does not.
function isStun(b: Buffer): boolean {
  return b.length >= 8 && b.readUInt32BE(4) === 0x2112a442
}

export function startDemux(opts: DemuxOptions): Demux {
  const idleMs = opts.idleMs ?? 120_000
  const wildcard = !opts.host || opts.host === '0.0.0.0' || opts.host === '::'
  const v6Host = opts.host.includes(':')
  // Dual-stack (udp6 + !ipv6Only) for the wildcard so one socket serves v4+v6.
  const pub = wildcard || v6Host
    ? dgram.createSocket({ type: 'udp6', ipv6Only: false, reuseAddr: true })
    : dgram.createSocket({ type: 'udp4', reuseAddr: true })

  interface Entry { sock: dgram.Socket; backendPort: number; last: number }
  const table = new Map<string, Entry>()

  pub.on('message', (data, rinfo) => {
    const key = `${rinfo.address}:${rinfo.port}`
    let e = table.get(key)
    if (!e) {
      const backendPort = isStun(data) ? opts.webrtcPort : opts.quicPort
      const sock = dgram.createSocket('udp4') // backends bind 127.0.0.1
      sock.on('message', (resp) => {
        const cur = table.get(key)
        if (cur) cur.last = Date.now()
        try { pub.send(resp, rinfo.port, rinfo.address) } catch { /* client gone */ }
      })
      sock.on('error', () => {})
      sock.bind(0, '127.0.0.1')
      e = { sock, backendPort, last: Date.now() }
      table.set(key, e)
    }
    e.last = Date.now()
    try { e.sock.send(data, e.backendPort, '127.0.0.1') } catch { /* backend gone */ }
  })
  pub.on('error', () => {})
  pub.bind(opts.port, wildcard ? undefined : opts.host)

  const gc = setInterval(() => {
    const now = Date.now()
    for (const [k, e] of table) {
      if (now - e.last > idleMs) { try { e.sock.close() } catch {} ; table.delete(k) }
    }
  }, Math.min(idleMs, 30_000))
  if (typeof (gc as { unref?: () => void }).unref === 'function') (gc as { unref: () => void }).unref()

  return {
    close() {
      clearInterval(gc)
      for (const e of table.values()) { try { e.sock.close() } catch {} }
      table.clear()
      try { pub.close() } catch {}
    },
  }
}
