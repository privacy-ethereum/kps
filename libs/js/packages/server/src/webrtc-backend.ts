// WebRTC accept backend: an IceUdpMuxListener bound on a loopback port behind
// the demux relay. Per unknown ufrag, lazily spawn an ICE-lite answerer with the
// certhash-derived password + pinned cert, and surface connected peers via
// onConnection. (Extracted from the original single-transport listener.)

import { PeerConnection, IceUdpMuxListener, type IceUdpMuxRequest } from 'node-datachannel'
import { deriveICEPwd, buildClientOffer } from '@kpstreams/core/webrtc'
import type { Connection as CoreConnection } from '@kpstreams/core'
import { Connection } from './connection.js'
import type { Identity } from './identity.js'

export interface WebRTCBackend {
  close(): void
}

// RFC 8839 ice-char: ALPHA / DIGIT / "+" / "/". A ufrag with any other byte
// (e.g. base64url's '-'/'_') makes libdatachannel throw "Invalid ICE attributes"
// from setLocalDescription — an UNCATCHABLE C++ throw that aborts the whole
// process (same class as murat-dogan/node-datachannel#427). A malformed ufrag
// therefore has to be rejected BEFORE it reaches the native layer, or any peer
// (buggy or hostile) could crash the server. Length is bounded per RFC 8839
// (4..256 chars).
const ICE_UFRAG_RE = /^[A-Za-z0-9+/]{4,256}$/


export function startWebRTCBackend(args: {
  identity: Identity
  host: string
  port: number
  onConnection: (conn: CoreConnection) => void
}): WebRTCBackend {
  const { identity, host, port, onConnection } = args
  const mux = new IceUdpMuxListener(port, host)
  const peers = new Map<string, PeerConnection>()
  let closed = false

  mux.onUnhandledStunRequest(async (req: IceUdpMuxRequest) => {
    if (closed || !req.ufrag || peers.has(req.ufrag)) return
    // Reject an out-of-charset ufrag up front: passing it to setLocalDescription
    // would abort the process (see ICE_UFRAG_RE). Drop the STUN instead.
    if (!ICE_UFRAG_RE.test(req.ufrag)) return
    const ufrag = req.ufrag

    // Claim the ufrag SYNCHRONOUSLY before any await. STUN is retransmitted, so
    // onUnhandledStunRequest can fire again for the same ufrag before a PC exists;
    // creating the PC (which needs no password) and registering it up front makes
    // the has()-guard above authoritative and prevents a duplicate/orphaned PC.
    const pc = new PeerConnection(`kps-${ufrag}`, {
      iceServers: [],
      disableAutoNegotiation: true,
      enableIceUdpMux: true,
      bindAddress: host,
      portRangeBegin: port,
      portRangeEnd: port,
      certificatePemFile: identity.certPath,
      keyPemFile: identity.keyPath,
      disableFingerprintVerification: true, // we don't pin the client
    })
    peers.set(ufrag, pc)

    // Only delete our own entry (a later ufrag reuse must not be clobbered).
    const drop = () => { if (peers.get(ufrag) === pc) peers.delete(ufrag) }
    const conn = new Connection(pc)
    conn.ready.then(() => { if (!closed) onConnection(conn) }).catch(() => { /* failed before open */ })
    conn.closed.then(drop).catch(drop)

    try {
      const pwd = await deriveICEPwd(identity.digest, ufrag)
      pc.setRemoteDescription(buildClientOffer(ufrag, pwd), 'offer')
      pc.setLocalDescription('answer', { iceUfrag: ufrag, icePwd: pwd })
    } catch {
      drop()
      try { pc.close() } catch { /* ignore */ }
    }
  })

  return {
    close() {
      // `closed = true` first makes onUnhandledStunRequest a no-op, so no new PC
      // is built from an in-flight STUN; then tear down the per-peer PCs before
      // stopping the mux.
      closed = true
      for (const pc of peers.values()) { try { pc.close() } catch { /* ignore */ } }
      peers.clear()
      try { mux.stop() } catch { /* ignore */ }
    },
  }
}
