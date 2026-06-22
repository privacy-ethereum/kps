# KPS Security & Censorship-Resistance Model (draft)

> Companion to [`SPEC.md`](SPEC.md). Records the threat model KPS targets, the
> mechanisms that meet it, and what is deliberately out of scope.

## 1. Authentication / confidentiality (the core guarantee)

A KPS address pins a self-signed certificate by hash (`SPEC.md` §3). The client
verifies the presented certificate hashes to the pinned certhash on **both**
transports (DTLS for WebRTC, TLS 1.3 for QUIC) and performs **no** CA/hostname
validation. Given the address arrives intact, the session cannot be MITM'd. The
server does not authenticate the client (application-level identity, if any, is
the application's concern).

## 2. Censorship-resistance: scope

**In scope (near term):** deny a **low-effort blanket censor** — one that
keyword-matches a protocol marker and blocks it — any easy KPS signal, and avoid
being *noticeably unusual for WebRTC* to a passive on-path observer of a real
client.

**Out of scope (near term):**
- **Endpoint / IP blocking.** KPS endpoints are discoverable by some mechanism
  *not defined by KPS*. A censor that identifies that discovery mechanism can
  enumerate and IP-block endpoints. Defeating this needs **private endpoints** —
  a separate, later story. No protocol-level trick here helps.
- **Blocking all WebRTC / all QUIC.** Direct browser access is a core goal, so
  the WebRTC path is accepted as blockable by a censor willing to take the
  (high) collateral of banning WebRTC wholesale.

**The ceiling.** Every mechanism below keys on the **certhash**, which is *not
secret from a censor who uses the same discovery mechanism*. So they defeat the
blanket censor but not one who harvests certhashes — which is exactly the
"private endpoints, later" boundary, not a regression.

## 3. Mechanism: the certhash is a pre-shared key on both transports

### WebRTC path
- **ICE password from certhash.** `ice-pwd = HMAC-SHA256(certhash_digest,
  "kps-ice-pwd-v1:" || ice-ufrag)` (`SPEC.md` §5.2), replacing the libp2p
  `ufrag == pwd` convention. Removes the recomputable-`MESSAGE-INTEGRITY`
  fingerprint, lets the ufrag return to normal WebRTC length, and means only a
  certhash-holder can pass STUN integrity → **active probers without the
  certhash never reach DTLS** and the server stays effectively silent to them.
- **Sanitized certificate.** Empty/random Subject CN and random serial (`SPEC.md`
  §3), so a certificate seen on the wire carries no KPS-identifying metadata.

### QUIC path
- **Non-identifying ALPN/SNI.** Do not advertise `kps/1`; use `h3` or none, and
  an empty/innocuous SNI (`SPEC.md` §5.3). A blanket keyword censor has nothing
  to match on the (publicly decryptable) QUIC Initial. Safe because demux is
  structural and KPS owns the port; version negotiation moves out of ALPN.

## 4. Residual exposure under DTLS 1.2 (WebRTC)

DTLS 1.2 sends the server Certificate message in **cleartext**; DTLS 1.3
encrypts it. With the §3 sanitization, a passive observer in 1.2 sees no
KPS-specific field — but a **200-year cert validity** is still *unusual for
WebRTC* (browser certs last ~weeks). It reads as "a custom WebRTC server," not
"KPS", so it fails a strict "not unusual" bar but not the "not KPS-identifiable"
bar.

- Fully fixing it needs either short rotating certs (which break the stable
  certhash/address) or **encrypting the cert via DTLS 1.3**.
- **DTLS 1.3-only is not viable now:** `pion/dtls v3.1.2` marks 1.3 as
  work-in-progress ("Only DTLS 1.2 is currently supported"), and browser
  DTLS-1.3-for-WebRTC is only mid-rollout (2025–2026). A 1.3-only server would
  reject real clients.
- **Plan:** ship the version-independent fixes (§3) now; adopt **opportunistic
  1.3** (prefer, fall back to 1.2) once pion's 1.3 is production-ready; revisit
  *requiring* 1.3 — which would close this residual — when both pion and browser
  support are solid. (Trigger to monitor.)

## 5. Implementation checklist (where these land)

- **Milestone 2 (WebRTC API):** ICE-pwd-from-certhash; shorter ufrag; cert CN
  blanked + random serial.
- **Milestone 3 (QUIC transport):** non-identifying ALPN/SNI; version
  negotiation out of ALPN.
- **Future:** opportunistic DTLS 1.3 (§4).
