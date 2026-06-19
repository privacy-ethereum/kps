# KPS Protocol Specification (draft v0)

> **Status:** draft for review. This document defines the wire-level and
> behavioural contract that every KPS implementation (Go, TypeScript, future
> Rust/Swift/Kotlin) must satisfy. The language libraries are *implementations
> of this spec*, not the spec itself. Where this document and a library
> disagree, this document is the intended source of truth and the library is a
> bug.

KPS = **Key-Pinned Stream**. A KPS endpoint is identified by a pinned
self-signed certificate, not by a CA-signed domain name. KPS provides an
authenticated, encrypted, multiplexed connection carrying unnamed reliable
bidirectional byte streams, plus optional connection-level datagrams.

---

## 1. Terminology

- **Identity** — a server's persistent self-signed X.509 certificate and its
  matching private key. The certificate's public-key/leaf bytes are what a
  client pins.
- **Certhash** — a multibase-encoded multihash of the certificate, used to pin
  the identity out-of-band (§3).
- **Address** — a UDP endpoint plus a certhash (§2).
- **Connection** — one authenticated secure session between a client and a
  pinned server identity.
- **Stream** — an unnamed bidirectional reliable ordered byte stream inside a
  connection (§6). No message boundaries, no names, no transport IDs in the
  public API.
- **Datagram** — an optional, unreliable, unordered, size-limited,
  connection-level message (§7).
- **Transport** — the concrete wire protocol carrying a connection. v0 defines
  two: **WebRTC** (browser-compatible) and **QUIC** (native). The public API
  hides which transport a connection uses.

---

## 2. Address format

```
<ip>:<udp-port>:<certhash>
```

- `ip` — IPv4 dotted quad (v0 is IPv4-only; IPv6 is a future extension).
- `udp-port` — decimal 1–65535.
- `certhash` — see §3.

The address identifies **a UDP endpoint and a pinned server identity, nothing
else.** It MUST NOT encode stream names, application protocols, or transport
selection. The same address is dialable by both transports; the dialer chooses
the transport (§5.4).

Future address formats (multiaddr-style, multiple certhashes, IPv6, DNS names)
are out of scope for v0 and explicitly deferred.

---

## 3. Certificate / key pinning

- The certhash is `multibase('u') || multihash`, where:
  - multibase prefix `u` = base64url, no padding.
  - multihash = `0x12 0x20 || sha256(cert)` — sha2-256 (code `0x12`), length 32
    (`0x20`), followed by the 32-byte digest.
- The digest is taken over the certificate's DER bytes — **the same bytes a
  WebRTC DTLS `a=fingerprint:sha-256` covers.** This is the load-bearing
  invariant that lets one identity certificate serve both transports (§5).

Pinning rules:

- A client MUST reject a connection whose presented certificate does not hash
  to the pinned certhash, regardless of transport.
- A client MUST NOT perform CA / hostname validation. Trust derives solely from
  the certhash matching.
- The KPS identity is a single certificate. Both the WebRTC DTLS handshake and
  the QUIC TLS 1.3 handshake present that same certificate, so a single
  certhash pins both transports.
- The certificate carries no identifying metadata: implementations MUST use an
  empty/random Subject CommonName and a random serial number, so that a
  certificate observed on the wire is not recognizable as KPS. (Rationale and
  the residual validity-period concern: see `SECURITY.md`.)

This v0 retains the libp2p webrtc-direct certhash encoding for ecosystem
familiarity.

---

## 4. Connection model

- A connection is an authenticated secure session to one pinned identity.
- A connection carries any number of concurrent, independent **streams**.
- A connection MAY carry **datagrams** if both ends support them (§7).
- Multiple independent connections to the same address from the same device MUST
  be supported and MUST be fully independent (separate streams, separate close
  lifetimes).
- Either side MAY open streams. Stream-open and stream-accept are symmetric
  primitives, not client-only.

---

## 5. Transports and demultiplexing

A KPS listener MUST accept both transports on the **same UDP port** behind the
same address.

### 5.1 Packet demux (single UDP socket)

For each inbound UDP datagram:

1. If it originates from the source address of an already-established
   connection, route it to that connection's transport.
2. Else if it is a STUN message (RFC 5389 magic cookie `0x2112A442` at bytes
   4–7, leading two bits zero) → **WebRTC path**.
3. Else if it is a QUIC long-header packet (high bit `0x80` set, plausible
   version field) → **QUIC path**.
4. Else → **drop**.

### 5.2 WebRTC transport

KPS-over-WebRTC descends from libp2p webrtc-direct:

- Server is ICE-lite. No signaling server. The client builds a real local
  offer, synthesizes the server's "answer" SDP from the address (the server's
  fingerprint comes from the certhash), and the server learns the connection's
  ICE ufrag from the first inbound STUN binding's `USERNAME`.
- **ICE credentials (KPS rule, diverges from libp2p webrtc-direct).** The
  `ice-ufrag` is a random connection-demux key with ~64 bits of entropy (normal
  WebRTC length; it does NOT double as the password). The `ice-pwd` is derived
  from the certhash both sides already share:

  ```
  ice-pwd = base64-std-nopad( HMAC-SHA256( key = certhash_digest(32 bytes),
                                           "kps-ice-pwd-v1:" || ice-ufrag ) )
  ```

  Both sides compute the identical pwd; it is only ever an HMAC key and is never
  transmitted. This removes the recomputable-`MESSAGE-INTEGRITY` fingerprint of
  the old `ufrag == pwd` convention and makes the cleartext ICE exchange look
  like ordinary browser WebRTC. It also gates DTLS behind certhash possession
  (probe resistance). See `SECURITY.md`.
- DTLS secures the channel; the client verifies the server's DTLS certificate
  hashes to the pinned certhash. The server does not pin the client.
- SCTP carries data channels. Each KPS **stream** is one SCTP data channel
  (§6.2).

### 5.3 QUIC transport

- One KPS connection = one QUIC connection. The ALPN MUST NOT be a
  KPS-identifying token in cleartext: implementations use a non-identifying
  ALPN (e.g. `h3`) or none, and an empty/innocuous SNI, so a passive observer
  cannot keyword-match KPS on the Initial. This is safe because the demux (§5.1)
  is structural, not ALPN-based, and KPS owns the port. KPS version negotiation
  lives in the address or the first application bytes, not in the ALPN.
- The server presents the KPS identity certificate. The client uses TLS 1.3
  with certificate verification disabled at the PKI level and instead verifies
  the presented leaf certificate hashes to the pinned certhash.
- Each KPS **stream** is one QUIC bidirectional stream (§6.3). QUIC's native
  stream semantics map directly onto KPS stream semantics with no extra framing.
- QUIC DATAGRAM frames carry KPS datagrams when enabled (§7).

### 5.4 Default transport selection

- Browser/JS clients: WebRTC (only option in-browser).
- Native clients (Go, Rust, CLI, daemons, mobile): QUIC by default.
- Implementations MAY expose an explicit transport override for tests/debugging.
  The override is not part of the address.

---

## 6. Stream semantics

A stream is an **unnamed, bidirectional, reliable, ordered byte stream with no
message boundaries.** It models the useful subset of QUIC bidirectional streams.

### 6.1 Operations

- **read bytes** — returns available bytes; EOF after the peer's write half
  finishes gracefully.
- **write bytes** — with backpressure (a write blocks / signals not-ready when
  the send buffer is full).
- **closeWrite / CloseWrite** — gracefully finish the local write half. The peer
  observes EOF on its read half *after* all previously written bytes are
  delivered.
- **cancelRead / CancelRead(reason)** — the local application no longer wants
  inbound bytes. Where the transport supports it, signal the peer to stop
  sending. This is *cancellation*, not graceful EOF.
- **resetWrite / ResetWrite(reason)** — abort the local write half. The peer
  observes a *stream error* (not EOF). Previously buffered bytes MAY or MAY NOT
  be delivered.
- **close** — shorthand for tearing down the whole stream (both halves).

There is deliberately **no `closeRead`** as the primary receive-side operation;
receive-side termination is cancellation, expressed by `cancelRead`.

The public API MUST NOT expose: stream IDs, connection IDs, transport
parameters, 0-RTT, migration, version negotiation, unidirectional streams (v0),
or fine-grained flow-control knobs.

### 6.2 Stream mapping over WebRTC (internal framing)

SCTP data channels are reliable, ordered, and *message-oriented*, and offer no
native half-close. To present a byte stream with QUIC-like lifecycle, KPS frames
each data-channel message. **This framing is internal to KPS and invisible to
applications** — applications see only a byte stream.

Each data-channel message is exactly one frame:

```
+--------+------------------------------+
| type=1 | payload (variable)           |
+--------+------------------------------+
```

| type | name         | payload            | meaning                                              |
|------|--------------|--------------------|------------------------------------------------------|
| 0x00 | DATA         | stream bytes       | append to the receiver's read buffer                 |
| 0x01 | FIN          | (empty)            | sender's write half finished → EOF after prior DATA  |
| 0x02 | RESET        | uint32 error code  | sender aborted write half → receiver sees stream err |
| 0x03 | STOP_SENDING | uint32 error code  | receiver cancelled read → sender should stop + reset |

- Error codes are big-endian `uint32`. `0` means "no specific reason".
- Ordering: because the channel is reliable+ordered, `FIN`/`RESET` after `DATA`
  arrive in order, giving clean EOF/error semantics.
- A `DATA` payload MAY be empty (no-op). Empty stream writes need not produce a
  frame.
- One SCTP data channel carries one stream in both directions. The data-channel
  **label is non-semantic** and MUST be ignored by receivers; implementations
  MAY use a generated/debug label.
- Reserved channels (negotiated, fixed IDs) are implementation details (§8) and
  MUST NOT surface as application streams.

Backpressure maps to SCTP `bufferedAmount` / `bufferedAmountLowThreshold`.

### 6.3 Stream mapping over QUIC

Direct mapping, no extra framing:

| KPS            | QUIC                                   |
|----------------|----------------------------------------|
| open stream    | open bidirectional stream              |
| accept stream  | accept bidirectional stream            |
| write / read   | stream write / read                    |
| closeWrite     | close send side (FIN)                  |
| cancelRead     | `STOP_SENDING` (CancelRead with code)  |
| resetWrite     | `RESET_STREAM` (CancelWrite with code) |
| backpressure   | QUIC stream flow control               |

### 6.4 Interop requirement

A WebRTC client and a QUIC client talking to the same listener MUST observe
identical stream semantics (EOF via closeWrite, error via resetWrite,
stop-sending via cancelRead). The §6.2 framing exists precisely to make the
WebRTC mapping behave like the QUIC mapping.

---

## 7. Datagrams (optional, capability-gated)

KPS does **not** offer "unreliable streams." If unreliable delivery is offered,
it is modelled as connection-level datagrams:

- unreliable, unordered, message-oriented, size-limited
- encrypted/authenticated under the connection
- independent of streams
- optional — gated behind a capability check

API shape (illustrative). `datagrams` is always present; the capability is
gated by `maxSize` (0 ⇒ unsupported) and a `send` that rejects with
`"unsupported"`. Inbound datagrams arrive unsolicited, so they are delivered
through a bounded buffer (e.g. drop-oldest when full), not a single racing
receive:

```ts
conn.datagrams.maxSize                 // 0 ⇒ unsupported
await conn.datagrams.send(bytes)       // rejects "too-large" / "unsupported"
conn.datagrams.incoming                // ReadableStream<Uint8Array> (bounded)
```
```go
ok  := conn.SupportsDatagrams()
err := conn.SendDatagram(p)
p, err := conn.ReceiveDatagram(ctx)
```

Transport mappings:

- **QUIC** — QUIC DATAGRAM frames (when negotiated).
- **WebRTC** — a single reserved unreliable, unordered data channel
  (`ordered:false`, `maxRetransmits:0`, negotiated, fixed ID). It MUST NOT
  surface as an application stream.

An implementation MAY report datagrams unsupported in v0. The abstraction MUST
exist so datagrams can be added later without an API break.

---

## 8. Reserved transport internals

These are implementation details, not part of the public API, and MUST NOT
surface as application streams or be relied upon by applications:

- WebRTC bootstrap data channel — negotiated, fixed ID `0`, used only to force
  the SCTP association up. Never delivered as a stream.
- WebRTC datagram channel (§7) — negotiated, fixed ID `1` when datagrams are
  enabled.
- Any data-channel label.

---

## 9. Error, reset and close behaviour (summary)

| Event                         | Read half of peer        | Write half of peer            |
|-------------------------------|--------------------------|-------------------------------|
| local `closeWrite`            | EOF after buffered bytes | unaffected                    |
| local `resetWrite(code)`      | stream error (with code) | unaffected                    |
| local `cancelRead(code)`      | unaffected               | writes fail; should reset     |
| connection close              | all streams error/EOF    | all streams error             |
| certhash mismatch (dial time) | dial fails; no connection| —                             |

### 9.1 Error-code registry

The reset/cancel/close `reason.code` is one of the following canonical names.
Each maps to a wire `uint32` carried in the §6.2 `RESET`/`STOP_SENDING` frames
(WebRTC) and in QUIC `RESET_STREAM` / `STOP_SENDING` / `CONNECTION_CLOSE`
application error codes (QUIC). Implementations MUST use these values so JS and
Go agree; an unknown received code maps to `internal-error`.

| code (string)       | wire `uint32` | meaning                                              |
|---------------------|---------------|------------------------------------------------------|
| (none)              | `0`           | no specific reason                                   |
| `cancelled`         | `1`           | operation cancelled by the local application         |
| `closed`            | `2`           | normal close / graceful teardown                     |
| `reset`             | `3`           | write half aborted (`resetWrite`)                    |
| `timeout`           | `4`           | deadline/idle timeout                                |
| `network-error`     | `5`           | transport/connectivity failure                       |
| `protocol-error`    | `6`           | malformed or out-of-contract peer behaviour          |
| `unsupported`       | `7`           | capability not supported (e.g. datagrams)            |
| `too-large`         | `8`           | payload exceeds a limit (e.g. datagram `maxSize`)    |
| `queue-full`        | `9`           | bounded inbound queue full; item rejected            |
| `permission-denied` | `10`          | refused by policy                                    |
| `internal-error`    | `11`          | unspecified local failure; also the unknown-code sink |

---

## 10. Interop requirements

Conforming implementations MUST interoperate across these scenarios:

1. Browser JS WebRTC client ↔ Go KPS listener.
2. Go native QUIC client ↔ Go KPS listener.
3. A WebRTC client and a QUIC client on the **same listener UDP port**.
4. Multiple concurrent independent connections from one device.
5. Multiple streams per connection.
6. Stream EOF via `closeWrite`.
7. Read cancellation via `cancelRead`.
8. Write reset via `resetWrite`.
9. Datagram capability — only if implemented; otherwise the capability check
   reports unsupported on both ends.

See `tests/interop/` for the executable matrix.
