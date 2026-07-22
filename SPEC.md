# KPS Protocol Specification (draft v0.2.1)

> **Status:** draft for review. This document defines the wire-level and
> behavioural contract that every KPS implementation (Go, TypeScript, Rust,
> future Swift/Kotlin) must satisfy. The language libraries are *implementations
> of this spec*, not the spec itself. Where this document and a library
> disagree, this document is the intended source of truth and the library is a
> bug. The document version tracks the library releases (this draft, 0.2.1,
> pairs with the 0.2.x packages); the WebRTC **wire version** (§8 HELLO,
> currently 1) independently counts incompatible wire revisions.

KPS = **Key Pinned Streams**. A KPS endpoint is identified by a pinned
self-signed certificate, not by a CA-signed domain name. KPS provides an
authenticated, encrypted, multiplexed connection carrying unnamed reliable
bidirectional byte streams, plus connection-level datagrams.

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
- **Datagram** — an unreliable, unordered, size-limited, connection-level
  message (§7). Always available in v0.2 (both transports provide them).
- **Transport** — the concrete wire protocol carrying a connection. v0.2 defines
  two: **WebRTC** (browser-compatible) and **QUIC** (native). The public API
  hides which transport a connection uses.

---

## 2. Address format

```
<ip>:<udp-port>:<certhash>          # IPv4
[<ipv6>]:<udp-port>:<certhash>      # IPv6 (host bracketed)
```

- `ip` — an IPv4 dotted quad, or an IPv6 literal in square brackets (the literal
  itself contains colons, so it is bracketed as in RFC 3986 / `net.JoinHostPort`).
- `udp-port` — decimal 1–65535.
- `certhash` — see §3.

The address identifies **a UDP endpoint and a pinned server identity, nothing
else.** It MUST NOT encode stream names, application protocols, or transport
selection. The same address is dialable by both transports; the dialer chooses
the transport (§5.4). A listener binds dual-stack (one UDP socket serves both
families), so one identity can be published at both a v4 and a v6 address —
same certhash, same port. (Caveat: WebRTC over IPv6 needs the listener bound to
a concrete v6 address for ICE candidate formation; QUIC works on the dual-stack
wildcard.)

Future address formats (multiaddr-style, multiple certhashes, DNS names) are out
of scope for v0.2 and explicitly deferred.

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

This v0.2 retains the libp2p webrtc-direct certhash encoding for ecosystem
familiarity.

---

## 4. Connection model

- A connection is an authenticated secure session to one pinned identity.
- A connection carries any number of concurrent, independent **streams**.
- A connection also carries **datagrams** (§7); in v0.2 they are always available
  (both transports provide them, and a listener controls both ends).
- Multiple independent connections to the same address from the same device MUST
  be supported and MUST be fully independent (separate streams, separate close
  lifetimes).
- Either side MAY open streams. Stream-open and stream-accept are symmetric
  primitives, not client-only.
- **Backpressure is end-to-end.** A peer that stops reading a stream MUST
  eventually block the other side's writes to it: the data in flight between a
  writer and a non-reading application is bounded. Local send-queue throttling
  is not sufficient. QUIC provides this natively (§6.3); the WebRTC mapping
  provides it with KPS-level credit (§6.5). The same discipline bounds
  KPS-managed unread data from conforming peers — streams, bytes per stream,
  aggregate bytes — and detects peers that exceed the advertised limits
  (§6.5).
- A connection exposes the peer's **remote UDP endpoint** (so acceptors can
  apply per-IP policy such as rate limits). It reflects the endpoint observed at
  connection establishment — the dialed endpoint on the dial side; the QUIC
  handshake source or the first STUN binding's source on the accept side — and
  MAY change over the connection's life (QUIC path migration, ICE renomination).

---

## 5. Transports and demultiplexing

A KPS listener MUST accept both transports on the **same UDP port** behind the
same address.

### 5.1 Packet demux (single UDP socket)

For each inbound UDP datagram:

1. If its source address belongs to an established **WebRTC** connection, route
   it there (covers post-handshake DTLS/SCTP).
2. Else if it is a STUN message (RFC 5389 magic cookie `0x2112A442` at bytes
   4–7, leading two bits zero) → **WebRTC path** (a new or in-progress ICE
   handshake).
3. Else → **QUIC path**: hand the datagram to the QUIC transport, which
   demultiplexes its own connections by connection ID and drops anything that
   is not a valid QUIC packet.

Equivalently: WebRTC is identified positively (STUN, or a known peer address);
everything else is QUIC. This avoids fragile long-/short-header sniffing for
established QUIC traffic.

### 5.2 WebRTC transport

KPS-over-WebRTC descends from libp2p webrtc-direct:

- Server is ICE-lite. No signaling server. The client builds a real local
  offer, synthesizes the server's "answer" SDP from the address (the server's
  fingerprint comes from the certhash), and the server learns the connection's
  ICE ufrag from the first inbound STUN binding's `USERNAME`.
- **ICE credentials (KPS rule, diverges from libp2p webrtc-direct).** The
  `ice-ufrag` is a random connection-demux key with at least 64 bits of entropy (normal
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
message boundaries.** It models a subset of QUIC bidirectional streams.

### 6.1 Operations

- **read bytes** — returns available bytes; **EOF if and only if the peer's
  write half finished (FIN)**. Any other terminal condition — peer `reset`, local
  `cancelRead`/`close`, or connection close/loss — makes a pending or subsequent
  read **fail with an error carrying a reason code, never EOF** (§9.2).
- **write bytes** — with end-to-end backpressure: a write blocks / signals
  not-ready when in-flight data reaches the peer's advertised receive window
  (§6.3, §6.5), not merely when a local send buffer is full.
- **closeWrite / CloseWrite** — gracefully finish the local write half. The peer
  observes EOF on its read half *after* all previously written bytes are
  delivered.
- **cancelRead / CancelRead(reason)** — the local application no longer wants
  inbound bytes: further inbound bytes are dropped locally and the peer is
  signalled to stop sending. This is *cancellation*, not graceful EOF.
- **resetWrite / ResetWrite(reason)** — abort the local write half. The peer
  observes a *stream error* (not EOF). Previously buffered bytes MAY or MAY NOT
  be delivered.
- **close** — tear down the whole stream (both halves). A close MAY carry an
  error code: with one, the peer observes a *stream error* (`RESET`) rather than
  EOF and is told to stop sending, and the code travels on the wire on both
  transports (§6.2 `RESET`/`STOP_SENDING`, QUIC `RESET_STREAM`/`STOP_SENDING`);
  with no code it is a clean teardown. (An implementation MAY expose the coded
  form as a separate call.)
- **closed** — observe termination: a completion signal plus the close reason
  (none for a clean close, else the error code). Best-effort — the reason may be
  lost if teardown races its delivery (§9).

There is deliberately **no `closeRead`** as the primary receive-side operation;
receive-side termination is cancellation, expressed by `cancelRead`.

The public API MUST NOT expose: stream IDs, connection IDs, transport
parameters, 0-RTT, migration, version negotiation, unidirectional streams (v0.2),
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

| type | name            | payload            | meaning                                              |
|------|-----------------|--------------------|------------------------------------------------------|
| 0x00 | DATA            | stream bytes       | append to the receiver's read buffer                 |
| 0x01 | FIN             | (empty)            | sender's write half finished → EOF after prior DATA  |
| 0x02 | RESET           | uint32 error code  | sender aborted write half → receiver sees stream err |
| 0x03 | STOP_SENDING    | uint32 error code  | receiver cancelled read → sender should stop + reset |
| 0x04 | MAX_STREAM_DATA | uint64 max offset  | flow-control credit for this stream's DATA (§6.5)    |

- Error codes are big-endian `uint32`. `0` means "no specific reason".
- Ordering: because the channel is reliable+ordered, `FIN`/`RESET` after `DATA`
  arrive in order, giving clean EOF/error semantics.
- **Frame size**: a frame (type byte + payload) MUST NOT exceed
  `MAX_WEBRTC_FRAME_SIZE = 16 384` bytes, so a DATA payload carries between 1
  and 16 383 bytes. Larger application writes MUST be split; empty application
  writes produce no frame. An empty or oversized DATA frame is a
  connection-level `protocol-error`. (The cap exists because an SCTP user
  message is received and reassembled *before* KPS can inspect its type or
  check credit — credit cannot protect a receiver from one arbitrarily large
  message — and it limits head-of-line blocking on SCTP stacks without message
  interleaving.)
- One SCTP data channel carries one stream in both directions. The data-channel
  **label is non-semantic** and MUST be ignored by receivers; implementations
  MAY use a generated/debug label.
- Within wire version 1 (§8), an unknown frame type, or a RESET /
  STOP_SENDING / MAX_STREAM_DATA frame with an incorrect payload length, is a
  protocol violation: close the connection with `protocol-error`. (Future
  frame types come with a version bump, not silent tolerance.)

**Write-half state machine** (normative; each direction of a stream
independently):

- DATA is valid only while the sender's write half is open. Exactly one
  terminal frame — FIN or RESET — ends a write half; DATA or a second terminal
  frame after FIN/RESET is a `protocol-error`.
- `closeWrite` preserves all DATA already accepted by the KPS write operation
  and queues FIN behind it.
- `resetWrite` discards queued-but-unsent DATA (releasing its credit
  reservations, §6.5) and queues RESET behind whatever DATA was already handed
  to the transport.
- On receiving STOP_SENDING: pending and future writes fail; if no terminal
  frame has been handed to the transport yet, the sender discards unsent DATA
  and replies with RESET; if the write half is already terminal, STOP_SENDING
  is ignored.
- MAX_STREAM_DATA received after the corresponding local send half is terminal
  MAY be ignored.
- Reserved channels (negotiated, fixed IDs) are implementation details (§8) and
  MUST NOT surface as application streams.

Write backpressure is governed by the §6.5 credit scheme, NOT by SCTP
`bufferedAmount` — `bufferedAmount` only measures data queued locally and says
nothing about whether the remote application is reading. Implementations SHOULD
still use `bufferedAmount` / `bufferedAmountLowThreshold` to bound their local
send queue, but it is a resource limit, not the flow-control mechanism.

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

QUIC's native limits (`MAX_STREAM_DATA`, `MAX_DATA`, `MAX_STREAMS`) provide the
same mechanisms the WebRTC mapping builds in §6.5. Implementations MUST ensure
their QUIC transport is configured so that receive-window advancement is tied
to application consumption and unread stream data and incoming stream counts
remain bounded, consistent with §6.5. The WebRTC and QUIC numerical limits need
not be identical; the observable semantics (§6.4) must be.

### 6.4 Interop requirement

A WebRTC client and a QUIC client talking to the same listener MUST observe
identical stream semantics (EOF via closeWrite, error via resetWrite,
stop-sending via cancelRead). The §6.2 framing exists precisely to make the
WebRTC mapping behave like the QUIC mapping.

### 6.5 End-to-end flow control (WebRTC mapping)

SCTP has only an association-wide receive window, which the stacks drain
eagerly regardless of application reads — and browsers expose no receive-side
pressure API at all. Data channels alone therefore give a slow application no
way to slow a fast sender. The WebRTC mapping closes this with QUIC-style
credit at the KPS layer: it bounds KPS-managed unread stream data from
conforming peers and detects peers that exceed the advertised limits.
(Transport stacks may buffer additional bounded in-flight data before KPS can
inspect it; the credit scheme cannot bound bytes a non-conforming peer puts
into the transport before KPS observes and kills the connection.) This is part
of **WebRTC wire version 1** (§8 HELLO).

Three limits, all expressed as **absolute (cumulative) values** — monotone,
idempotent, no wrap at 4 GiB:

| limit           | bounds                                              | granted via                                      | initial value (HELLO)  |
|-----------------|-----------------------------------------------------|--------------------------------------------------|------------------------|
| stream data     | DATA payload bytes on one stream                    | `MAX_STREAM_DATA` frame on that stream's channel | `initialMaxStreamData` |
| connection data | aggregate DATA payload bytes across all streams     | `MAX_DATA` control message (§8)                  | `initialMaxData`       |
| stream count    | cumulative application streams the peer may open    | `MAX_STREAMS` control message (§8)               | `initialMaxStreams`    |

Each limit governs one direction; the endpoint that *receives* grants credit
for what the peer may send/open toward it. Initial values are receiver-chosen,
announced in HELLO (§8), and MUST be honored as sent.

**Sender rules:**

- A sender distinguishes **sent** bytes (passed to the transport) from
  **reserved** bytes (admitted to its ordered send queue but not yet passed to
  the transport — they may still be discarded by STOP_SENDING, reset, or
  teardown). Before a DATA frame of `n` payload bytes enters the send queue
  for stream `s`, both must hold, atomically:

  ```text
  streamSent(s) + streamReserved(s) + n ≤ peerMaxStreamData(s)
  connSent      + connReserved      + n ≤ peerMaxData
  ```

  otherwise the write waits. The connection-level reservation MUST be atomic
  across concurrent stream writers (two streams must not both observe the same
  residual credit). When a queued frame is passed to the transport its bytes
  move from reserved to sent; when a queued frame is discarded unsent, its
  reservation is released.
- Credit MUST be reserved *before* the frame is queued — a credit-blocked DATA
  frame at the head of a send queue would trap lifecycle frames (FIN, RESET,
  STOP_SENDING, MAX_STREAM_DATA) behind it and deadlock.
- Stream opening uses the same reservation pattern: before creating a channel,
  atomically require `streamsOpened + streamsReserved < peerMaxStreams`
  (otherwise the open waits); a successful channel creation commits the
  reservation to the cumulative `streamsOpened`, a synchronous failure releases
  it. Once committed the count never decreases — capacity returns only via a
  larger `MAX_STREAMS`. The SCTP stack's own simultaneous-channel limit remains
  an additional local constraint.
- Only DATA **payload** bytes consume credit. FIN / RESET / STOP_SENDING /
  MAX_STREAM_DATA frames, control-channel messages, and datagrams are exempt.

**Receiver rules:**

- Inbound DATA is buffered in a bounded queue. Bytes are **consumed** only
  when (a) a read operation returns or copies them to the application, or
  (b) they are explicitly discarded by `cancelRead`, reset handling, or
  teardown. Handing bytes to an intermediate queue the application has not yet
  read (e.g. a WHATWG `ReadableStream`'s internal queue via
  `controller.enqueue`) is NOT consumption — counting it as such recreates the
  unbounded-buffer bug one queue over. Browser/JS implementations should use a
  pending-read-driven source (zero high-water mark) and count consumption when
  a read is fulfilled.
- As bytes are consumed, the receiver re-advertises credit:
  `MAX_STREAM_DATA(consumed + window)` and `MAX_DATA` likewise. A receiver MAY
  batch updates, but the consumed-yet-unadvertised amount MUST remain bounded
  by a threshold smaller than the receive window (no greater than half the
  window is RECOMMENDED) — this guarantees a reading receiver eventually
  resumes a blocked sender. The receiver's *enforcement* limit advances when
  an update is committed for sending, not when the peer acknowledges it:
  control and stream channels are separate SCTP streams, so DATA spending the
  new credit can legitimately arrive before the peer received the update
  message.
- On `cancelRead`: buffered bytes are discarded and count as consumed for
  connection-level accounting, as does any in-flight DATA that arrives before
  the peer processes STOP_SENDING. The receiver MUST NOT grant further
  `MAX_STREAM_DATA` on that stream (it wants no more), but SHOULD release the
  corresponding `MAX_DATA` so the cancelled stream does not starve unrelated
  streams. On receiving RESET, an implementation may either deliver
  already-buffered bytes before surfacing the error or discard them — its
  credit accounting MUST match whichever it does.
- Enforcement is by explicit counters — `streamReceived(s)` / `connReceived`
  against `localMaxStreamData(s)` / `localMaxData`. Before buffering `n`
  inbound payload bytes, both `streamReceived(s) + n ≤ localMaxStreamData(s)`
  and `connReceived + n ≤ localMaxData` must hold, checked **atomically**
  (DATA callbacks on different channels can run concurrently); the counters
  are incremented before the payload is queued or exposed.
- DATA beyond an advertised limit, or a stream opened beyond `MAX_STREAMS`, is
  a **flow-control violation**: close the connection with `protocol-error`
  (§8 CONNECTION_CLOSE) — the analogue of QUIC's `FLOW_CONTROL_ERROR` /
  `STREAM_LIMIT_ERROR`. Limits are monotone: a received update that would
  *decrease* a limit MUST be ignored.
- All KPS-owned queues, inbound and outbound, MUST be finite: peer credit is a
  remote flow-control ceiling, not permission to allocate an equally large
  local queue. Because credit values are absolute, queued-but-unsent credit
  updates SHOULD be coalesced to the latest value.
- A full DATA queue MUST NOT block lifecycle or credit traffic: FIN / RESET /
  STOP_SENDING, credit updates, and CONNECTION_CLOSE must remain sendable when
  DATA queues are full — reserve capacity for them, coalesce them, or
  represent them separately from queued DATA (each is at most one pending
  item per stream half, and credit updates coalesce to the latest value).

**Stream-count accounting** (per direction, in transport events — not
application `acceptStream` calls):

```text
peerOpenedStreams    cumulative peer-initiated application channels observed
peerRetiredStreams   those that have reached their terminal state
advertisedMaxStreams initialMaxStreams + peerRetiredStreams
```

A peer-opened stream consumes a slot from the moment it is observed —
including while it waits, unaccepted, in the incoming-stream queue (otherwise
an application that stops accepting accumulates an unbounded queue).

Retirement is staged, and drainage is part of it — otherwise a peer could
send one byte + FIN on each of many streams and reclaim its slots while the
unread bytes (cheap in connection credit) pin an unbounded number of stream
objects:

1. A stream is **wire-complete** when both write halves have ended with a
   terminal frame *on the wire*: the local half by sending FIN or RESET, the
   peer's half by its FIN or RESET being received. `cancelRead` alone does NOT
   complete the read half — the peer's terminal frame must still arrive (a
   conforming peer answers STOP_SENDING with RESET), because that frame fixes
   the stream's final DATA offset for credit accounting.
2. Once wire-complete, either endpoint MAY gracefully close the underlying
   data channel — legitimately even while the other endpoint still holds
   unread buffered data (drainage is not observable on the wire). An endpoint
   that is wire-complete *and* locally drained MUST initiate the close if the
   channel is still open, so retirement always makes progress.
3. The stream is **retired** — and replacement `MAX_STREAMS` credit granted —
   only when it is wire-complete, its channel has closed, *and* all inbound
   DATA has been consumed by the application or explicitly discarded locally.

This makes `initialMaxStreams` a bound on live, unaccepted, *or unread* stream
state, not merely on currently open SCTP channels.

**Teardown accounting:** stream frames are reliable and ordered, so both ends
agree on a stream's final DATA offset when it ends with an ordered terminal
frame (FIN or RESET) — no QUIC-style final-size signal is needed; bytes
buffered but never delivered count as consumed and release connection credit.
An application data channel that disappears while the connection is otherwise
healthy and the stream was **not** wire-complete leaves connection-level
credit ambiguous and MUST be treated as a connection failure
(`protocol-error`), not tolerated per-stream. (A wire-complete stream's
channel closing is the expected retirement step — and legitimate even if the
closer cannot know whether this endpoint has drained; when channels are dying
because the PeerConnection/SCTP association itself failed, the
connection-level network-error path applies instead.)

**Integers:** all offsets, limits and counts are unsigned with a ceiling of
`MAX_OFFSET = 2^62 − 1` (QUIC's integer range). An initial limit or update
above `MAX_OFFSET` is a protocol violation; a receiver's advertisements
saturate at `MAX_OFFSET`; offset additions are overflow-checked. Arithmetic
MUST be exact — JavaScript implementations MUST use `BigInt` (or equivalent),
never rounding counters through floating point.

**Recommended initial values:** `initialMaxStreamData` 1 MiB,
`initialMaxData` 8 MiB, `initialMaxStreams` 100. These trade throughput per
stream against worst-case receiver memory per connection; exact values are a
receiver policy, not a protocol constant. The steady-state bound on
KPS-managed unread data is approximately `initialMaxData` plus per-message
slack and the transport stack's own bounded in-flight buffering — not exactly
`initialMaxData`. Bounds *across* connections (per-IP connection limits, using
the connection's remote endpoint, §4) are application policy and out of scope
here.

The reserved channels (§8) are not application streams: they are exempt from
`MAX_STREAMS` and stream credit. Datagrams are bounded by their own drop-oldest
receive queue (§7).

---

## 7. Datagrams (required)

KPS does **not** offer "unreliable streams." Unreliable delivery is modelled as
connection-level datagrams, which **every KPS connection MUST provide**:

- unreliable, unordered, message-oriented, size-limited
- encrypted/authenticated under the connection
- independent of streams

Datagram support is a conformance requirement for KPS transports, like reliable
streams. Both v0.2 transports carry datagrams natively and a listener controls
both ends, so there is **no "unsupported" state** — only a size limit. (A future
reliable-only transport that could not carry datagrams would have to reintroduce
a capability gate; v0.2 has none.)

There is a per-connection datagram size limit, but it is transport- and
path-dependent, so KPS does **not** expose it as a fixed property. Instead an
oversized send fails with a **too-large error that reports the current limit**
(mirroring QUIC). As a rule of thumb, **payloads up to ~1100 bytes are safe on
every connection**; larger payloads may or may not fit. Delivery is best-effort:
a sent datagram may never arrive. Inbound datagrams arrive unsolicited, so they
are delivered through a bounded buffer (drop-oldest when full), not a single
racing receive.

API shape — flat `send`/`receive` methods on the connection, mirrored across
languages (`receive` is pull-based: one datagram per call, from the bounded
buffer):

```ts
// rejects with { code: 'too-large', maxDatagramPayloadSize } if over the limit
await conn.sendDatagram(bytes)
const p = await conn.receiveDatagram()  // next inbound datagram (bounded, drop-oldest)
```
```go
err := conn.SendDatagram(p)            // *DatagramTooLargeError{MaxDatagramPayloadSize} if over the limit
p, err := conn.ReceiveDatagram(ctx)
```

Transport mappings:

- **QUIC** — QUIC DATAGRAM frames (RFC 9221), enabled on both ends.
- **WebRTC** — a single reserved unreliable, unordered data channel
  (`ordered:false`, `maxRetransmits:0`, negotiated, fixed ID `1`). It MUST NOT
  surface as an application stream.

---

## 8. Reserved transport internals

These are implementation details, not part of the public API, and MUST NOT
surface as application streams or be relied upon by applications:

- WebRTC control channel — negotiated, reliable, ordered, fixed ID `0`. Both
  peers create it (the client before the offer, which also forces the SCTP
  association and its m-line up). Never delivered as a stream. Each message is
  a 1-byte type followed by a type-specific payload (integers big-endian):

  | type | name             | payload                                                                     | total size |
  |------|------------------|-----------------------------------------------------------------------------|------------|
  | 0x00 | CONNECTION_CLOSE | `uint32` application error code                                             | 5 bytes    |
  | 0x01 | HELLO            | `uint8` wire version, `uint64` initialMaxStreamData, `uint64` initialMaxData, `uint64` initialMaxStreams | 26 bytes |
  | 0x02 | MAX_DATA         | `uint64` absolute connection DATA-byte limit (§6.5)                         | 9 bytes    |
  | 0x03 | MAX_STREAMS      | `uint64` cumulative application-stream-open limit (§6.5)                    | 9 bytes    |

  Message sizes are exact. An unknown message type, a message whose length
  does not match its type, or a credit value above `MAX_OFFSET` (§6.5) is a
  protocol violation: close with `protocol-error`. Malformed input is never
  interpreted as a clean close.

  **HELLO** MUST be the first control message in normal operation, sent as
  soon as the channel opens, exactly once: a second HELLO, or a HELLO after
  any other message, is a protocol violation. The one exception is rejection —
  CONNECTION_CLOSE MAY be sent before HELLO solely to refuse or abort
  establishment, and a CONNECTION_CLOSE received before HELLO is a valid
  handshake rejection. The current wire version is `1`. A connection is not
  established — dial/accept MUST NOT complete — until the endpoint has both
  sent and received HELLO; a malformed HELLO is closed with `protocol-error`,
  an unsupported version with `unsupported`, both immediately. There is no
  downgrade. The pre-HELLO state MUST be bounded by a finite
  implementation-defined timeout. Sent eagerly (the channel is negotiated, so
  it is usable the moment SCTP establishes), HELLO shares a network flight
  with the tail of SCTP establishment in each direction and adds **no round
  trip** to connection setup or to a cold request.

  **Cross-channel ordering:** the control channel and application data
  channels are separate SCTP streams with no ordering between them, so a
  conforming peer's first application channel (or its DATA) can be *observed*
  before that peer's HELLO arrives. This is not a violation. An endpoint MUST
  NOT open application streams or send stream DATA until it has itself sent
  and received HELLO — but a receiver stages early-observed channels rather
  than rejecting them: it counts them against its own initial limits (which it
  knows, having chosen them), buffers their DATA against those windows, and
  surfaces nothing to the application until the HELLO exchange completes. There is no downgrade. (Consequently the
  control channel is always open on an established connection, so a close code
  never races channel setup.) Compatibility: pre-versioned implementations
  parsed every control message as a bare `uint32` close code; HELLO's leading
  bytes are nonzero, so an old peer reads it as a close and disconnects
  promptly instead of hanging. Wire version 1 is a breaking revision over
  those pre-versioned (0.1.x) implementations.

  **CONNECTION_CLOSE** is sent best-effort before teardown — the WebRTC
  analogue of QUIC `CONNECTION_CLOSE`. On receipt, the peer records the code as
  the connection's close reason and tears down.

  **MAX_DATA / MAX_STREAMS** carry connection-level flow-control credit
  (§6.5).
- WebRTC datagram channel (§7) — negotiated, fixed ID `1`, always present.
- **Loss of a reserved channel is fatal.** Without the control channel there
  is no flow control, no close signaling, and no versioning; without the
  datagram channel the connection can no longer provide the datagram service
  §7 requires. If either closes while the connection is otherwise healthy,
  fail the connection (`protocol-error`); if the PeerConnection/SCTP
  association itself failed, the connection-level network-error path applies
  instead.
- Any data-channel label.

---

## 9. Error, reset and close behaviour (summary)

| Event                         | Read half of peer        | Write half of peer            |
|-------------------------------|--------------------------|-------------------------------|
| local `closeWrite`            | EOF after buffered bytes | unaffected                    |
| local `resetWrite(code)`      | stream error (with code) | unaffected                    |
| local `cancelRead(code)`      | unaffected               | writes fail; should reset     |
| local `close(code)`           | stream error (with code) | writes fail; should reset     |
| connection close (with code)  | all streams error/EOF    | all streams error             |
| flow-control violation (§6.5) | connection closed `protocol-error` — all streams error ||
| certhash mismatch (dial time) | dial fails; no connection| —                             |

A connection close MAY carry an application error code, observable at the peer as
the connection's close reason: QUIC `CONNECTION_CLOSE` and the WebRTC control
channel's `CONNECTION_CLOSE` (§8) both convey it (best-effort — teardown may race
delivery, as with QUIC's single-packet close). A code of `0`/none is a clean
close.

### 9.1 Error-code registry

The reset/cancel/close `reason.code` is one of the following canonical names.
Each maps to a wire `uint32` carried, on WebRTC, in the §6.2 `RESET`/
`STOP_SENDING` frames and the control channel's `CONNECTION_CLOSE` (§8); and, on
QUIC, in `RESET_STREAM` / `STOP_SENDING` / `CONNECTION_CLOSE` application error
codes. All implementations MUST use these values so they agree on the wire; an
unknown received code maps to `internal-error`.

| code (string)       | wire `uint32` | meaning                                              |
|---------------------|---------------|------------------------------------------------------|
| (none)              | `0`           | no specific reason                                   |
| `cancelled`         | `1`           | operation cancelled by the local application         |
| `closed`            | `2`           | normal close / graceful teardown                     |
| `reset`             | `3`           | write half aborted (`resetWrite`)                    |
| `timeout`           | `4`           | deadline/idle timeout                                |
| `network-error`     | `5`           | transport/connectivity failure                       |
| `protocol-error`    | `6`           | malformed or out-of-contract peer behaviour          |
| `unsupported`       | `7`           | capability not supported                             |
| `too-large`         | `8`           | payload exceeds a limit (e.g. datagram `maxSize`)    |
| `queue-full`        | `9`           | bounded inbound queue full; item rejected            |
| `permission-denied` | `10`          | refused by policy                                    |
| `internal-error`    | `11`          | unspecified local failure; also the unknown-code sink |

### 9.2 Local read/write results

The §9 table above is **peer-observable** behaviour (what the remote endpoint
sees on the wire). The **local** result of a read or write when a stream
terminates is defined here, so that a holder of only a stream — not the
connection — can distinguish an unfinished stream from a completed one.

**A read yields EOF if and only if the peer's write half finished via FIN; every
other terminal condition yields an error** carrying a reason code:

| Local terminal condition        | local `read`                 | local `write`                |
|---------------------------------|------------------------------|------------------------------|
| peer FIN received               | **EOF** (after buffered bytes) | unaffected                 |
| peer RESET received             | **error** (peer's code)      | unaffected                   |
| peer STOP_SENDING received      | unaffected                   | **error** (peer's code)      |
| local `cancelRead(code)`        | **error** (`cancelled`/code) | unaffected                   |
| local `resetWrite(code)`        | unaffected                   | **error** (`reset`/code)     |
| local `close(code?)`            | **error** (`closed`/code)    | **error** (`closed`/code)    |
| connection closed locally       | **error** (`closed`)         | **error** (`closed`)         |
| connection lost                 | **error** (`network-error`)  | **error** (`network-error`)  |

The error's concrete representation is implementation- and transport-native
(e.g. Go `io.EOF` for the EOF row vs. a `*StreamError`/`ApplicationError`; Rust
`Ok(0)` vs. an `io::Error`; a rejected promise in JS) — this section constrains
only **EOF-vs-error** and the accompanying reason code, not the language-level
type. This matches native QUIC: quic-go's `CancelRead`/connection close surface a
`StreamError`/`ApplicationError`, and quinn's `stop`/connection loss surface
`ReadError::ClosedStream`/`ConnectionLost` (`io::ErrorKind::NotConnected`) —
never a spurious clean EOF.

**`closed`** (§6.1) reports `ok` for an orderly local teardown (a `close()` with
no code, or normal completion) and not-`ok` with the reason otherwise (peer
reset, coded close, connection failure). Bilateral FIN is **not** required for a
clean `close()` to report `ok`; it is the *read* operation that is unambiguously
an error when it cannot be fulfilled and no FIN was seen.

---

## 10. Interop requirements

Conforming implementations MUST interoperate across these scenarios:

1. A WebRTC client ↔ a KPS listener (including a browser-originated WebRTC client).
2. A QUIC client ↔ a KPS listener.
3. A WebRTC client and a QUIC client on the **same listener UDP port**.
4. Multiple concurrent independent connections from one device.
5. Multiple streams per connection.
6. Stream EOF via `closeWrite`.
7. Read cancellation via `cancelRead`.
8. Write reset via `resetWrite`.
9. Datagrams — send/receive over QUIC (DATAGRAM) and WebRTC (the reserved
   unreliable channel); oversize payloads rejected with `too-large`.
10. End-to-end backpressure — a receiver that stops reading a stream blocks the
    peer's writes once the advertised windows are exhausted; resuming reads
    resumes the writer; receiver memory stays bounded throughout. Both
    transports.
11. Stream-count limiting — opening streams beyond the advertised limit blocks
    a conforming opener until streams close; on WebRTC, a peer that violates
    a flow-control limit is closed with `protocol-error`.

See `tests/interop/` for the executable matrix.
