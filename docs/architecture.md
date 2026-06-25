# KPS Architecture (draft)

> Implementation companion to [`SPEC.md`](../SPEC.md). The spec defines the wire
> contract; this document defines the public API surface, the repository layout,
> and how each language library realises the spec.

---

## 1. Public API surface

The same conceptual API in every language. Connection-first; streams are
unnamed; transport is hidden.

### TypeScript (browser-first; WebRTC only in-browser)

This surface is the one consumed by downstream wrappers such as the anon-rpc
worker capability API (`KpsApi`/`KpsConn`/`KpsStream`), so it deliberately uses
WHATWG streams and pull-based accept to be a thin, adapter-free fit.

```ts
const conn   = await kps.dial(addr, { signal })   // transport hidden; signal-first
const stream = await conn.openStream({ signal })  // no name

// byte I/O via WHATWG streams — no message boundaries, backpressure built in
stream.readable   // ReadableStream<Uint8Array>
stream.writable   // WritableStream<Uint8Array>

await stream.closeWrite()                 // graceful local write-half EOF
await stream.cancelRead(reason?)          // stop wanting inbound bytes (not EOF)
await stream.resetWrite(reason?)          // abort local write half (peer sees error)
await stream.close(reason?)               // tear down both halves
stream.closed                             // Promise<KpsStreamCloseInfo>

// inbound streams: PULL, symmetric with Go's AcceptStream (not an onstream callback)
const inbound = await conn.acceptStream({ signal })
await conn.close(reason?)
conn.closed                               // Promise<KpsConnCloseInfo>

// datagrams: always available (required). Size limit surfaces via the send
// error (code 'too-large', maxDatagramPayloadSize); ~1100 bytes is safe.
await conn.datagrams.send(bytes)
conn.datagrams.incoming                   // ReadableStream<Uint8Array> (bounded buffer)

// one-shot sugar over dial → openStream — the stream owns the hidden connection
const stream = await kps.openStream(addr)
```

`reason` is `KpsReason { code?: KpsErrorCode, message? }`; the `KpsErrorCode`
string set and its mapping to wire codes is the registry in `SPEC.md` §9.

### Go (native; QUIC by default, WebRTC available)

```go
conn, err := kps.Dial(ctx, addr)               // DialOption: WithTransport(...), etc.
stream, err := conn.OpenStream(ctx)

n, err := stream.Write(p)                        // backpressure via blocking
n, err := stream.Read(p)                         // io.EOF after peer CloseWrite

err := stream.CloseWrite()
err := stream.CancelRead(code)
err := stream.ResetWrite(code)
err := stream.Close()

// server
ln, err := kps.Listen(ctx, ":4242", kps.Options{ /* Identity | KeyFile */ })
for {
    conn, err := ln.Accept(ctx)
    go handleConn(conn)
}
func handleConn(conn kps.Conn) {
    for {
        stream, err := conn.AcceptStream(ctx)
        go handleStream(stream)
    }
}

// datagrams (always available; required). Oversized send → *DatagramTooLargeError
err := conn.SendDatagram(p)
p, err := conn.ReceiveDatagram(ctx)

// one-shot sugar
stream, err := kps.OpenStream(ctx, addr)
```

`Stream` SHOULD satisfy `io.Reader`, `io.Writer`, `io.Closer` in Go so it drops
into existing byte-stream code. In TS, `read()`/`write()` are the primitives and
async iteration is sugar.

**What is gone vs today:** `listener.Handle(name, ...)`, `conn.openStream(name)`,
`stream.Name()`, `Send`/`Recv` message framing, the `'message'` event as the
core read path. Demos move their protocol into the stream bytes.

---

## 2. Repository layout (target)

```
kps/
  README.md
  SPEC.md
  PROTOCOL.md          # optional: wire byte-level detail split out of SPEC
  SECURITY.md

  libs/
    js/                # was client/  — browser/client library (npm: key-pinned-streams)
      package.json
      src/
      test/
    go/                # was server/  — listener + native client + both transports
      go.mod
      kps/
      cmd/
      test/

  demos/
    chat/   { server-go/, web/ }     # was demo/, protocol moved into stream bytes
    echo/   { server-go/, client-web/, client-go/ }
    rpc-proxy/ { server-go/, web/ }

  tests/
    interop/ { fixtures/, matrix/, scripts/ }   # was tests/ (Playwright echo)

  docs/
    architecture.md
    address-format.md
    stream-semantics.md
    datagrams.md
```

### Go module path (decision needed — see roadmap §"Open decisions")

Today: `github.com/privacy-ethereum/kps/server`, imported as
`github.com/privacy-ethereum/kps/server/kps`. The doubled tail is what we want to fix.

A module not at the repo root must have a module path equal to its repo-relative
subdir, so a module at `libs/go` is `github.com/privacy-ethereum/kps/libs/go`.
Recommended option: put the public `kps` package **at the module root** so the
import path tail is `go`, not a doubled `kps`:

```
libs/go/go.mod         → module github.com/privacy-ethereum/kps/libs/go
libs/go/*.go           → package kps          // import ".../libs/go", used as kps.Dial
libs/go/cmd/...        → demo/CLI binaries
```

This trades the doubled `…/kps/kps` for a `…/libs/go` tail. Alternatives (root
go.mod for a clean `github.com/privacy-ethereum/kps`; or a vanity import path) are listed
as an open decision in the roadmap.

---

## 3. Go server internals (target)

The existing listener already demuxes WebRTC clients on one UDP port by ufrag
and virtualizes a `net.PacketConn` per PeerConnection. The redesign generalises
this into a transport-neutral demux plus a connection-accept queue.

```
UDP socket
  └── demux (per inbound datagram)
        ├── known src addr ───────────────► existing connection's transport
        ├── STUN ──────────────────────────► WebRTC path (spawn/route PeerConnection)
        ├── QUIC long-header ──────────────► QUIC path (feed quic.Transport's PacketConn)
        └── else ──────────────────────────► drop
```

- **WebRTC path** keeps today's ufrag/byAddr routing and per-PC inbox
  `net.PacketConn`. `OnDataChannel` now wraps each channel as a §6.2-framed
  `Stream` and enqueues it on the owning `Conn`'s accept queue (instead of
  dispatching to a name handler).
- **QUIC path** hands QUIC-destined datagrams to a virtual `net.PacketConn` read
  by a `quic.Transport`; each accepted `quic.Connection` becomes a `Conn`, each
  `quic.Stream` a `Stream` (no extra framing).
- **`Listener.Accept`** returns a `Conn` once either transport reports a new
  connection established. `Conn.AcceptStream` drains that connection's stream
  queue.
- A single `Conn` interface abstracts both transports; callers cannot tell which
  transport backs a connection.

The current `Identity`/certhash machinery is reused unchanged: the same
self-signed cert is fed to pion (DTLS) and to quic-go's `tls.Config`, and its
sha256(DER) is the certhash for both (SPEC §3).

---

## 4. TypeScript client internals (target)

- Stays WebRTC-only (browsers have no QUIC-over-UDP socket API).
- `openStream()` creates a data channel with a generated, non-semantic label and
  wraps it in the §6.2 framing layer to expose byte `read`/`write` plus
  `closeWrite`/`cancelRead`/`resetWrite`.
- The `Stream` becomes byte-oriented: an internal reassembly buffer over `DATA`
  frames; `read()` returns bytes, EOF on `FIN`, error on `RESET`.
- Bootstrap channel (negotiated id 0) stays as an internal SCTP-bring-up detail.
- The existing SDP/ufrag synthesis (`sdp.ts`) and certhash decode (`certhash.ts`)
  are unchanged.

---

## 5. Demos (target)

Demos consume **public library APIs only** and implement their own framing in
the stream bytes:

- **echo** — minimal: write bytes, read them back. Adds `client-go` (QUIC) and
  `client-web` (WebRTC) against one listener.
- **chat** — moves today's line-delimited JSON protocol (`hello`/`bulletin`/
  `dm`/`ack`/`roster`) into the stream bytes over a single stream. The framing
  (newline-delimited JSON) becomes the *application's* concern, not a KPS
  message boundary.
- **rpc-proxy** — same: newline-delimited JSON-RPC envelopes over one stream.

No demo references stream names, transport internals, or `Send`/`Recv` message
semantics.
