# KPS Redesign Roadmap (draft)

Staged plan to take KPS from "WebRTC, named message streams" to the target in
[`SPEC.md`](../SPEC.md) / [`architecture.md`](architecture.md): transport-neutral,
connection-first, unnamed byte streams, two transports, clean monorepo.

Milestones are ordered so each one leaves the repo green (builds + tests pass)
and so the riskiest new capability (QUIC) lands only after the abstraction is
proven on the transport that already works.

---

## Milestone 0 — Plan + spec *(this PR, no code)* — COMPLETE

- [x] `SPEC.md`, `docs/architecture.md`, `docs/roadmap.md`.
- [x] `SECURITY.md` — censorship-resistance model + mechanisms.
- [x] Open decisions resolved (below).

---

## Milestone 1 — Monorepo restructure *(pure relocation, behaviour unchanged)* — COMPLETE

**Goal:** move to the target tree with import paths fixed and all current tests
still passing. **No API or behaviour change.** This is deliberately boring and
reviewable as a diff that is almost entirely moves + path edits.

Done: directories moved via `git mv` (history preserved); module →
`github.com/voltrevo/kps/libs/go` with the `kps` package at the module root;
`go.work` + demo `replace` added; all import/config/CI/README paths fixed.
Verified green: `go build` (workspace), `libs/js` tsc build, web demo vite
build, and the Playwright echo test (browser ↔ Go, ~490 ms).

Moves (via `git mv` to preserve history):

| From                     | To                          |
|--------------------------|-----------------------------|
| `client/`                | `libs/js/`                  |
| `server/`                | `libs/go/`                  |
| `server/kps/`            | `libs/go/kps/` (or root — see decision) |
| `server/cmd/`            | `libs/go/cmd/`              |
| `demo/`                  | `demos/chat/` (split)       |
| `tests/`                 | `tests/interop/`            |

Path/config edits:

- `libs/go/go.mod`: rename module per the resolved Go-module decision; update
  all imports.
- `demos/*/go.mod`: update `require` + `replace` paths to `../../libs/go`.
- `libs/js/package.json`: keep `@kps/client` (rename to `kps`/`@kps/client` is a
  separate decision); `demos/*/web/package.json` update the `file:` dep path.
- `tests/interop/`: fix the Playwright config paths and the `@kps/client` /
  served-page references.
- `.github/workflows/pages.yml`: update build paths.
- `README.md`: update the layout section and quick-taste paths.

**Exit criteria:** `go build ./...` in `libs/go`, `npm run build` in `libs/js`,
the demos build, and the Playwright echo test passes — all against relocated
paths, with zero semantic changes.

---

## Milestone 2 — Connection-first API, unnamed byte streams (WebRTC) — CORE COMPLETE

Realise SPEC §6.2 on the existing WebRTC transport. This is the core abstraction
change.

Done: Go + JS byte `Stream` with the DATA/FIN/RESET/STOP_SENDING framing and
`CloseWrite`/`CancelRead`/`ResetWrite`; `Conn`/`Listener.Accept`/`AcceptStream`
(Go) and `openStream`/`acceptStream`/`close`/`datagrams` (JS, WHATWG
`readable`/`writable`); the one-shot `kps.openStream(addr)`; the censorship
fixes (ICE pwd from certhash, shorter ufrag, blanked cert CN + random serial);
and both demos migrated to a one-line protocol selector over the byte stream.
Verified green: Go build/vet, JS tsc, web vite build, and the Playwright echo
test (browser ↔ Go over unnamed byte streams; `CloseWrite`→EOF exercised).
Remaining nicety: dedicated interop assertions for `cancelRead`/`resetWrite`
(deferred to the M4 interop matrix).

- Go: add `Conn` interface + `Listener.Accept` / `Conn.AcceptStream`; drop
  `Handle(name,…)`. Replace message `Send`/`Recv` with byte `Read`/`Write` +
  the §6.2 DATA/FIN/RESET/STOP_SENDING frame layer; implement `CloseWrite`,
  `CancelRead`, `ResetWrite`. `Stream` implements `io.Reader/Writer/Closer`.
- JS: `openStream()` (no name); byte `read`/`write`; §6.2 framing; `closeWrite`,
  `cancelRead`, `resetWrite`; async iteration sugar; `kps.openStream(addr)`
  one-shot.
- Generated non-semantic data-channel labels; bootstrap channel stays internal.
- Define the datagram API surface (capability check returns unsupported for now).
- **Censorship-resistance fixes (`SECURITY.md` §3):** `ice-pwd` derived from the
  certhash (not `ufrag == pwd`); ufrag shrunk to a normal-length random demux
  key; cert Subject CN blanked + serial randomized.
- Rewrite demos (echo/chat/rpc-proxy) to put their framing in the stream bytes.
- Update the Playwright test to the new byte API.

**Exit criteria:** browser ↔ Go listener works end-to-end with unnamed byte
streams; closeWrite→EOF, resetWrite→error, cancelRead→stop all observable.

---

## Milestone 3 — QUIC native transport + shared-port demux — COMPLETE

Add the second transport (SPEC §5.1, §5.3, §6.3).

Done: `Conn`/`Stream` promoted to interfaces (WebRTC impls renamed
`webrtcConn`/`webrtcStream`); `quic-go` added; native `kps.Dial` over QUIC with
certhash pinning (`VerifyPeerCertificate`) and non-identifying `h3` ALPN;
`quicConn`/`quicStream` mapping (`CloseWrite`→FIN, `CancelRead`→STOP_SENDING,
`ResetWrite`→RESET_STREAM); and the shared-port demux — pump routes WebRTC
(known addr / STUN) to pion and everything else to a `quic.Transport` over a
virtual PacketConn on the same UDP socket. Verified: `go test` QUIC echo +
certhash-mismatch rejection (sandbox-disabled, as quic-go's client socket needs
DF/GSO syscalls), and the Playwright WebRTC echo against the QUIC-enabled
listener — i.e. both transports on one listener/port.

Note: a one-shot `kps.OpenStream(ctx, addr)` (Go) and a transport override are
not yet added (native default is QUIC; there is no Go WebRTC client). The
"both transports simultaneously in one process" assertion is folded into M4.

- Add `quic-go`. Server: demux STUN vs QUIC long-header on the one UDP socket;
  feed QUIC datagrams to a `quic.Transport`; accept `quic.Connection`s as
  `Conn`s and `quic.Stream`s as `Stream`s.
- **Non-identifying ALPN/SNI (`SECURITY.md` §3):** advertise `h3` or no ALPN and
  an empty/innocuous SNI — never `kps/1` in cleartext; version negotiation lives
  in the address or first app bytes.
- Native Go QUIC client dial; certhash pinning via `VerifyPeerCertificate`.
- Default transport selection: native→QUIC, with an override for tests.
- Add `demos/echo/client-go` (QUIC).

**Exit criteria:** Go QUIC client ↔ Go listener works; a WebRTC browser client
and a Go QUIC client both connect to the **same** listener UDP port.

---

## Milestone 4 — Interop test matrix — COMPLETE

Executable version of SPEC §10: browser-WebRTC↔Go, Go-QUIC↔Go, both-on-one-port,
multi-conn, multi-stream, closeWrite/cancelRead/resetWrite. Wire into CI.

Done:
- **Go WebRTC client** (`DialWebRTC`) — the spec's "explicit transport override"
  (SPEC §5.4); mirrors the browser dial (synthesized answer, certhash-derived ICE
  pwd, pion pins the server cert via the answer fingerprint). Lets the WebRTC
  transport be tested programmatically, no browser required.
- **Interop matrix** (`libs/go/*_test.go`): §10.2 QUIC echo, §10.4 multi-conn,
  §10.5 multi-stream, §10.6 closeWrite→EOF, §10.7 cancelRead→peer-stop,
  §10.8 resetWrite→peer-error — each over **both** transports where applicable;
  §10.3 **both transports on one UDP port simultaneously** in a single process;
  certhash-mismatch rejection; and framing wire-format unit tests (the bytes the
  Go and JS impls must agree on).
- §10.1 browser-WebRTC↔Go stays covered by the Playwright test.
- **CI** (`.github/workflows/ci.yml`): Go build/vet/test + JS build + Playwright.
  Note: QUIC client tests need real UDP socket access (DF/GSO/ECN socket options),
  so they must not run under a restrictive seccomp sandbox — CI runners are fine.
- §10.9 datagrams remain unimplemented (capability reports unsupported) → M5.

---

## Milestone 5 — Datagrams (REQUIRED) + docs polish — CORE COMPLETE

Datagrams are **mandatory**, not optional (decided with the maintainer: both v0
transports carry them natively and the listener controls both ends, so there is
no "unsupported" state — only a size limit; a reliable-only future transport
would be the only reason to gate, and QUIC already replaced that idea).

Done:
- Mandatory datagram API: `SupportsDatagrams()` and any queryable size property
  removed; `SendDatagram`/`ReceiveDatagram` (Go) and `conn.datagrams.{send,incoming}`
  (JS) always live. The per-connection size limit is transport/path-dependent, so
  it is surfaced via the oversized-send error (`*DatagramTooLargeError{MaxDatagramPayloadSize}`
  / `{code:'too-large'}`), mirroring QUIC; ~1100 bytes is safe on any connection.
- QUIC: `EnableDatagrams` on both ends → QUIC DATAGRAM (RFC 9221).
- WebRTC: reserved negotiated unreliable/unordered channel (ID 1) on the Go
  server, Go client, and JS browser client; bounded inbound buffer (drop-oldest).
- Interop tests: datagram round-trip over QUIC and WebRTC, and oversize→too-large.
- SPEC §7/§8/§10.9 rewritten from "optional/capability-gated" to "required".

Remaining polish (optional): the per-topic doc split (`address-format.md`,
`stream-semantics.md`, `datagrams.md`) — currently all covered inline in
`SPEC.md`, so these would be pointers, deferred as low-value.

---

## Decisions (resolved)

1. **Go module path** → `module github.com/voltrevo/kps/libs/go`, public `kps`
   package **at the module root** → import `github.com/voltrevo/kps/libs/go`,
   used as `kps.Dial`. Removes the doubled `…/kps/kps` tail; resolves via
   `go get`.
2. **JS package name** → keep `@kps/client`.
3. **Milestone 1 scope** → **pure move.** M1 relocates files, fixes the import
   paths the move forces (incl. the module-path rename in #1), and keeps every
   *exported API symbol and signature unchanged* (`Handle`, `Send`/`Recv`,
   `openStream(name)`, `Stream.Name()` all survive M1 untouched). All
   semantic/API changes are deferred to M2. Exit proof = existing tests pass
   unchanged.
4. **Demos module wiring** → `go.work` workspace across `libs/go` + demos **plus**
   a local `replace github.com/voltrevo/kps/libs/go => ../../../libs/go` in the
   demo `go.mod`. (Under Go 1.24 the workspace `use` directive alone did not
   satisfy the demo's `require …/libs/go v0.0.0` — Go tried to fetch it from
   VCS — so the `replace` is required; it also lets the demo build standalone.)
5. **`PROTOCOL.md` split** → keep byte-level detail in `SPEC.md` for now; no
   separate `PROTOCOL.md` yet. *(default)*
