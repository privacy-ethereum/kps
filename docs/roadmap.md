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

## Milestone 2 — Connection-first API, unnamed byte streams (WebRTC)

Realise SPEC §6.2 on the existing WebRTC transport. This is the core abstraction
change.

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

## Milestone 3 — QUIC native transport + shared-port demux

Add the second transport (SPEC §5.1, §5.3, §6.3).

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

## Milestone 4 — Interop test matrix

Executable version of SPEC §10: browser-WebRTC↔Go, Go-QUIC↔Go, both-on-one-port,
multi-conn, multi-stream, closeWrite/cancelRead/resetWrite. Wire into CI.

---

## Milestone 5 — Datagrams (optional) + docs polish

Implement datagrams behind the capability check (QUIC DATAGRAM; WebRTC reserved
unreliable channel), or consciously defer. Fill in `SECURITY.md`,
`address-format.md`, `stream-semantics.md`, `datagrams.md`.

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
