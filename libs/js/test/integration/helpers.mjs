// Shared fixtures for the KPS JS integration tests (node:test). These exercise
// the built packages/*/dist, so `npm run build` must run first (the root
// `test:integration` script does). Everything binds 127.0.0.1 on OS-assigned
// ports; the cross-impl fixtures shell out to the Go lib (server + cmd/dial),
// which needs a real UDP stack (no restrictive seccomp) and the `go` toolchain.

import dgram from 'node:dgram'
import { spawn, execFileSync } from 'node:child_process'
import { mkdtempSync } from 'node:fs'
import { mkdtemp, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { dirname, join, resolve } from 'node:path'
import { fileURLToPath } from 'node:url'

const here = dirname(fileURLToPath(import.meta.url))
export const libsJs = resolve(here, '../..')
export const repoRoot = resolve(libsJs, '../..')
export const libsGo = join(repoRoot, 'libs', 'go')

// ── Server ────────────────────────────────────────────────────────────────

// Grab an OS-assigned free UDP port on loopback. The public demux port can't be
// bound with :0 through listen() today (the Listener reports the requested
// port), and node-datachannel's mux needs a concrete port up front, so tests
// pre-pick one. The window between close and re-bind is tiny on loopback.
export function freeUdpPort() {
  return new Promise((res, rej) => {
    const s = dgram.createSocket('udp4')
    s.once('error', rej)
    s.bind(0, '127.0.0.1', () => {
      const port = s.address().port
      s.close(() => res(port))
    })
  })
}

// Start an in-process @kpstreams/server that echoes every stream: it copies the
// peer's bytes straight back, then finishes its write half (mirrors the Go
// echoHandler). Returns { srv, address, close }.
export async function startJsServer({ transports } = {}) {
  const { listen } = await import('@kpstreams/server')
  const port = await freeUdpPort()
  // Fresh temp identity per server (avoids reusing/persisting kps-*.pem in cwd).
  const idDir = mkdtempSync(join(tmpdir(), 'kps-it-id-'))
  const srv = await listen({
    port, address: '127.0.0.1', transports,
    certPath: join(idDir, 'cert.pem'), keyPath: join(idDir, 'key.pem'),
  })
  const address = srv.address('127.0.0.1')
  let running = true

  ;(async () => {
    while (running) {
      let conn
      try { conn = await srv.accept() } catch { return }
      handleConn(conn)
    }
  })()

  return {
    srv,
    address,
    async close() { running = false; await srv.close().catch(() => {}) },
  }
}

function handleConn(conn) {
  // Echo streams.
  ;(async () => {
    for (;;) {
      let stream
      try { stream = await conn.acceptStream() } catch { return }
      echoStream(stream)
    }
  })()
  // Echo datagrams (best-effort; SPEC §7).
  ;(async () => {
    const reader = conn.datagrams.incoming.getReader()
    try {
      for (;;) {
        const { value, done } = await reader.read()
        if (done) break
        if (value) await conn.datagrams.send(value).catch(() => {})
      }
    } catch { /* connection closed */ }
  })()
}

async function echoStream(stream) {
  const reader = stream.readable.getReader()
  const writer = stream.writable.getWriter()
  try {
    for (;;) {
      const { value, done } = await reader.read()
      if (done) break
      if (value && value.length) await writer.write(value)
    }
    await writer.close()
  } catch { /* peer went away; drop the stream */ }
}

// ── Client-side stream helpers ──────────────────────────────────────────────

// Open a stream, write `bytes`, finish the write half (peer sees EOF), and read
// the echo back to EOF. Returns the echoed bytes as a Buffer. Reading runs
// CONCURRENTLY with writing: an echo peer writes bytes back as it receives them,
// so a large payload would deadlock if we wrote everything before draining
// (both sides block on a full flow-control window). Drain from the start.
export async function echoRoundTrip(conn, bytes) {
  const stream = await conn.openStream()
  const readPromise = readAll(stream.readable)
  const writer = stream.writable.getWriter()
  await writer.write(bytes)
  await writer.close()
  return await readPromise
}

export async function readAll(readable) {
  const reader = readable.getReader()
  const chunks = []
  for (;;) {
    const { value, done } = await reader.read()
    if (done) break
    if (value && value.length) chunks.push(Buffer.from(value))
  }
  return Buffer.concat(chunks)
}

// ── Go interop (server binary + dial client) ────────────────────────────────

let goChecked
export function goAvailable() {
  if (goChecked !== undefined) return goChecked
  try { execFileSync('go', ['version'], { stdio: 'ignore' }); goChecked = true }
  catch { goChecked = false }
  return goChecked
}

let binDir
function goBin(name, pkg) {
  if (!binDir) binDir = mkdtempSync(join(tmpdir(), 'kps-it-bin-'))
  const out = join(binDir, name)
  // Build once per process; execFileSync throws (fails the test) on error.
  execFileSync('go', ['build', '-o', out, pkg], { cwd: libsGo, stdio: 'pipe' })
  return out
}

const built = new Map()
function buildOnce(name, pkg) {
  if (!built.has(name)) built.set(name, goBin(name, pkg))
  return built.get(name)
}

// Spawn the Go kps server (echoes streams) on a loopback OS-assigned port and
// resolve once it prints its dial address. Returns { address, kill }.
export async function spawnGoServer() {
  const bin = buildOnce('server', './cmd/server')
  const stateDir = await mkdtemp(join(tmpdir(), 'kps-it-go-'))
  const child = spawn(bin, ['-listen', '127.0.0.1:0', '-ip', '127.0.0.1', '-key', join(stateDir, 'kps.key')],
    { cwd: libsGo, stdio: ['ignore', 'pipe', 'pipe'] })
  child.stderr.on('data', () => {}) // swallow quic buffer-size warnings

  const address = await new Promise((resolve, reject) => {
    let buf = ''
    const timer = setTimeout(() => { try { child.kill() } catch {} ; reject(new Error('go server: timed out printing address')) }, 15_000)
    child.stdout.on('data', (chunk) => {
      buf += chunk
      const m = buf.match(/127\.0\.0\.1:\d+:[A-Za-z0-9_-]+/)
      if (m) { clearTimeout(timer); resolve(m[0]) }
    })
    child.on('exit', (code) => { clearTimeout(timer); reject(new Error(`go server exited (${code}) before printing address`)) })
  })

  return {
    address,
    async kill() {
      if (!child.killed) child.kill('SIGTERM')
      await rm(stateDir, { recursive: true, force: true }).catch(() => {})
    },
  }
}

function spawnGoClientOnce({ addr, transport, message, timeoutMs }) {
  const bin = buildOnce('dial', './cmd/dial')
  return new Promise((resolve) => {
    const child = spawn(bin, [
      '-addr', addr, '-transport', transport, '-message', message,
      '-timeout', `${Math.ceil(timeoutMs / 1000)}s`,
    ], { cwd: libsGo, stdio: ['ignore', 'pipe', 'pipe'] })
    let out = '', err = ''
    child.stdout.on('data', (d) => { out += d })
    child.stderr.on('data', (d) => { err += d })
    child.on('exit', (code) => resolve({ code, out, err }))
    child.on('error', (e) => resolve({ code: -1, out, err: String(e) }))
  })
}

// Run the Go dial client against `addr` over `transport` ('quic' | 'webrtc'),
// sending `message` and verifying the echo. Resolves { code, out, err }.
//
// A failed *connection* is retried (default: up to 3 attempts). WebRTC ICE
// establishment is best-effort and can occasionally miss its window under CI
// load; a retry re-dials cleanly. An *echo mismatch* is NOT retried — that would
// be a real correctness bug — so retries stop as soon as the client reports one.
export async function spawnGoClient({ addr, transport = 'quic', message = 'hello-kps', timeoutMs = 15_000, attempts = 3 }) {
  let last
  for (let i = 0; i < attempts; i++) {
    last = await spawnGoClientOnce({ addr, transport, message, timeoutMs })
    if (last.code === 0 || /echo mismatch/.test(last.err)) return last
  }
  return last
}
