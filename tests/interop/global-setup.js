import { spawn } from 'node:child_process'
import { createServer } from 'node:http'
import { existsSync } from 'node:fs'
import { mkdtemp, readFile, writeFile, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { dirname, extname, join, resolve } from 'node:path'
import { fileURLToPath } from 'node:url'

const here = dirname(fileURLToPath(import.meta.url))
const repoRoot = resolve(here, '../..')
const clientDir = join(repoRoot, 'libs/js')
// After the package split the browser client is built per-package; the page uses
// an import map to resolve the bare specifiers to these dist trees (see
// page/index.html). @kpstreams/core is dependency-free; webrtc-client imports it.
const coreDist = join(clientDir, 'packages/core/dist')
const webrtcClientDist = join(clientDir, 'packages/webrtc-client/dist')
const serverDir = join(repoRoot, 'libs/go')
const serverBin = join(serverDir, 'server')
const rustDir = join(repoRoot, 'libs/rust')
const rustServerBin = join(rustDir, 'target/debug/kps-server')
const jsServerScript = join(here, 'kps-js-server.mjs')
const pageDir = join(here, 'page')
const stateFilePath = join(here, '.run-state.json')

const MIME = {
  '.html': 'text/html; charset=utf-8',
  '.js':   'application/javascript; charset=utf-8',
  '.mjs':  'application/javascript; charset=utf-8',
  '.css':  'text/css; charset=utf-8',
  '.json': 'application/json; charset=utf-8',
  '.map':  'application/json'
}

function run(cmd, args, opts) {
  return new Promise((res, rej) => {
    const p = spawn(cmd, args, { stdio: 'inherit', ...opts })
    p.on('exit', code => code === 0 ? res() : rej(new Error(`${cmd} ${args.join(' ')} exited ${code}`)))
    p.on('error', rej)
  })
}

async function buildClient() {
  if (!existsSync(join(clientDir, 'node_modules'))) {
    console.log('[setup] installing JS workspace dependencies...')
    await run('npm', ['install', '--no-audit', '--no-fund'], { cwd: clientDir })
  }
  console.log('[setup] npm run build (all @kpstreams packages)...')
  await run('npm', ['run', 'build'], { cwd: clientDir })
}

async function buildServer() {
  console.log('[setup] go build ./cmd/server...')
  await run('go', ['build', '-o', 'server', './cmd/server'], { cwd: serverDir })
}

async function buildRustServer() {
  console.log('[setup] cargo build -p kps-server...')
  await run('cargo', ['build', '-p', 'kps-server'], { cwd: rustDir })
}

// Route bare-specifier package roots to their dist trees; everything else is
// served from the page dir. Keeps the browser ESM import map (page/index.html)
// resolvable without a bundler.
const STATIC_ROUTES = [
  ['/kps/core/', coreDist],
  ['/kps/webrtc-client/', webrtcClientDist],
]

function startStaticServer() {
  const server = createServer(async (req, res) => {
    try {
      const url = new URL(req.url, 'http://x')
      let p = decodeURIComponent(url.pathname)
      if (p.includes('..')) { res.writeHead(400); return res.end('bad') }

      let filePath
      const route = STATIC_ROUTES.find(([prefix]) => p.startsWith(prefix))
      if (route) {
        filePath = join(route[1], p.slice(route[0].length))
      } else {
        if (p === '/' || p === '') p = '/index.html'
        filePath = join(pageDir, p)
      }

      const data = await readFile(filePath)
      res.writeHead(200, { 'content-type': MIME[extname(filePath).toLowerCase()] ?? 'application/octet-stream' })
      res.end(data)
    } catch {
      res.writeHead(404); res.end('not found')
    }
  })
  return new Promise((res, rej) => {
    server.once('error', rej)
    server.listen(0, '127.0.0.1', () => res(server))
  })
}

// Spawn a child that prints a "127.0.0.1:<port>:<certhash>" line, then resolve
// with { address, child, cleanup }. Used for both the Go server binary and the
// JS server script.
function startAddressServer(label, cmd, args, opts = {}) {
  const child = spawn(cmd, args, { stdio: ['ignore', 'pipe', 'pipe'], ...opts })
  child.stderr.on('data', chunk => process.stderr.write(`[${label}] ${chunk}`))
  return new Promise((resolve, reject) => {
    let buf = ''
    let done = false
    const timer = setTimeout(() => {
      if (done) return
      done = true
      try { child.kill() } catch {}
      reject(new Error(`${label}: timed out waiting for address`))
    }, 20_000)
    const onData = chunk => {
      buf += chunk.toString()
      process.stdout.write(`[${label}] ${chunk}`)
      const m = buf.match(/127\.0\.0\.1:\d+:[A-Za-z0-9_-]+/)
      if (m && !done) {
        done = true
        clearTimeout(timer)
        child.stdout.off('data', onData)
        child.stdout.on('data', c => process.stdout.write(`[${label}] ${c}`))
        resolve({ address: m[0], child })
      }
    }
    child.stdout.on('data', onData)
    child.on('exit', code => {
      if (done) return
      done = true
      clearTimeout(timer)
      reject(new Error(`${label} exited (${code}) before printing address`))
    })
  })
}

async function stopChild(child) {
  if (!child || child.killed) return
  child.kill('SIGTERM')
  await new Promise(res => {
    const t = setTimeout(() => { try { child.kill('SIGKILL') } catch {} ; res() }, 3_000)
    child.on('exit', () => { clearTimeout(t); res() })
  })
}

export default async function globalSetup() {
  await buildClient()
  await buildServer()
  await buildRustServer()

  const stateDir = await mkdtemp(join(tmpdir(), 'kps-it-'))
  const go = await startAddressServer('go-server', serverBin,
    ['-listen', '127.0.0.1:0', '-key', join(stateDir, 'kps.key'), '-ip', '127.0.0.1'],
    { cwd: serverDir })
  const js = await startAddressServer('js-server', process.execPath, [jsServerScript])
  const rust = await startAddressServer('rust-server', rustServerBin,
    ['-listen', '127.0.0.1:0', '-key', join(stateDir, 'kps-rust.key'), '-ip', '127.0.0.1'],
    { cwd: rustDir })

  const httpServer = await startStaticServer()
  const port = httpServer.address().port
  const baseUrl = `http://127.0.0.1:${port}`
  await writeFile(stateFilePath, JSON.stringify(
    { goAddress: go.address, jsAddress: js.address, rustAddress: rust.address, baseUrl }, null, 2))
  console.log(`[setup] go server:   ${go.address}`)
  console.log(`[setup] js server:   ${js.address}`)
  console.log(`[setup] rust server: ${rust.address}`)
  console.log(`[setup] static site: ${baseUrl}`)

  return async () => {
    await new Promise(res => httpServer.close(() => res()))
    await stopChild(go.child)
    await stopChild(js.child)
    await stopChild(rust.child)
    try { await rm(stateDir, { recursive: true, force: true }) } catch {}
    try { await rm(stateFilePath, { force: true }) } catch {}
  }
}
