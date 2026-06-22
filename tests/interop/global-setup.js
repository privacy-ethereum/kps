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
const clientDist = join(clientDir, 'dist')
const serverDir = join(repoRoot, 'libs/go')
const serverBin = join(serverDir, 'server')
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
    console.log('[setup] installing client dependencies...')
    await run('npm', ['install', '--no-audit', '--no-fund'], { cwd: clientDir })
  }
  console.log('[setup] tsc ./client...')
  await run('npx', ['tsc'], { cwd: clientDir })
}

async function buildServer() {
  console.log('[setup] go build ./cmd/server...')
  await run('go', ['build', '-o', 'server', './cmd/server'], { cwd: serverDir })
}

function startStaticServer() {
  const server = createServer(async (req, res) => {
    try {
      const url = new URL(req.url, 'http://x')
      let p = decodeURIComponent(url.pathname)
      if (p.includes('..')) { res.writeHead(400); return res.end('bad') }

      let filePath
      if (p.startsWith('/kps-client/')) {
        filePath = join(clientDist, p.slice('/kps-client/'.length))
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

async function startKpsServer() {
  const stateDir = await mkdtemp(join(tmpdir(), 'kps-it-'))
  const keyFile = join(stateDir, 'kps.key')
  const child = spawn(serverBin, ['-listen', '127.0.0.1:0', '-key', keyFile, '-ip', '127.0.0.1'], {
    cwd: serverDir,
    stdio: ['ignore', 'pipe', 'pipe']
  })
  child.stderr.on('data', chunk => process.stderr.write(`[server] ${chunk}`))

  return await new Promise((resolve, reject) => {
    let buf = ''
    let done = false
    const timer = setTimeout(() => {
      if (done) return
      done = true
      try { child.kill() } catch {}
      reject(new Error('timed out waiting for kps server address'))
    }, 15_000)

    const onData = chunk => {
      const s = chunk.toString()
      buf += s
      process.stdout.write(`[server] ${s}`)
      const m = buf.match(/127\.0\.0\.1:\d+:[A-Za-z0-9_-]+/)
      if (m && !done) {
        done = true
        clearTimeout(timer)
        child.stdout.off('data', onData)
        child.stdout.on('data', c => process.stdout.write(`[server] ${c}`))
        resolve({ child, address: m[0], stateDir })
      }
    }
    child.stdout.on('data', onData)
    child.on('exit', code => {
      if (done) return
      done = true
      clearTimeout(timer)
      reject(new Error(`server exited (${code}) before printing address`))
    })
  })
}

export default async function globalSetup() {
  await buildClient()
  await buildServer()
  const { child: serverProc, address, stateDir } = await startKpsServer()
  const httpServer = await startStaticServer()
  const port = httpServer.address().port
  await writeFile(stateFilePath, JSON.stringify({ address, baseUrl: `http://127.0.0.1:${port}` }, null, 2))
  console.log(`[setup] kps address: ${address}`)
  console.log(`[setup] static site: http://127.0.0.1:${port}`)

  return async () => {
    await new Promise(res => httpServer.close(() => res()))
    if (!serverProc.killed) {
      serverProc.kill('SIGTERM')
      await new Promise(res => {
        const t = setTimeout(() => { try { serverProc.kill('SIGKILL') } catch {} ; res() }, 3_000)
        serverProc.on('exit', () => { clearTimeout(t); res() })
      })
    }
    try { await rm(stateDir, { recursive: true, force: true }) } catch {}
    try { await rm(stateFilePath, { force: true }) } catch {}
  }
}
