import { test, expect } from '@playwright/test'
import { readFileSync } from 'node:fs'
import { dirname, join } from 'node:path'
import { fileURLToPath } from 'node:url'

const here = dirname(fileURLToPath(import.meta.url))
const { goAddress, jsAddress, rustAddress, baseUrl } = JSON.parse(readFileSync(join(here, '.run-state.json'), 'utf8'))

// Sequential streams to cycle on one connection — well past the ~2 the browser
// leg currently manages before a reused SCTP id stalls (kps#4).
const STREAM_CYCLES = 20

// The real-browser WebRTC leg of the matrix: a Chromium page runs the JS
// webrtc-client (the actual package, no polyfill) against ALL server
// implementations over the single advertised address. Headless node:test
// covers QUIC and the Go/Rust-client cross-impl paths (see
// libs/js/test/integration).
for (const [name, address] of [['Go server', goAddress], ['@kpstreams/server', jsAddress], ['Rust server', rustAddress]]) {
  test(`browser webrtc-client dials ${name} and echoes a message`, async ({ page }) => {
    page.on('pageerror', err => console.error('[page error]', err))
    page.on('console', msg => {
      if (msg.type() === 'error') console.error('[page console]', msg.text())
    })

    await page.goto(baseUrl)
    const ok = await page.evaluate(addr => window.runEcho(addr), address)
    expect(ok).toBe(true)
    await expect(page.locator('#status')).toHaveText('done')
  })

  // Crosses the 1 MiB per-stream window, so it exercises real §6.5 credit
  // (MAX_STREAM_DATA / MAX_DATA) through an actual RTCPeerConnection — the one
  // path no headless test covers.
  test(`browser webrtc-client streams a large payload to ${name} (flow control)`, async ({ page }) => {
    page.on('pageerror', err => console.error('[page error]', err))
    page.on('console', msg => {
      if (msg.type() === 'error') console.error('[page console]', msg.text())
    })

    await page.goto(baseUrl)
    const ok = await page.evaluate(addr => window.runLargeEcho(addr), address)
    expect(ok).toBe(true)
    await expect(page.locator('#status')).toHaveText('done')
  })

  // Sustained cycling: many sequential streams on one connection. The browser
  // frees and reuses low SCTP stream ids as each channel closes, so this is the
  // only leg that exercises id reuse (Go/Rust clients allocate monotonically).
  test(`browser webrtc-client cycles ${STREAM_CYCLES} sequential streams to ${name}`, async ({ page }) => {
    page.on('pageerror', err => console.error('[page error]', err))
    page.on('console', msg => {
      if (msg.type() === 'error') console.error('[page console]', msg.text())
    })

    await page.goto(baseUrl)
    const completed = await page.evaluate(
      ({ addr, n }) => window.runManyStreams(addr, n),
      { addr: address, n: STREAM_CYCLES },
    )
    expect(completed).toBe(STREAM_CYCLES)
  })
}
