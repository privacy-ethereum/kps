import { test, expect } from '@playwright/test'
import { readFileSync } from 'node:fs'
import { dirname, join } from 'node:path'
import { fileURLToPath } from 'node:url'

const here = dirname(fileURLToPath(import.meta.url))
const { goAddress, jsAddress, baseUrl } = JSON.parse(readFileSync(join(here, '.run-state.json'), 'utf8'))

// The real-browser WebRTC leg of the matrix: a Chromium page runs the JS
// webrtc-client (the actual package, no polyfill) against BOTH server
// implementations over the single advertised address. Headless node:test
// covers QUIC and the Go-client cross-impl paths (see libs/js/test/integration).
for (const [name, address] of [['Go server', goAddress], ['@kpstreams/server', jsAddress]]) {
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
}
