import { test, expect } from '@playwright/test'
import { readFileSync } from 'node:fs'
import { dirname, join } from 'node:path'
import { fileURLToPath } from 'node:url'

const here = dirname(fileURLToPath(import.meta.url))
const { address, baseUrl } = JSON.parse(readFileSync(join(here, '.run-state.json'), 'utf8'))

test('browser dials kps server and echoes a message', async ({ page }) => {
  page.on('pageerror', err => console.error('[page error]', err))
  page.on('console', msg => {
    if (msg.type() === 'error') console.error('[page console]', msg.text())
  })

  await page.goto(baseUrl)
  const ok = await page.evaluate(addr => window.runEcho(addr), address)
  expect(ok).toBe(true)
  await expect(page.locator('#status')).toHaveText('done')
})
