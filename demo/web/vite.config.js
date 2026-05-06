import { copyFileSync, readdirSync, statSync } from 'node:fs'
import { join } from 'node:path'
import { defineConfig } from 'vite'

/**
 * `vite build`                  → site build, dist/
 * `vite build --mode extension` → MV3 extension build, dist-extension/
 */
export default defineConfig(({ mode }) => {
  const isExtension = mode === 'extension'
  return {
    base: './',
    build: {
      outDir: isExtension ? 'dist-extension' : 'dist',
      target: 'es2022',
      sourcemap: true
    },
    plugins: isExtension ? [copyExtensionFilesPlugin()] : []
  }
})

function copyExtensionFilesPlugin() {
  return {
    name: 'copy-extension-files',
    closeBundle() {
      const src = 'extension'
      const dest = 'dist-extension'
      for (const entry of readdirSync(src)) {
        const from = join(src, entry)
        const to = join(dest, entry)
        if (statSync(from).isFile()) copyFileSync(from, to)
      }
    }
  }
}
