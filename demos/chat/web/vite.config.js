import { copyFileSync, readdirSync, statSync, readFileSync } from 'node:fs'
import { join, resolve } from 'node:path'
import { defineConfig } from 'vite'
import { Marked } from 'marked'
import markedShiki from 'marked-shiki'
import { createHighlighter } from 'shiki'

/**
 * `vite build`                  → site build (landing + spec), dist/
 * `vite build --mode extension` → MV3 extension build, dist-extension/
 */
export default defineConfig(({ mode }) => {
  const isExtension = mode === 'extension'
  return {
    base: './',
    build: {
      outDir: isExtension ? 'dist-extension' : 'dist',
      target: 'es2022',
      sourcemap: true,
      // Multi-page site: the landing page plus the hosted spec. The extension
      // build stays single-page (no spec).
      ...(isExtension
        ? {}
        : {
            rollupOptions: {
              input: {
                main: resolve(import.meta.dirname, 'index.html'),
                spec: resolve(import.meta.dirname, 'spec/index.html'),
              },
            },
          }),
    },
    plugins: isExtension ? [copyExtensionFilesPlugin()] : [specPage()],
  }
})

// The spec page hosts the repo's normative SPEC.md, rendered at build time (and
// live in `vite dev`) into the <!--SPEC_HTML--> slot of spec/index.html, so the
// hosted spec can never drift from the committed markdown. Code fences are
// syntax-highlighted with Shiki at build time — inline-styled spans, no runtime
// JS. Dracula's pink/purple/cyan sits naturally on the site's violet palette;
// its background is remapped to the site's code-block black.
let renderSpec
async function renderSpecMarkdown() {
  if (!renderSpec) {
    const highlighter = await createHighlighter({
      themes: ['dracula'],
      langs: ['typescript', 'go'],
    })
    const marked = new Marked(
      markedShiki({
        highlight: (code, lang) =>
          highlighter.codeToHtml(code, {
            lang: highlighter.getLoadedLanguages().includes(lang) ? lang : 'text',
            theme: 'dracula',
            colorReplacements: { '#282a36': '#08060d' },
          }),
      }),
    )
    renderSpec = (md) => marked.parse(md)
  }
  const md = readFileSync(resolve(import.meta.dirname, '../../../SPEC.md'), 'utf8')
  const html = await renderSpec(md)
  // Put the GitHub link on the title line: wrap the document's <h1> in a flex
  // row with the button, which flexbox centers at any viewport/font size.
  const gh =
    '<a class="btn secondary sm" href="https://github.com/privacy-ethereum/kps/blob/main/SPEC.md" target="_blank" rel="noopener">View on GitHub →</a>'
  return html.replace(
    /<h1([^>]*)>([\s\S]*?)<\/h1>/,
    (_m, attrs, inner) => `<div class="doc-head"><h1${attrs}>${inner}</h1>${gh}</div>`,
  )
}

function specPage() {
  return {
    name: 'inject-spec-markdown',
    transformIndexHtml: {
      order: 'pre',
      async handler(html, ctx) {
        if (!ctx.filename.endsWith(`spec${ctx.filename.includes('\\') ? '\\' : '/'}index.html`)) return
        const spec = await renderSpecMarkdown()
        // Replacement via callback: rendered code can contain `$`, which a
        // string replacement would interpret as a substitution pattern.
        return html.replace('<!--SPEC_HTML-->', () => spec)
      },
    },
  }
}

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
    },
  }
}
