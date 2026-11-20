// Standalone mock API: `npm run mock` (PORT=8787). Also serves the built UI
// from dist/ if present, so `npm run build && npm run mock` is a full demo.
import { createReadStream, existsSync, statSync } from 'node:fs'
import { createServer } from 'node:http'
import { extname, join, normalize, resolve } from 'node:path'
import { createMock } from './mock.ts'

const port = Number(process.env.PORT ?? 8787)
const dist = resolve(import.meta.dirname, '..', 'dist')
const types: Record<string, string> = {
  '.html': 'text/html; charset=utf-8',
  '.js': 'text/javascript',
  '.css': 'text/css',
  '.svg': 'image/svg+xml',
  '.json': 'application/json',
}

const mock = createMock()

createServer((req, res) => {
  if (mock.handle(req, res)) return
  const url = new URL(req.url ?? '/', 'http://x')
  let file = normalize(join(dist, decodeURIComponent(url.pathname)))
  if (!file.startsWith(dist) || !existsSync(file) || statSync(file).isDirectory()) file = join(dist, 'index.html')
  if (!existsSync(file)) {
    res.writeHead(404, { 'Content-Type': 'text/plain' })
    res.end('Mock API is running. No dist/ yet: run `npm run build`, or use `npm run dev`.\n')
    return
  }
  res.writeHead(200, { 'Content-Type': types[extname(file)] ?? 'application/octet-stream' })
  createReadStream(file).pipe(res)
}).listen(port, () => {
  console.log(`beacon mock API on http://localhost:${port}  (${JSON.stringify(mock.stats())})`)
})
