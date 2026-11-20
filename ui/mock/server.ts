// Standalone mock API: `npm run mock` (PORT=8787). Also serves the built UI
// from dist/ if present, so `npm run build && npm run mock` is a full demo.
import { createReadStream, existsSync, statSync } from 'node:fs'
import { createServer } from 'node:http'
import { extname, join, normalize, resolve, sep } from 'node:path'
import { createMock } from './mock.ts'

const host = process.env.HOST ?? '127.0.0.1'
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
  let decoded: string
  try {
    decoded = decodeURIComponent(url.pathname)
  } catch {
    res.writeHead(400, { 'Content-Type': 'text/plain' })
    res.end('Bad Request\n')
    return
  }
  let file = normalize(join(dist, decoded))
  const inDist = file === dist || file.startsWith(dist + sep)
  if (!inDist || !existsSync(file) || statSync(file).isDirectory()) file = join(dist, 'index.html')
  if (!existsSync(file)) {
    res.writeHead(404, { 'Content-Type': 'text/plain' })
    res.end('Mock API is running. No dist/ yet: run `npm run build`, or use `npm run dev`.\n')
    return
  }
  res.writeHead(200, { 'Content-Type': types[extname(file)] ?? 'application/octet-stream' })
  createReadStream(file).pipe(res)
}).listen(port, host, () => {
  console.log(`beacon mock API on http://${host}:${port}  (${JSON.stringify(mock.stats())})`)
})
