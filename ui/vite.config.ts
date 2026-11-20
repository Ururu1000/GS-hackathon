import react from '@vitejs/plugin-react'
import { defineConfig, type Plugin } from 'vite'
import { createMock } from './mock/mock.ts'

// `npm run dev` serves the in-process mock API (ui/mock) unless BEACON_API_URL
// points at a real backend, e.g. BEACON_API_URL=http://localhost:8080.
const backend = process.env.BEACON_API_URL

function mockApi(): Plugin {
  return {
    name: 'beacon-mock-api',
    apply: 'serve',
    configureServer(server) {
      const mock = createMock()
      server.httpServer?.on('close', () => mock.close())
      server.middlewares.use((req, res, next) => {
        if (!mock.handle(req, res)) next()
      })
    },
  }
}

export default defineConfig({
  // Relative asset URLs: the build can be served from any path by the backend,
  // compose (nginx) or `npm run mock`.
  base: './',
  plugins: [react(), ...(backend ? [] : [mockApi()])],
  server: {
    port: 5173,
    proxy: backend
      ? {
          '/api': { target: backend, changeOrigin: true },
          '/ingest': { target: backend, changeOrigin: true },
        }
      : undefined,
  },
  build: { outDir: 'dist', sourcemap: true },
})
