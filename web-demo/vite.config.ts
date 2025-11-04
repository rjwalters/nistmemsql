import { defineConfig } from 'vite'
import wasm from 'vite-plugin-wasm'
import { resolve } from 'path'

export default defineConfig({
  plugins: [wasm()],
  base: '/vibesql/', // GitHub Pages path
  build: {
    target: 'esnext', // Modern browsers only
    rollupOptions: {
      input: {
        main: resolve(__dirname, 'index.html'),
        conformance: resolve(__dirname, 'conformance.html'),
        benchmarks: resolve(__dirname, 'benchmarks.html'),
      },
    },
  },
  define: {
    __BUILD_TIMESTAMP__: JSON.stringify(new Date().toISOString()),
  },
})
