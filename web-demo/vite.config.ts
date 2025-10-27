import { defineConfig } from 'vite'
import wasm from 'vite-plugin-wasm'

export default defineConfig({
  plugins: [wasm()],
  base: '/nistmemsql/', // GitHub Pages path
  build: {
    target: 'esnext', // Modern browsers only
  }
})
