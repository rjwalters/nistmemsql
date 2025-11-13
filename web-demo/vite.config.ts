import { defineConfig } from 'vite'
import wasm from 'vite-plugin-wasm'
import monacoEditorPlugin from 'vite-plugin-monaco-editor'
import { resolve } from 'path'
import { copyFileSync, mkdirSync, existsSync } from 'fs'

export default defineConfig({
  plugins: [
    wasm(),
    // @ts-ignore - Plugin types may not match exactly
    monacoEditorPlugin.default({
      languageWorkers: ['editorWorkerService', 'typescript', 'json'],
      customWorkers: [],
    }),
    {
      name: 'copy-benchmark-data',
      closeBundle() {
        // Copy benchmark data to dist after build
        const benchmarkSrc = resolve(__dirname, '../benchmarks/benchmark_results.json')
        const benchmarkDest = resolve(__dirname, 'dist/benchmarks/benchmark_results.json')

        try {
          // Create benchmarks directory if it doesn't exist
          const benchmarksDir = resolve(__dirname, 'dist/benchmarks')
          if (!existsSync(benchmarksDir)) {
            mkdirSync(benchmarksDir, { recursive: true })
          }

          // Copy the file if source exists
          if (existsSync(benchmarkSrc)) {
            copyFileSync(benchmarkSrc, benchmarkDest)
            console.log('✓ Copied benchmark_results.json to dist/benchmarks/')
          } else {
            console.warn('⚠️  benchmark_results.json not found at:', benchmarkSrc)
          }
        } catch (err) {
          console.error('Failed to copy benchmark data:', err)
        }
      },
    },
  ],
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
