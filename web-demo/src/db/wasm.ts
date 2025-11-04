import type { WasmModule, Database } from './types'

// In production, Vite copies public/ files to dist root, so public/pkg/ becomes /pkg/
// We need the full path with base for GitHub Pages deployment
const isProdBuild =
  typeof import.meta !== 'undefined' &&
  Boolean((import.meta as { env?: { PROD?: boolean } }).env?.PROD)
const WASM_MODULE_PATH = isProdBuild
  ? '/vibesql/pkg/vibesql_wasm.js'
  : '../../public/pkg/vibesql_wasm.js'

let wasmModule: WasmModule | null = null
let db: Database | null = null

async function loadWasmModule(): Promise<WasmModule> {
  if (wasmModule) return wasmModule

  try {
    const module = (await import(/* @vite-ignore */ WASM_MODULE_PATH)) as unknown as WasmModule
    wasmModule = module
    return module
  } catch (error) {
    if (isProdBuild) {
      const message = error instanceof Error ? error.message : String(error)
      throw new Error(
        `WASM bindings missing in production build: ${message}. Generate them with ` +
          '`wasm-pack build --target web --out-dir web-demo/public/pkg` prior to deploying.'
      )
    }

    console.warn(
      'WASM bindings not found; falling back to stub module. Run `wasm-pack build --target web --out-dir web-demo/public/pkg` to enable database features.'
    )
    const module = (await import('./wasm-fallback')) as unknown as WasmModule
    wasmModule = module
    return module
  }
}

/**
 * Initialize the WASM database module.
 * Must be called before any database operations.
 *
 * @returns Promise that resolves to Database instance
 * @throws Error if WASM fails to load
 */
export async function initDatabase(): Promise<Database> {
  if (db) return db

  try {
    const module = await loadWasmModule()
    await module.default()
    db = new module.Database()
    return db
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error)
    throw new Error(`Failed to load database: ${message}`)
  }
}

/**
 * Get the initialized database instance.
 *
 * @returns Database instance
 * @throws Error if database not initialized
 */
export function getDatabase(): Database {
  if (!db) {
    throw new Error('Database not initialized. Call initDatabase() first.')
  }
  return db
}

/**
 * Check if database is initialized
 *
 * @returns True if database is ready, false otherwise
 */
export function isDatabaseReady(): boolean {
  return db !== null
}
