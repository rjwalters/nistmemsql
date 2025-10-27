import type { WasmModule, Database } from './types'

let wasmModule: WasmModule | null = null
let db: Database | null = null

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
    // Dynamic import for code splitting
    wasmModule = await import('../../public/pkg/nistmemsql_wasm')

    // Null check for TypeScript strict mode
    if (!wasmModule) {
      throw new Error('Failed to import WASM module')
    }

    // Wait for WASM to initialize
    await wasmModule.default()

    // Create database instance
    db = new wasmModule.Database()

    console.log('✅ Database initialized successfully')
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
