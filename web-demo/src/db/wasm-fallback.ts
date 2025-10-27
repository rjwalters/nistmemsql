import type { Database as DatabaseInterface, QueryResult, TableSchema, WasmModule } from './types'

const MISSING_BINDINGS_MESSAGE =
  'WASM bindings are not available. Generate them with `wasm-pack build --target web --out-dir web-demo/public/pkg`.'

const fallbackModule: WasmModule = {
  default: async () => {
    console.warn(MISSING_BINDINGS_MESSAGE)
  },
  Database: class FallbackDatabase implements DatabaseInterface {
    execute(_sql: string): QueryResult {
      throw new Error(MISSING_BINDINGS_MESSAGE)
    }

    execute_batch(_sql: string): void {
      throw new Error(MISSING_BINDINGS_MESSAGE)
    }

    get_tables(): string[] {
      throw new Error(MISSING_BINDINGS_MESSAGE)
    }

    get_schema(_table: string): TableSchema {
      throw new Error(MISSING_BINDINGS_MESSAGE)
    }
  },
}

export default fallbackModule.default
export const Database = fallbackModule.Database
