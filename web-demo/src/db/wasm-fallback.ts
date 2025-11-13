import type {
  Database as DatabaseInterface,
  QueryResult,
  ExecuteResult,
  TableSchema,
  WasmModule,
} from './types'

const MISSING_BINDINGS_MESSAGE =
  'WASM bindings are not available. Generate them with `wasm-pack build --target web --out-dir web-demo/public/pkg`.'

class FallbackDatabase implements DatabaseInterface {
  execute(_sql: string): ExecuteResult {
    throw new Error(MISSING_BINDINGS_MESSAGE)
  }

  query(_sql: string): QueryResult {
    throw new Error(MISSING_BINDINGS_MESSAGE)
  }

  list_tables(): string[] {
    throw new Error(MISSING_BINDINGS_MESSAGE)
  }

  describe_table(_table: string): TableSchema {
    throw new Error(MISSING_BINDINGS_MESSAGE)
  }

  load_employees(): ExecuteResult {
    throw new Error(MISSING_BINDINGS_MESSAGE)
  }

  load_northwind(): ExecuteResult {
    throw new Error(MISSING_BINDINGS_MESSAGE)
  }

  version(): string {
    throw new Error(MISSING_BINDINGS_MESSAGE)
  }
}

const FallbackDatabaseConstructor = Object.assign(FallbackDatabase, {
  newWithPersistence: () => new FallbackDatabase(),
}) as { new (): DatabaseInterface; newWithPersistence(): DatabaseInterface }

const fallbackModule: WasmModule = {
  default: async () => {
    console.warn(MISSING_BINDINGS_MESSAGE)
  },
  Database: FallbackDatabaseConstructor,
}

export default fallbackModule.default
export const Database = fallbackModule.Database
