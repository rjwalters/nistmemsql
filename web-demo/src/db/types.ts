/**
 * Type-safe interfaces for WASM database bindings
 */

/** Main database interface */
export interface Database {
  /** Execute a single SQL statement and return results */
  execute(sql: string): QueryResult

  /** Execute multiple SQL statements (no results) */
  execute_batch(sql: string): void

  /** Get list of all table names */
  get_tables(): string[]

  /** Get schema information for a table */
  get_schema(table: string): TableSchema
}

/** Query execution result */
export interface QueryResult {
  columns: string[]
  rows: SqlValue[][]
  row_count: number
}

/** Table schema metadata */
export interface TableSchema {
  name: string
  columns: ColumnInfo[]
}

/** Column metadata */
export interface ColumnInfo {
  name: string
  data_type: string
  nullable: boolean
}

/** SQL value types */
export type SqlValue = string | number | boolean | null

/** WASM module interface */
export interface WasmModule {
  /** Initialize WASM (must be called first) */
  default(): Promise<void>

  /** Database constructor */
  Database: {
    new (): Database
  }
}
