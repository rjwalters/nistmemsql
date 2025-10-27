// Type declarations for WASM module
// The WASM package is generated at build time and not available during type checking
declare module '*/nistmemsql_wasm' {
  export default function init(): Promise<void>
  export class Database {
    execute(sql: string): {
      columns: string[]
      rows: (string | number | boolean | null)[][]
      row_count: number
    }
    execute_batch(sql: string): void
    get_tables(): string[]
    get_schema(table: string): {
      name: string
      columns: Array<{
        name: string
        data_type: string
        nullable: boolean
      }>
    }
  }
}
