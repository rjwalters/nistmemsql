/* tslint:disable */
/* eslint-disable */
/**
 * Initializes the WASM module and sets up panic hooks
 */
export function init_wasm(): void;
/**
 * Table column metadata
 */
export class ColumnInfo {
  private constructor();
  free(): void;
  [Symbol.dispose](): void;
  /**
   * Column name
   */
  name: string;
  /**
   * Data type (as string)
   */
  data_type: string;
  /**
   * Whether column can be NULL
   */
  nullable: boolean;
}
/**
 * In-memory SQL database with WASM bindings
 */
export class Database {
  free(): void;
  [Symbol.dispose](): void;
  /**
   * Creates a new empty database instance
   */
  constructor();
  /**
   * Executes a DDL or DML statement (CREATE TABLE, INSERT, UPDATE, DELETE)
   * Returns a JSON string with the result
   */
  execute(sql: string): any;
  /**
   * Executes a SELECT query and returns results as JSON
   */
  query(sql: string): any;
  /**
   * Lists all table names in the database
   */
  list_tables(): any;
  /**
   * Gets the schema for a specific table
   */
  describe_table(table_name: string): any;
  /**
   * Loads the Employees example database (hierarchical org structure)
   * Demonstrates recursive queries with WITH RECURSIVE
   */
  load_employees(): any;
  /**
   * Loads the Northwind example database (classic sales/orders database)
   * Demonstrates JOINs, aggregates, and relational database concepts
   */
  load_northwind(): any;
  /**
   * Returns the version string
   */
  version(): string;
}
/**
 * Result of an execute (DDL/DML) operation
 */
export class ExecuteResult {
  private constructor();
  free(): void;
  [Symbol.dispose](): void;
  /**
   * Number of rows affected (for INSERT, UPDATE, DELETE)
   */
  rows_affected: number;
  /**
   * Success message
   */
  message: string;
}
/**
 * Result of a query execution
 */
export class QueryResult {
  private constructor();
  free(): void;
  [Symbol.dispose](): void;
  /**
   * Column names
   */
  columns: string[];
  /**
   * Row data as JSON strings
   */
  rows: string[];
  /**
   * Number of rows
   */
  row_count: number;
}
/**
 * Table schema information
 */
export class TableSchema {
  private constructor();
  free(): void;
  [Symbol.dispose](): void;
  /**
   * Table name
   */
  name: string;
  /**
   * Column definitions
   */
  columns: ColumnInfo[];
}

export type InitInput = RequestInfo | URL | Response | BufferSource | WebAssembly.Module;

export interface InitOutput {
  readonly memory: WebAssembly.Memory;
  readonly init_wasm: () => void;
  readonly __wbg_queryresult_free: (a: number, b: number) => void;
  readonly __wbg_get_queryresult_columns: (a: number) => [number, number];
  readonly __wbg_set_queryresult_columns: (a: number, b: number, c: number) => void;
  readonly __wbg_get_queryresult_rows: (a: number) => [number, number];
  readonly __wbg_set_queryresult_rows: (a: number, b: number, c: number) => void;
  readonly __wbg_get_queryresult_row_count: (a: number) => number;
  readonly __wbg_set_queryresult_row_count: (a: number, b: number) => void;
  readonly __wbg_executeresult_free: (a: number, b: number) => void;
  readonly __wbg_get_executeresult_rows_affected: (a: number) => number;
  readonly __wbg_set_executeresult_rows_affected: (a: number, b: number) => void;
  readonly __wbg_columninfo_free: (a: number, b: number) => void;
  readonly __wbg_get_columninfo_name: (a: number) => [number, number];
  readonly __wbg_set_columninfo_name: (a: number, b: number, c: number) => void;
  readonly __wbg_get_columninfo_data_type: (a: number) => [number, number];
  readonly __wbg_set_columninfo_data_type: (a: number, b: number, c: number) => void;
  readonly __wbg_get_columninfo_nullable: (a: number) => number;
  readonly __wbg_set_columninfo_nullable: (a: number, b: number) => void;
  readonly __wbg_tableschema_free: (a: number, b: number) => void;
  readonly __wbg_get_tableschema_columns: (a: number) => [number, number];
  readonly __wbg_set_tableschema_columns: (a: number, b: number, c: number) => void;
  readonly __wbg_database_free: (a: number, b: number) => void;
  readonly database_new: () => number;
  readonly database_execute: (a: number, b: number, c: number) => [number, number, number];
  readonly database_query: (a: number, b: number, c: number) => [number, number, number];
  readonly database_list_tables: (a: number) => [number, number, number];
  readonly database_describe_table: (a: number, b: number, c: number) => [number, number, number];
  readonly database_load_employees: (a: number) => [number, number, number];
  readonly database_load_northwind: (a: number) => [number, number, number];
  readonly database_version: (a: number) => [number, number];
  readonly __wbg_set_executeresult_message: (a: number, b: number, c: number) => void;
  readonly __wbg_set_tableschema_name: (a: number, b: number, c: number) => void;
  readonly __wbg_get_executeresult_message: (a: number) => [number, number];
  readonly __wbg_get_tableschema_name: (a: number) => [number, number];
  readonly __wbindgen_free: (a: number, b: number, c: number) => void;
  readonly __wbindgen_malloc: (a: number, b: number) => number;
  readonly __wbindgen_realloc: (a: number, b: number, c: number, d: number) => number;
  readonly __wbindgen_export_3: WebAssembly.Table;
  readonly __externref_drop_slice: (a: number, b: number) => void;
  readonly __externref_table_alloc: () => number;
  readonly __externref_table_dealloc: (a: number) => void;
  readonly __wbindgen_start: () => void;
}

export type SyncInitInput = BufferSource | WebAssembly.Module;
/**
* Instantiates the given `module`, which can either be bytes or
* a precompiled `WebAssembly.Module`.
*
* @param {{ module: SyncInitInput }} module - Passing `SyncInitInput` directly is deprecated.
*
* @returns {InitOutput}
*/
export function initSync(module: { module: SyncInitInput } | SyncInitInput): InitOutput;

/**
* If `module_or_path` is {RequestInfo} or {URL}, makes a request and
* for everything else, calls `WebAssembly.instantiate` directly.
*
* @param {{ module_or_path: InitInput | Promise<InitInput> }} module_or_path - Passing `InitInput` directly is deprecated.
*
* @returns {Promise<InitOutput>}
*/
export default function __wbg_init (module_or_path?: { module_or_path: InitInput | Promise<InitInput> } | InitInput | Promise<InitInput>): Promise<InitOutput>;
