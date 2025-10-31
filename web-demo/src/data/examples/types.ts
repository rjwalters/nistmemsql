/**
 * JSON schema for example SQL payloads
 */

export interface ExamplePayload {
  /** The SQL query string */
  sql: string
  /** Expected result rows (excluding header) */
  expectedRows?: string[][]
  /** Expected number of rows */
  expectedCount?: number
}

export interface CategoryPayloads {
  [exampleId: string]: ExamplePayload
}
