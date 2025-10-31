/**
 * SQL:1999 Feature Showcase - Example Queries
 *
 * Comprehensive library of SQL examples organized by feature category.
 * SQL payloads are loaded from separate JSON files for maintainability.
 */

import type {
  QueryExample,
  ExampleCategory,
  ExampleMetadata,
  CategoryMetadata
} from './examples-metadata'

// Re-export types and interfaces
export type {
  QueryExample,
  ExampleCategory,
  ExampleMetadata,
  CategoryMetadata
}

// Re-export for backward compatibility
export { exampleCategoriesMetadata } from './examples-metadata'

// Re-export metadata functions
export {
  getAllExampleMetadata,
  findExampleMetadata,
  getExampleMetadataForDatabase
} from './examples-metadata'

// Import loader
import { loadExamplesForCategory } from './examples/loader'
import { exampleCategoriesMetadata } from './examples-metadata'

/**
 * Load all example categories with SQL payloads
 */
export const exampleCategories: ExampleCategory[] = exampleCategoriesMetadata.map(metadata => ({
  id: metadata.id,
  title: metadata.title,
  description: metadata.description,
  queries: loadExamplesForCategory(
    metadata.id,
    metadata.title,
    metadata.description,
    metadata.queries
  )
}))

/**
 * Get all examples flattened from all categories
 */
export function getAllExamples(): QueryExample[] {
  return exampleCategories.flatMap(cat => cat.queries)
}

/**
 * Find an example by ID
 */
export function findExample(id: string): QueryExample | undefined {
  return getAllExamples().find(ex => ex.id === id)
}

/**
 * Get examples for a specific database
 */
export function getExamplesForDatabase(
  database: 'northwind' | 'employees' | 'company' | 'university' | 'empty'
): QueryExample[] {
  return getAllExamples().filter(ex => ex.database === database)
}
