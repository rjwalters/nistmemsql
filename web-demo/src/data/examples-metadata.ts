/**
 * SQL:1999 Feature Showcase - Example Metadata
 *
 * Metadata for SQL examples organized by feature category.
 * SQL payloads and expected results are loaded separately from JSON files.
 */

export interface QueryExample {
  id: string
  title: string
  database: 'northwind' | 'employees' | 'company' | 'university' | 'empty'
  sql: string
  description: string
  sqlFeatures: string[]
  // Enhanced metadata (Step 2 additions)
  difficulty?: 'beginner' | 'intermediate' | 'advanced'
  useCase?: 'analytics' | 'admin' | 'development' | 'reports' | 'data-quality'
  performanceNotes?: string
  executionTimeMs?: number
  relatedExamples?: string[] // IDs of related examples
  tags?: string[] // Searchable tags
}

export interface ExampleCategory {
  id: string
  title: string
  description: string
  queries: QueryExample[]
}

export interface ExampleMetadata {
  id: string
  title: string
  database: 'northwind' | 'employees' | 'company' | 'university' | 'empty'
  description: string
  sqlFeatures: string[]
  difficulty?: 'beginner' | 'intermediate' | 'advanced'
  useCase?: 'analytics' | 'admin' | 'development' | 'reports' | 'data-quality'
  performanceNotes?: string
  executionTimeMs?: number
  relatedExamples?: string[]
  tags?: string[]
}

export interface CategoryMetadata {
  id: string
  title: string
  description: string
  queries: ExampleMetadata[]
}

/**
 * Metadata for all example categories (without SQL payloads)
 */
export const exampleCategoriesMetadata: CategoryMetadata[] = [
  {
    id: 'basic',
    title: 'Basic Queries',
    description: 'SELECT, WHERE, ORDER BY, LIMIT fundamentals',
    queries: [
      {
        id: 'basic-1',
        title: 'Simple SELECT',
        database: 'northwind',
        description: 'Retrieve first 5 products',
        sqlFeatures: ['SELECT', 'LIMIT'],
      },
      {
        id: 'basic-2',
        title: 'WHERE clause filtering',
        database: 'northwind',
        description: 'Filter products by price with sorting',
        sqlFeatures: ['SELECT', 'WHERE', 'ORDER BY'],
      },
      {
        id: 'basic-3',
        title: 'Column aliases',
        database: 'northwind',
        description: 'Use aliases for columns and calculated fields',
        sqlFeatures: ['SELECT', 'AS', 'Expressions'],
      },
      {
        id: 'basic-4',
        title: 'DISTINCT values',
        database: 'northwind',
        description: 'Get unique category IDs',
        sqlFeatures: ['SELECT', 'DISTINCT', 'ORDER BY'],
      },
    ],
  },
  // TODO: Add other categories...
]

/**
 * Get all example metadata flattened
 */
export function getAllExampleMetadata(): ExampleMetadata[] {
  return exampleCategoriesMetadata.flatMap(cat => cat.queries)
}

/**
 * Find metadata by ID
 */
export function findExampleMetadata(id: string): ExampleMetadata | undefined {
  return getAllExampleMetadata().find(ex => ex.id === id)
}

/**
 * Get metadata for examples of a specific database
 */
export function getExampleMetadataForDatabase(
  database: 'northwind' | 'employees' | 'company' | 'university' | 'empty'
): ExampleMetadata[] {
  return getAllExampleMetadata().filter(ex => ex.database === database)
}
