/**
 * SQL:1999 Feature Showcase - Example Metadata
 *
 * Metadata for SQL examples organized by feature category.
 * SQL payloads and expected results are loaded separately from JSON files.
 */

export interface QueryExample {
  id: string
  title: string
  database: 'northwind' | 'employees' | 'company' | 'university' | 'empty' | 'sqllogictest'
  sql: string
  description: string
  sqlFeatures: string[]
  // Expected results for testing
  expectedRows?: string[][]
  expectedCount?: number
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
  database: 'northwind' | 'employees' | 'company' | 'university' | 'empty' | 'sqllogictest'
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
 * Load JSON metadata file by category ID
 */
async function loadCategoryJSON(categoryId: string): Promise<CategoryMetadata> {
  // Dynamically import JSON files to keep them separate and maintainable
  const jsonModule = await import(`./examples/${categoryId}.json`, {
    assert: { type: 'json' }
  })
  return jsonModule.default as CategoryMetadata
}

/**
 * Map of category IDs to their JSON files
 */
const CATEGORY_IDS = [
  'basic',
  'string',
  'joins',
  'aggregates',
  'recursive',
  'subqueries',
  'case',
  'set',
  'company',
  'university',
  'ddl',
  'dml',
  'datetime',
  'patterns',
  'math',
  'null-handling',
  'window',
  'business-intelligence',
  'data-quality',
  'advanced-multi-feature',
  'sql1999-standards',
  'performance-patterns',
  'report-templates',
  'sqllogictest'
]

/**
 * Lazy-loaded cache of example categories
 */
let exampleCategoriesCache: CategoryMetadata[] | null = null

/**
 * Initialize metadata from JSON files (lazy load)
 */
async function initializeMetadata(): Promise<CategoryMetadata[]> {
  if (exampleCategoriesCache !== null) {
    return exampleCategoriesCache
  }

  try {
    // Load all category JSON files in parallel
    const categoryPromises = CATEGORY_IDS.map(id => loadCategoryJSON(id))
    exampleCategoriesCache = await Promise.all(categoryPromises)
    return exampleCategoriesCache
  } catch (error) {
    console.error('Failed to load example metadata:', error)
    // Return empty array on error to allow app to continue
    return []
  }
}

/**
 * Validate that loaded JSON matches expected types
 */
function validateCategoryMetadata(data: unknown): data is CategoryMetadata {
  if (typeof data !== 'object' || data === null) return false
  const obj = data as Record<string, unknown>
  return (
    typeof obj.id === 'string' &&
    typeof obj.title === 'string' &&
    typeof obj.description === 'string' &&
    Array.isArray(obj.queries)
  )
}

/**
 * Get all example metadata flattened
 */
export async function getAllExampleMetadata(): Promise<ExampleMetadata[]> {
  const categories = await initializeMetadata()
  return categories.flatMap(cat => cat.queries)
}

/**
 * Find metadata by ID
 */
export async function findExampleMetadata(id: string): Promise<ExampleMetadata | undefined> {
  const all = await getAllExampleMetadata()
  return all.find(ex => ex.id === id)
}

/**
 * Get metadata for examples of a specific database
 */
export async function getExampleMetadataForDatabase(
  database: 'northwind' | 'employees' | 'company' | 'university' | 'empty' | 'sqllogictest'
): Promise<ExampleMetadata[]> {
  const all = await getAllExampleMetadata()
  return all.filter(ex => ex.database === database)
}

/**
 * Get all categories (useful for navigation)
 */
export async function getExampleCategories(): Promise<CategoryMetadata[]> {
  return initializeMetadata()
}

/**
 * Get a specific category by ID
 */
export async function getExampleCategory(id: string): Promise<CategoryMetadata | undefined> {
  const categories = await initializeMetadata()
  return categories.find(cat => cat.id === id)
}
