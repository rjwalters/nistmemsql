import type { QueryExample } from '../examples'
import type { CategoryPayloads } from './types'

// Import JSON payloads directly (these will be bundled by Vite)
import basicPayloads from './basic.json'

/**
 * Get payloads for a category
 */
function getCategoryPayloads(categoryId: string): CategoryPayloads {
  switch (categoryId) {
    case 'basic':
      return basicPayloads as CategoryPayloads
    default:
      console.warn(`No payloads found for category ${categoryId}`)
      return {}
  }
}

/**
 * Combine metadata with payloads to create complete QueryExample objects
 */
export function loadExamplesForCategory(
  categoryId: string,
  _title: string,
  _description: string,
  metadataExamples: Omit<QueryExample, 'sql'>[]
): QueryExample[] {
  const payloads = getCategoryPayloads(categoryId)

  return metadataExamples.map(metadata => {
    const payload = payloads[metadata.id]
    if (!payload) {
      console.warn(`No payload found for example ${metadata.id}`)
      return { ...metadata, sql: '' }
    }

    return {
      ...metadata,
      sql: payload.sql
    }
  })
}
