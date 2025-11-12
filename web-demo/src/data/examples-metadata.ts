/**
 * SQL:1999 Feature Showcase - Example Metadata
 *
 * Metadata generated from JSON source files. No duplication with payloads.
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

// Import JSON payloads directly (these will be bundled by Vite)
import advancedMultiFeaturePayloads from './examples/advanced-multi-feature.json'
import aggregatesPayloads from './examples/aggregates.json'
import basicPayloads from './examples/basic.json'
import businessIntelligencePayloads from './examples/business-intelligence.json'
import casePayloads from './examples/case.json'
import companyPayloads from './examples/company.json'
import dataQualityPayloads from './examples/data-quality.json'
import datetimePayloads from './examples/datetime.json'
import ddlPayloads from './examples/ddl.json'
import dmlPayloads from './examples/dml.json'
import joinsPayloads from './examples/joins.json'
import mathPayloads from './examples/math.json'
import nullHandlingPayloads from './examples/null-handling.json'
import patternsPayloads from './examples/patterns.json'
import performancePatternsPayloads from './examples/performance-patterns.json'
import recursivePayloads from './examples/recursive.json'
import reportTemplatesPayloads from './examples/report-templates.json'
import setPayloads from './examples/set.json'
import sql1999StandardsPayloads from './examples/sql1999-standards.json'
import sqlLogicTestPayloads from './examples/sqllogictest.json'
import stringPayloads from './examples/string.json'
import subqueriesPayloads from './examples/subqueries.json'
import universityPayloads from './examples/university.json'
import windowPayloads from './examples/window.json'

/**
 * Extract metadata from JSON payloads
 * Omits SQL-related fields to avoid duplication
 */
function extractMetadata(payload: Record<string, any>): ExampleMetadata[] {
  return Object.entries(payload)
    .map(([id, data]: [string, any]) => ({
      id,
      title: data.title,
      database: data.database,
      description: data.description,
      sqlFeatures: data.sqlFeatures,
      difficulty: data.difficulty,
      useCase: data.useCase,
      performanceNotes: data.performanceNotes,
      executionTimeMs: data.executionTimeMs,
      relatedExamples: data.relatedExamples,
      tags: data.tags
    }))
}

/**
 * Category configuration with metadata extracted from JSON
 */
interface CategoryConfig {
  id: string
  title: string
  description: string
  payloads: Record<string, any>
}

const categoryConfigs: CategoryConfig[] = [
  {
    id: 'advanced-multi-feature',
    title: 'Advanced Multi-Feature Queries',
    description: 'Complex queries combining multiple SQL features',
    payloads: advancedMultiFeaturePayloads
  },
  {
    id: 'aggregates',
    title: 'Aggregates',
    description: 'COUNT, SUM, AVG, MIN, MAX, GROUP BY, HAVING',
    payloads: aggregatesPayloads
  },
  {
    id: 'basic',
    title: 'Basic Queries',
    description: 'SELECT, WHERE, ORDER BY, LIMIT fundamentals',
    payloads: basicPayloads
  },
  {
    id: 'business-intelligence',
    title: 'Business Intelligence',
    description: 'Reports, metrics, and business analytics queries',
    payloads: businessIntelligencePayloads
  },
  {
    id: 'case',
    title: 'CASE Expressions',
    description: 'Conditional logic with CASE and COALESCE',
    payloads: casePayloads
  },
  {
    id: 'company',
    title: 'Company Database',
    description: 'Queries using the company database',
    payloads: companyPayloads
  },
  {
    id: 'data-quality',
    title: 'Data Quality',
    description: 'Validation, profiling, and data quality checks',
    payloads: dataQualityPayloads
  },
  {
    id: 'datetime',
    title: 'Date & Time Functions',
    description: 'Date arithmetic, extraction, and formatting',
    payloads: datetimePayloads
  },
  {
    id: 'ddl',
    title: 'DDL (Data Definition)',
    description: 'CREATE, ALTER, DROP statements',
    payloads: ddlPayloads
  },
  {
    id: 'dml',
    title: 'DML (Data Manipulation)',
    description: 'INSERT, UPDATE, DELETE statements',
    payloads: dmlPayloads
  },
  {
    id: 'joins',
    title: 'Joins',
    description: 'INNER, LEFT, RIGHT, FULL, CROSS joins',
    payloads: joinsPayloads
  },
  {
    id: 'math',
    title: 'Math Functions',
    description: 'ABS, CEIL, FLOOR, ROUND, POWER, SQRT',
    payloads: mathPayloads
  },
  {
    id: 'null-handling',
    title: 'NULL Handling',
    description: 'IS NULL, COALESCE, NULLIF functions',
    payloads: nullHandlingPayloads
  },
  {
    id: 'patterns',
    title: 'SQL Patterns',
    description: 'Common patterns and techniques',
    payloads: patternsPayloads
  },
  {
    id: 'performance-patterns',
    title: 'Performance Patterns',
    description: 'Optimization techniques and best practices',
    payloads: performancePatternsPayloads
  },
  {
    id: 'recursive',
    title: 'Recursive Queries',
    description: 'WITH RECURSIVE and hierarchical data',
    payloads: recursivePayloads
  },
  {
    id: 'report-templates',
    title: 'Report Templates',
    description: 'Pre-built report queries',
    payloads: reportTemplatesPayloads
  },
  {
    id: 'set',
    title: 'Set Operations',
    description: 'UNION, INTERSECT, EXCEPT operators',
    payloads: setPayloads
  },
  {
    id: 'sql1999-standards',
    title: 'SQL:1999 Standards',
    description: 'SQL standard compliance and features',
    payloads: sql1999StandardsPayloads
  },
  {
    id: 'sqllogictest',
    title: 'SQL Logic Test',
    description: 'Logic test examples',
    payloads: sqlLogicTestPayloads
  },
  {
    id: 'string',
    title: 'String Functions',
    description: 'UPPER, LOWER, SUBSTRING, TRIM, CONCAT, LENGTH, and more',
    payloads: stringPayloads
  },
  {
    id: 'subqueries',
    title: 'Subqueries',
    description: 'Scalar, inline views, and correlated subqueries',
    payloads: subqueriesPayloads
  },
  {
    id: 'university',
    title: 'University Database',
    description: 'Queries using the university database',
    payloads: universityPayloads
  },
  {
    id: 'window',
    title: 'Window Functions',
    description: 'ROW_NUMBER, RANK, DENSE_RANK, LAG, LEAD, aggregates with OVER',
    payloads: windowPayloads
  }
]

/**
 * Metadata for all example categories (without SQL payloads)
 * Generated from JSON source files
 */
export const exampleCategoriesMetadata: CategoryMetadata[] = categoryConfigs.map(config => ({
  id: config.id,
  title: config.title,
  description: config.description,
  queries: extractMetadata(config.payloads)
}))

/**
 * Helper functions for accessing metadata
 */

/**
 * Get all example metadata across all categories
 */
export function getAllExampleMetadata(): ExampleMetadata[] {
  return exampleCategoriesMetadata.flatMap(cat => cat.queries)
}

/**
 * Find metadata for a specific example by ID
 */
export function findExampleMetadata(id: string): ExampleMetadata | undefined {
  return getAllExampleMetadata().find(ex => ex.id === id)
}

/**
 * Get example metadata for a specific database
 */
export function getExampleMetadataForDatabase(
  database: 'northwind' | 'employees' | 'company' | 'university' | 'empty' | 'sqllogictest'
): ExampleMetadata[] {
  return getAllExampleMetadata().filter(ex => ex.database === database)
}
