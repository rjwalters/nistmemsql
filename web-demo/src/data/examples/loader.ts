import type { QueryExample } from '../examples'
import type { CategoryPayloads } from './types'

// Import JSON payloads directly (these will be bundled by Vite)
import advancedMultiFeaturePayloads from './advanced-multi-feature.json'
import aggregatesPayloads from './aggregates.json'
import basicPayloads from './basic.json'
import businessIntelligencePayloads from './business-intelligence.json'
import casePayloads from './case.json'
import companyPayloads from './company.json'
import dataQualityPayloads from './data-quality.json'
import datetimePayloads from './datetime.json'
import ddlPayloads from './ddl.json'
import dmlPayloads from './dml.json'
import joinsPayloads from './joins.json'
import mathPayloads from './math.json'
import nullHandlingPayloads from './null-handling.json'
import patternsPayloads from './patterns.json'
import performancePatternsPayloads from './performance-patterns.json'
import recursivePayloads from './recursive.json'
import reportTemplatesPayloads from './report-templates.json'
import setPayloads from './set.json'
import sql1999StandardsPayloads from './sql1999-standards.json'
import sqlLogicTestPayloads from './sqllogictest.json'
import stringPayloads from './string.json'
import subqueriesPayloads from './subqueries.json'
import universityPayloads from './university.json'
import windowPayloads from './window.json'

/**
 * Get payloads for a category
 */
function getCategoryPayloads(categoryId: string): CategoryPayloads {
  switch (categoryId) {
    case 'advanced-multi-feature':
      return advancedMultiFeaturePayloads as CategoryPayloads
    case 'aggregates':
      return aggregatesPayloads as CategoryPayloads
    case 'basic':
      return basicPayloads as CategoryPayloads
    case 'business-intelligence':
      return businessIntelligencePayloads as CategoryPayloads
    case 'case':
      return casePayloads as CategoryPayloads
    case 'company':
      return companyPayloads as CategoryPayloads
    case 'data-quality':
      return dataQualityPayloads as CategoryPayloads
    case 'datetime':
      return datetimePayloads as CategoryPayloads
    case 'ddl':
      return ddlPayloads as CategoryPayloads
    case 'dml':
      return dmlPayloads as CategoryPayloads
    case 'joins':
      return joinsPayloads as CategoryPayloads
    case 'math':
      return mathPayloads as CategoryPayloads
    case 'null-handling':
      return nullHandlingPayloads as CategoryPayloads
    case 'patterns':
      return patternsPayloads as CategoryPayloads
    case 'performance-patterns':
      return performancePatternsPayloads as CategoryPayloads
    case 'recursive':
      return recursivePayloads as CategoryPayloads
    case 'report-templates':
      return reportTemplatesPayloads as CategoryPayloads
    case 'set':
      return setPayloads as CategoryPayloads
    case 'sql1999-standards':
      return sql1999StandardsPayloads as CategoryPayloads
    case 'sqllogictest':
      return sqlLogicTestPayloads as CategoryPayloads
    case 'string':
      return stringPayloads as CategoryPayloads
    case 'subqueries':
      return subqueriesPayloads as CategoryPayloads
    case 'university':
      return universityPayloads as CategoryPayloads
    case 'window':
      return windowPayloads as CategoryPayloads
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
    sql: payload.sql,
      expectedRows: payload.expectedRows,
      expectedCount: payload.expectedCount
    }
  })
}
