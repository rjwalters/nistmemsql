import type { ConformanceData, SQLLogicTestData } from './types'

/**
 * Handles loading and processing of conformance test data
 */
export class DataProcessor {
  /**
   * Load sqltest conformance data from JSON file
   */
  async loadSqltestData(): Promise<ConformanceData> {
    const response = await fetch('/nistmemsql/badges/sqltest_results.json')
    if (!response.ok) {
      throw new Error(`Failed to load sqltest data: ${response.statusText}`)
    }
    return (await response.json()) as ConformanceData
  }

  /**
   * Load SQLLogicTest data, preferring cumulative results over single-run results
   */
  async loadSQLLogicTestData(): Promise<SQLLogicTestData | null> {
    try {
      // Try cumulative results first (updated by boost workflow)
      let sltResponse = await fetch('/nistmemsql/badges/sqllogictest_cumulative.json')
      if (sltResponse.ok) {
        const cumulativeData = await sltResponse.json()
        // Transform cumulative data structure to match interface
        const result: SQLLogicTestData = {
          total: cumulativeData.summary.total_tested_files,
          passed: cumulativeData.summary.passed,
          failed: cumulativeData.summary.failed,
          errors: 0, // Not tracked in cumulative data
          pass_rate: cumulativeData.summary.pass_rate,
          categories: {}, // Not available in cumulative data
        }
        console.log('Loaded cumulative SQLLogicTest results')
        return result
      } else {
        // Fall back to single-run results (from CI workflow)
        console.log('Cumulative results not available, trying single-run results')
        sltResponse = await fetch('/nistmemsql/badges/sqllogictest_results.json')
        if (sltResponse.ok) {
          const singleRunData = await sltResponse.json()
          // Transform single-run data structure to match interface
          const result: SQLLogicTestData = {
            total: singleRunData.total_tested || singleRunData.total,
            passed: singleRunData.passed,
            failed: singleRunData.failed,
            errors: singleRunData.errors || 0,
            pass_rate: singleRunData.pass_rate,
            categories: {}, // Not available in single-run data
          }
          console.log('Loaded single-run SQLLogicTest results')
          return result
        }
      }
    } catch (error) {
      // SQLLogicTest data is optional, continue without it
      console.warn('SQLLogicTest results not available:', error)
    }
    return null
  }
}
