import type { ConformanceData, SQLLogicTestData } from './types'

/**
 * Handles loading and processing of conformance test data
 */
export class DataProcessor {
  /**
   * Load sqltest conformance data from JSON file
   * Falls back to using SQLLogicTest cumulative data if sqltest_results.json is unavailable
   */
  async loadSqltestData(): Promise<ConformanceData> {
    // Add cache-busting parameter to prevent CDN from serving stale 404s
    const cacheBust = Math.floor(Date.now() / 60000) // Update every minute
    const response = await fetch(`/vibesql/badges/sqltest_results.json?v=${cacheBust}`)
    if (!response.ok) {
      console.warn('sqltest_results.json not available, attempting to use cumulative data as fallback')
      // Try to use cumulative results as fallback
      const fallbackResponse = await fetch(`/vibesql/badges/sqllogictest_cumulative.json?v=${cacheBust}`)
      if (fallbackResponse.ok) {
        const cumulativeData = await fallbackResponse.json()
        // Transform cumulative data to ConformanceData format
        return {
          total: cumulativeData.summary.total_tested_files,
          passed: cumulativeData.summary.passed,
          failed: cumulativeData.summary.failed,
          errors: 0, // Not tracked in cumulative
          pass_rate: cumulativeData.summary.pass_rate,
        }
      }
      throw new Error(`Failed to load sqltest data: ${response.statusText}`)
    }
    return (await response.json()) as ConformanceData
  }

  /**
   * Load SQLLogicTest data, preferring cumulative results over single-run results
   */
  async loadSQLLogicTestData(): Promise<SQLLogicTestData | null> {
    try {
      // Add cache-busting parameter to prevent CDN from serving stale 404s
      const cacheBust = Math.floor(Date.now() / 60000) // Update every minute

      // Try cumulative results first (updated by boost workflow)
      let sltResponse = await fetch(`/vibesql/badges/sqllogictest_cumulative.json?v=${cacheBust}`)
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
        sltResponse = await fetch(`/vibesql/badges/sqllogictest_results.json?v=${cacheBust}`)
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
