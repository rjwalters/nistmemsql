import { Component } from './base'
import type { ConformanceReportState } from './conformance/types'
import { DataProcessor } from './conformance/data-processor'
import {
  renderMetadataCard,
  renderSqltestResults,
  renderExplanation,
  renderFailingTests,
  renderSQLLogicTestResults,
  renderRunningTestsLocally,
} from './conformance/section-renderers'

/**
 * Conformance Report component - displays SQL:1999 test results
 */
export class ConformanceReportComponent extends Component<ConformanceReportState> {
  private dataProcessor = new DataProcessor()

  constructor() {
    super('#conformance-content', {
      data: null,
      sltData: null,
      loading: true,
      error: null,
    })
    this.loadData()
  }

  private async loadData(): Promise<void> {
    try {
      const data = await this.dataProcessor.loadSqltestData()
      const sltData = await this.dataProcessor.loadSQLLogicTestData()
      this.setState({ data, sltData, loading: false })
    } catch (error) {
      this.setState({
        error: error instanceof Error ? error.message : 'Unknown error',
        loading: false,
      })
    }
  }

  protected render(): void {
    const { data, sltData, loading, error } = this.state

    if (loading) {
      this.element.innerHTML = `
        <div class="text-center py-12">
          <div class="inline-block animate-spin rounded-full h-12 w-12 border-b-2 border-blue-600"></div>
          <p class="mt-4 text-gray-600 dark:text-gray-400">Loading conformance report...</p>
        </div>
      `
      return
    }

    if (error) {
      this.element.innerHTML = `
        <div class="text-center py-12">
          <div class="text-red-600 dark:text-red-400 text-xl mb-4">⚠️ Error Loading Report</div>
          <p class="text-gray-600 dark:text-gray-400">${this.escapeHtml(error)}</p>
        </div>
      `
      return
    }

    if (!data) {
      this.element.innerHTML = `
        <div class="text-center py-12">
          <p class="text-gray-600 dark:text-gray-400">No conformance data available</p>
        </div>
      `
      return
    }

    const timestamp = new Date().toISOString().replace('T', ' ').split('.')[0] + ' UTC'
    const commit = 'latest'

    this.element.innerHTML = `
      <div class="space-y-8">
        ${renderMetadataCard(commit, timestamp, data.pass_rate || 0)}
        ${renderSqltestResults(data)}
        ${sltData ? renderSQLLogicTestResults(sltData) : ''}
        ${renderExplanation(data, sltData)}
        ${data.error_tests && data.error_tests.length > 0 ? renderFailingTests(data.error_tests) : ''}
        ${renderRunningTestsLocally()}
      </div>
    `
  }
}
