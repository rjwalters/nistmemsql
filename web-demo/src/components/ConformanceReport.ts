import { Component } from './base'

interface ErrorTest {
  id: string
  sql: string
  error: string
}

interface ConformanceData {
  total: number
  passed: number
  failed: number
  errors: number
  pass_rate: number
  error_tests?: ErrorTest[]
}

interface ConformanceReportState {
  data: ConformanceData | null
  loading: boolean
  error: string | null
}

/**
 * Conformance Report component - displays SQL:1999 test results
 */
export class ConformanceReportComponent extends Component<ConformanceReportState> {
  constructor() {
    super('#conformance-content', {
      data: null,
      loading: true,
      error: null,
    })
    this.loadData()
  }

  private async loadData(): Promise<void> {
    try {
      const response = await fetch('/nistmemsql/badges/sqltest_results.json')
      if (!response.ok) {
        throw new Error(`Failed to load conformance data: ${response.statusText}`)
      }
      const data = (await response.json()) as ConformanceData
      this.setState({ data, loading: false })
    } catch (error) {
      this.setState({
        error: error instanceof Error ? error.message : 'Unknown error',
        loading: false,
      })
    }
  }

  private getStatusColor(passRate: number): string {
    if (passRate >= 80) return '#10b981' // green
    if (passRate >= 60) return '#84cc16' // lime
    if (passRate >= 40) return '#eab308' // yellow
    if (passRate >= 20) return '#f97316' // orange
    return '#ef4444' // red
  }

  private getStatusText(passRate: number): string {
    if (passRate >= 80) return 'Excellent'
    if (passRate >= 60) return 'Good'
    if (passRate >= 40) return 'Fair'
    if (passRate >= 20) return 'Poor'
    return 'Needs Work'
  }

  private renderMetadataCard(commit: string, timestamp: string, passRate: number): string {
    const statusColor = this.getStatusColor(passRate)
    const statusText = this.getStatusText(passRate)

    return `
      <div class="bg-white dark:bg-gray-800 rounded-lg shadow-md border border-gray-200 dark:border-gray-700 p-6">
        <div class="grid grid-cols-1 md:grid-cols-3 gap-4 text-sm">
          <div>
            <span class="text-gray-600 dark:text-gray-400">Generated:</span>
            <span class="ml-2 text-gray-900 dark:text-white font-medium">${timestamp}</span>
          </div>
          <div>
            <span class="text-gray-600 dark:text-gray-400">Commit:</span>
            <a
              href="https://github.com/rjwalters/nistmemsql/commit/${commit}"
              target="_blank"
              class="ml-2 text-blue-600 dark:text-blue-400 hover:underline font-mono"
            >
              ${commit}
            </a>
          </div>
          <div>
            <span class="text-gray-600 dark:text-gray-400">Status:</span>
            <span class="ml-2 font-medium" style="color: ${statusColor}">${statusText}</span>
          </div>
        </div>
      </div>
    `
  }

  private renderSummaryCards(data: ConformanceData): string {
    const passRate = data.pass_rate.toFixed(1)

    return `
      <div class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-5 gap-6 fade-in">
        <!-- Pass Rate Card (Featured) -->
        <div class="md:col-span-2 lg:col-span-2 bg-gradient-to-br from-blue-500 to-blue-700 rounded-lg shadow-lg p-8 text-white">
          <div class="text-sm font-semibold uppercase tracking-wider opacity-90 mb-2">Pass Rate</div>
          <div class="text-5xl font-bold mb-2">${passRate}%</div>
          <div class="text-sm opacity-75">of ${data.total} total tests</div>
          <div class="mt-6 bg-white/20 rounded-full h-3 overflow-hidden">
            <div
              class="bg-white h-full rounded-full transition-all duration-500"
              style="width: ${passRate}%"
            ></div>
          </div>
        </div>

        <!-- Passed Tests -->
        <div class="bg-white dark:bg-gray-800 rounded-lg shadow-md border border-gray-200 dark:border-gray-700 p-6">
          <div class="text-xs font-semibold uppercase tracking-wider text-gray-600 dark:text-gray-400 mb-2">Passed</div>
          <div class="text-3xl font-bold text-green-600 dark:text-green-400 mb-1">${data.passed}</div>
          <div class="text-2xl">✅</div>
        </div>

        <!-- Failed Tests -->
        <div class="bg-white dark:bg-gray-800 rounded-lg shadow-md border border-gray-200 dark:border-gray-700 p-6">
          <div class="text-xs font-semibold uppercase tracking-wider text-gray-600 dark:text-gray-400 mb-2">Failed</div>
          <div class="text-3xl font-bold text-red-600 dark:text-red-400 mb-1">${data.failed}</div>
          <div class="text-2xl">❌</div>
        </div>

        <!-- Errors -->
        <div class="bg-white dark:bg-gray-800 rounded-lg shadow-md border border-gray-200 dark:border-gray-700 p-6">
          <div class="text-xs font-semibold uppercase tracking-wider text-gray-600 dark:text-gray-400 mb-2">Errors</div>
          <div class="text-3xl font-bold text-orange-600 dark:text-orange-400 mb-1">${data.errors}</div>
          <div class="text-2xl">⚠️</div>
        </div>
      </div>
    `
  }

  private renderTestCoverage(): string {
    return `
      <div class="bg-white dark:bg-gray-800 rounded-lg shadow-md border border-gray-200 dark:border-gray-700 p-8">
        <h2 class="text-2xl font-bold text-gray-900 dark:text-white mb-6">Test Coverage</h2>

        <p class="text-gray-700 dark:text-gray-300 mb-6">
          Tests from <a href="https://github.com/elliotchance/sqltest" target="_blank" class="text-blue-600 dark:text-blue-400 hover:underline">sqltest</a>
          - upstream-recommended SQL:1999 conformance test suite.
        </p>

        <div class="grid grid-cols-1 md:grid-cols-2 gap-4">
          <div>
            <h3 class="text-lg font-semibold text-gray-900 dark:text-white mb-3">Core Features (E-Series)</h3>
            <ul class="space-y-2 text-sm text-gray-700 dark:text-gray-300">
              <li><span class="font-mono text-xs bg-gray-100 dark:bg-gray-700 px-2 py-1 rounded">E011</span> Numeric data types</li>
              <li><span class="font-mono text-xs bg-gray-100 dark:bg-gray-700 px-2 py-1 rounded">E021</span> Character string types</li>
              <li><span class="font-mono text-xs bg-gray-100 dark:bg-gray-700 px-2 py-1 rounded">E031</span> Identifiers</li>
              <li><span class="font-mono text-xs bg-gray-100 dark:bg-gray-700 px-2 py-1 rounded">E051</span> Basic query specification</li>
              <li><span class="font-mono text-xs bg-gray-100 dark:bg-gray-700 px-2 py-1 rounded">E061</span> Basic predicates and search conditions</li>
              <li><span class="font-mono text-xs bg-gray-100 dark:bg-gray-700 px-2 py-1 rounded">E071</span> Basic query expressions</li>
              <li><span class="font-mono text-xs bg-gray-100 dark:bg-gray-700 px-2 py-1 rounded">E081</span> Basic privileges</li>
              <li><span class="font-mono text-xs bg-gray-100 dark:bg-gray-700 px-2 py-1 rounded">E091</span> Set functions</li>
            </ul>
          </div>
          <div>
            <h3 class="text-lg font-semibold text-gray-900 dark:text-white mb-3">Additional Features</h3>
            <ul class="space-y-2 text-sm text-gray-700 dark:text-gray-300">
              <li><span class="font-mono text-xs bg-gray-100 dark:bg-gray-700 px-2 py-1 rounded">E101</span> Basic data manipulation</li>
              <li><span class="font-mono text-xs bg-gray-100 dark:bg-gray-700 px-2 py-1 rounded">E111</span> Single row SELECT statement</li>
              <li><span class="font-mono text-xs bg-gray-100 dark:bg-gray-700 px-2 py-1 rounded">E121</span> Basic cursor support</li>
              <li><span class="font-mono text-xs bg-gray-100 dark:bg-gray-700 px-2 py-1 rounded">E131</span> Null value support</li>
              <li><span class="font-mono text-xs bg-gray-100 dark:bg-gray-700 px-2 py-1 rounded">E141</span> Basic integrity constraints</li>
              <li><span class="font-mono text-xs bg-gray-100 dark:bg-gray-700 px-2 py-1 rounded">E151</span> Transaction support</li>
              <li><span class="font-mono text-xs bg-gray-100 dark:bg-gray-700 px-2 py-1 rounded">E161</span> SQL comments</li>
              <li><span class="font-mono text-xs bg-gray-100 dark:bg-gray-700 px-2 py-1 rounded">F031</span> Basic schema manipulation</li>
            </ul>
          </div>
        </div>
      </div>
    `
  }

  private renderFailingTests(errorTests: ErrorTest[]): string {
    if (errorTests.length === 0) return ''

    const errorCards = errorTests
      .map(
        test => `
        <div class="bg-gray-50 dark:bg-gray-900 rounded-lg p-4 border border-gray-200 dark:border-gray-700">
          <div class="flex items-start justify-between mb-2">
            <span class="font-mono text-xs text-blue-600 dark:text-blue-400 font-semibold">${this.escapeHtml(test.id)}</span>
          </div>
          <div class="font-mono text-sm text-gray-800 dark:text-gray-200 mb-2 bg-white dark:bg-gray-800 p-2 rounded overflow-x-auto">${this.escapeHtml(test.sql)}</div>
          <div class="text-xs text-red-600 dark:text-red-400"><span class="font-semibold">Error:</span> ${this.escapeHtml(test.error)}</div>
        </div>
      `
      )
      .join('')

    return `
      <div class="bg-white dark:bg-gray-800 rounded-lg shadow-md border border-gray-200 dark:border-gray-700 p-8">
        <h2 class="text-2xl font-bold text-gray-900 dark:text-white mb-6">Failing Tests</h2>

        <p class="text-gray-700 dark:text-gray-300 mb-4">
          The following tests are currently failing. Click to expand details.
        </p>

        <details class="mt-4">
          <summary class="cursor-pointer text-blue-600 dark:text-blue-400 hover:underline font-medium">
            View failing test details (${errorTests.length} tests)
          </summary>

          <div class="mt-4 space-y-3 max-h-96 overflow-y-auto">
            ${errorCards}
          </div>
        </details>
      </div>
    `
  }

  private renderRunningTestsLocally(): string {
    return `
      <div class="bg-white dark:bg-gray-800 rounded-lg shadow-md border border-gray-200 dark:border-gray-700 p-8">
        <h2 class="text-2xl font-bold text-gray-900 dark:text-white mb-6">Running Tests Locally</h2>

        <div class="bg-gray-100 dark:bg-gray-900 rounded-lg p-4 font-mono text-sm overflow-x-auto">
          <div class="text-gray-700 dark:text-gray-300">
            <div class="text-green-600 dark:text-green-400"># Run all conformance tests</div>
            <div>cargo test --test sqltest_conformance -- --nocapture</div>
            <div class="mt-4 text-green-600 dark:text-green-400"># Generate coverage report</div>
            <div>cargo coverage</div>
            <div class="mt-2 text-green-600 dark:text-green-400"># Open coverage report</div>
            <div>open target/llvm-cov/html/index.html</div>
          </div>
        </div>
      </div>
    `
  }

  protected render(): void {
    const { data, loading, error } = this.state

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
        ${this.renderMetadataCard(commit, timestamp, data.pass_rate)}
        ${this.renderSummaryCards(data)}
        ${this.renderTestCoverage()}
        ${data.error_tests && data.error_tests.length > 0 ? this.renderFailingTests(data.error_tests) : ''}
        ${this.renderRunningTestsLocally()}
      </div>
    `
  }
}
