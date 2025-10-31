import { escapeHtml } from './utils'

interface TestStatistics {
  total: number
  passing: number
  coverage: number
  linesOfCode: number
  byModule: Record<string, number>
}

export class TestStatisticsComponent {
  render(stats: TestStatistics): string {
    const passingRate = ((stats.passing / stats.total) * 100).toFixed(1)

    return `
      <div class="bg-white dark:bg-gray-800 rounded-lg shadow-lg p-6">
        <h3 class="text-xl font-semibold text-gray-900 dark:text-gray-100 mb-4">
          Test Coverage & Statistics
        </h3>
        <p class="text-sm text-gray-600 dark:text-gray-400 mb-6">
          Comprehensive test suite metrics and code coverage analysis
        </p>

        <!-- Overall Stats Grid -->
        <div class="grid grid-cols-2 md:grid-cols-4 gap-4 mb-6">
          <div class="bg-gray-50 dark:bg-gray-700/50 rounded-lg p-4">
            <div class="text-sm text-gray-600 dark:text-gray-400 mb-1">Total Tests</div>
            <div class="text-2xl font-bold text-gray-900 dark:text-gray-100">
              ${stats.total}
            </div>
          </div>
          <div class="bg-green-50 dark:bg-green-900/20 rounded-lg p-4">
            <div class="text-sm text-gray-600 dark:text-gray-400 mb-1">Passing</div>
            <div class="text-2xl font-bold text-green-600 dark:text-green-400">
              ${stats.passing}
            </div>
            <div class="text-xs text-gray-500 dark:text-gray-500">${passingRate}%</div>
          </div>
          <div class="bg-blue-50 dark:bg-blue-900/20 rounded-lg p-4">
            <div class="text-sm text-gray-600 dark:text-gray-400 mb-1">Code Coverage</div>
            <div class="text-2xl font-bold text-blue-600 dark:text-blue-400">
              ${stats.coverage}%
            </div>
          </div>
          <div class="bg-purple-50 dark:bg-purple-900/20 rounded-lg p-4">
            <div class="text-sm text-gray-600 dark:text-gray-400 mb-1">Lines of Code</div>
            <div class="text-2xl font-bold text-purple-600 dark:text-purple-400">
              ${stats.linesOfCode.toLocaleString()}
            </div>
          </div>
        </div>

        <!-- Module Coverage Breakdown -->
        <div>
          <h4 class="text-lg font-semibold text-gray-900 dark:text-gray-100 mb-4">
            Coverage by Module
          </h4>
          <div class="space-y-3">
            ${Object.entries(stats.byModule)
              .map(
                ([module, coverage]) => `
              <div>
                <div class="flex items-center justify-between mb-1">
                  <span class="text-sm font-medium text-gray-700 dark:text-gray-300">${escapeHtml(module)}</span>
                  <span class="text-sm font-semibold ${
                    coverage >= 80
                      ? 'text-green-600 dark:text-green-400'
                      : coverage >= 60
                        ? 'text-yellow-600 dark:text-yellow-400'
                        : 'text-red-600 dark:text-red-400'
                  }">${coverage}%</span>
                </div>
                <div class="h-2 bg-gray-200 dark:bg-gray-700 rounded-full overflow-hidden">
                  <div
                    class="h-full transition-all duration-300 ${
                      coverage >= 80
                        ? 'bg-green-500'
                        : coverage >= 60
                          ? 'bg-yellow-500'
                          : 'bg-red-500'
                    }"
                    style="width: ${coverage}%"
                    role="progressbar"
                    aria-valuenow="${coverage}"
                    aria-valuemin="0"
                    aria-valuemax="100"
                    aria-label="Coverage for ${escapeHtml(module)}: ${coverage}%"
                  ></div>
                </div>
              </div>
            `
              )
              .join('')}
          </div>
        </div>

        <!-- Coverage Legend -->
        <div class="mt-6 pt-4 border-t border-gray-200 dark:border-gray-700">
          <div class="flex flex-wrap gap-4 text-sm">
            <div class="flex items-center gap-2">
              <div class="w-4 h-4 bg-green-500 rounded"></div>
              <span class="text-gray-600 dark:text-gray-400">≥ 80% (Excellent)</span>
            </div>
            <div class="flex items-center gap-2">
              <div class="w-4 h-4 bg-yellow-500 rounded"></div>
              <span class="text-gray-600 dark:text-gray-400">60-79% (Good)</span>
            </div>
            <div class="flex items-center gap-2">
              <div class="w-4 h-4 bg-red-500 rounded"></div>
              <span class="text-gray-600 dark:text-gray-400">< 60% (Needs Improvement)</span>
            </div>
          </div>
        </div>
      </div>
    `
  }
}
