import { escapeHtml } from './utils'

interface RecentUpdate {
  date: string
  feature: string
  tests: number
  pr: number
}

export class RecentUpdatesComponent {
  render(updates: RecentUpdate[]): string {
    return `
      <div class="bg-white dark:bg-gray-800 rounded-lg shadow p-6">
        <h3 class="text-xl font-semibold text-gray-900 dark:text-gray-100 mb-4">
          📰 Recent Updates
        </h3>

        <div class="space-y-3">
          ${updates.map(update => {
            const date = new Date(update.date)
            const formattedDate = date.toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' })

            return `
              <div class="flex items-start gap-3 pb-3 border-b border-gray-200 dark:border-gray-700 last:border-0 last:pb-0">
                <div class="flex-shrink-0 w-20 text-sm text-gray-600 dark:text-gray-400">
                  ${formattedDate}
                </div>
                <div class="flex-1">
                  <div class="font-medium text-gray-900 dark:text-gray-100 text-sm">
                    ${escapeHtml(update.feature)}
                  </div>
                  <div class="flex items-center gap-2 mt-1">
                    <span class="text-xs text-gray-600 dark:text-gray-400">
                      +${update.tests} tests
                    </span>
                    <a
                      href="https://github.com/rjwalters/nistmemsql/pull/${update.pr}"
                      target="_blank"
                      rel="noopener noreferrer"
                      class="text-xs text-blue-600 dark:text-blue-400 hover:text-blue-800 dark:hover:text-blue-300"
                    >
                      PR #${update.pr}
                    </a>
                  </div>
                </div>
                <div class="flex-shrink-0">
                  <span class="text-green-600 dark:text-green-400">✅</span>
                </div>
              </div>
            `
          }).join('')}
        </div>
      </div>
    `
  }
}
