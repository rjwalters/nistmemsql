import { escapeHtml } from './utils'

interface DatabaseComparison {
  database: string
  percentage: number
  target?: number
  notes: string
}

export class DatabaseComparisonComponent {
  render(comparisons: DatabaseComparison[]): string {
    const maxPercentage = Math.max(...comparisons.map(db => db.percentage))

    return `
      <div class="bg-white dark:bg-gray-800 rounded-lg shadow p-6">
        <h3 class="text-xl font-semibold text-gray-900 dark:text-gray-100 mb-1">
          📊 SQL:1999 Core Compliance Comparison
        </h3>
        <p class="text-sm text-gray-600 dark:text-gray-400 mb-6">
          Approximate compliance estimates based on published documentation
        </p>

        <div class="space-y-4">
          ${comparisons.map(db => {
            const isNistMemSQL = db.database === 'nistmemsql'
            const displayPercentage = isNistMemSQL && db.target ? db.target : db.percentage
            const progressPercentage = db.percentage

            return `
              <div>
                <div class="flex justify-between items-center mb-2">
                  <div class="flex items-center gap-2">
                    <span class="font-medium text-sm text-gray-900 dark:text-gray-100 ${isNistMemSQL ? 'font-bold' : ''}">
                      ${escapeHtml(db.database)}
                    </span>
                    ${isNistMemSQL && db.target ? `
                      <span class="text-xs text-gray-600 dark:text-gray-400">
                        (currently ${db.percentage}%)
                      </span>
                    ` : ''}
                  </div>
                  <span class="text-sm font-semibold text-gray-900 dark:text-gray-100">
                    ${displayPercentage}%${isNistMemSQL && db.target ? ' target' : ''}
                  </span>
                </div>
                <div class="h-3 bg-gray-200 dark:bg-gray-700 rounded-full overflow-hidden">
                  ${isNistMemSQL && db.target ? `
                    <div class="h-full relative">
                      <div
                        class="h-full bg-blue-500 transition-all duration-500 absolute"
                        style="width: ${progressPercentage}%"
                        role="progressbar"
                        aria-valuenow="${progressPercentage}"
                        aria-valuemin="0"
                        aria-valuemax="${displayPercentage}"
                        aria-label="${escapeHtml(db.database)} current: ${progressPercentage}%"
                      ></div>
                      <div
                        class="h-full border-2 border-blue-300 dark:border-blue-600 bg-transparent absolute"
                        style="width: ${displayPercentage}%"
                        aria-label="${escapeHtml(db.database)} target: ${displayPercentage}%"
                      ></div>
                    </div>
                  ` : `
                    <div
                      class="h-full ${displayPercentage === 100 ? 'bg-green-500' : 'bg-gray-400 dark:bg-gray-500'} transition-all duration-500"
                      style="width: ${(displayPercentage / maxPercentage) * 100}%"
                      role="progressbar"
                      aria-valuenow="${displayPercentage}"
                      aria-valuemin="0"
                      aria-valuemax="${maxPercentage}"
                      aria-label="${escapeHtml(db.database)}: ${displayPercentage}%"
                    ></div>
                  `}
                </div>
                <p class="text-xs text-gray-600 dark:text-gray-400 mt-1">
                  ${escapeHtml(db.notes)}
                </p>
              </div>
            `
          }).join('')}
        </div>
      </div>
    `
  }
}
