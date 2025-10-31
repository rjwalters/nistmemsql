import { getStatusColor, getStatusLabel, escapeHtml } from './utils'

interface ComplianceCategory {
  name: string
  percentage: number
  implemented: number
  total: number
  status: string
  description: string
}

export class CategoryBreakdownComponent {
  private renderCategoryRow(category: ComplianceCategory): string {
    const statusColor = getStatusColor(category.status)
    const statusLabel = getStatusLabel(category.status)

    return `
      <tr class="hover:bg-gray-50 dark:hover:bg-gray-700/50 transition-colors">
        <td class="px-4 py-4">
          <span class="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium ${statusColor}">
            ${statusLabel}
          </span>
        </td>
        <td class="px-4 py-4 font-medium text-gray-900 dark:text-gray-100">
          ${escapeHtml(category.name)}
        </td>
        <td class="px-4 py-4 text-sm text-gray-600 dark:text-gray-400">
          ${escapeHtml(category.description)}
        </td>
        <td class="px-4 py-4">
          <div class="flex items-center gap-3">
            <div class="flex-1 min-w-[120px]">
              <div class="h-2 bg-gray-200 dark:bg-gray-700 rounded-full overflow-hidden">
                <div
                  class="h-full bg-blue-500 transition-all duration-300"
                  style="width: ${category.percentage}%"
                  role="progressbar"
                  aria-valuenow="${category.percentage}"
                  aria-valuemin="0"
                  aria-valuemax="100"
                ></div>
              </div>
            </div>
            <span class="text-sm font-medium text-gray-700 dark:text-gray-300 whitespace-nowrap">
              ${category.percentage}%
            </span>
          </div>
        </td>
        <td class="px-4 py-4 text-sm text-gray-600 dark:text-gray-400 text-center">
          ${category.implemented} / ${category.total}
        </td>
      </tr>
    `
  }

  render(categories: ComplianceCategory[]): string {
    return `
      <div class="bg-white dark:bg-gray-800 rounded-lg shadow-lg p-6">
        <h3 class="text-xl font-semibold text-gray-900 dark:text-gray-100 mb-4">
          Category Breakdown
        </h3>
        <p class="text-sm text-gray-600 dark:text-gray-400 mb-6">
          Implementation status for each Core SQL:1999 feature category
        </p>

        <div class="overflow-x-auto">
          <table class="w-full text-left">
            <thead class="bg-gray-50 dark:bg-gray-700/50">
              <tr>
                <th class="px-4 py-3 text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">
                  Status
                </th>
                <th class="px-4 py-3 text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">
                  Category
                </th>
                <th class="px-4 py-3 text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">
                  Description
                </th>
                <th class="px-4 py-3 text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">
                  Progress
                </th>
                <th class="px-4 py-3 text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider text-center">
                  Features
                </th>
              </tr>
            </thead>
            <tbody class="bg-white dark:bg-gray-800 divide-y divide-gray-200 dark:divide-gray-700">
              ${categories.map(cat => this.renderCategoryRow(cat)).join('')}
            </tbody>
          </table>
        </div>
      </div>
    `
  }
}
