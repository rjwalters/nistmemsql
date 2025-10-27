interface ComplianceCategory {
  name: string
  percentage: number
  implemented: number
  total: number
  status: string
  description: string
}

interface ComplianceData {
  overall: {
    percentage: number
    implemented: number
    total: number
    lastUpdated: string
  }
  categories: ComplianceCategory[]
}

export class ComplianceDashboard {
  private root: HTMLElement
  private data: ComplianceData | null = null

  constructor() {
    this.root = document.createElement('div')
    this.root.className = 'compliance-dashboard'
    this.loadData()
  }

  private async loadData(): Promise<void> {
    try {
      const response = await fetch('/data/compliance.json')
      this.data = await response.json()
      this.render()
    } catch (error) {
      console.error('Failed to load compliance data:', error)
      this.renderError()
    }
  }

  private getStatusColor(status: string): string {
    switch (status) {
      case 'complete':
        return 'bg-green-100 text-green-800 dark:bg-green-900/30 dark:text-green-400'
      case 'advanced':
        return 'bg-blue-100 text-blue-800 dark:bg-blue-900/30 dark:text-blue-400'
      case 'in-progress':
        return 'bg-yellow-100 text-yellow-800 dark:bg-yellow-900/30 dark:text-yellow-400'
      case 'early':
        return 'bg-orange-100 text-orange-800 dark:bg-orange-900/30 dark:text-orange-400'
      case 'not-started':
        return 'bg-gray-100 text-gray-800 dark:bg-gray-700 dark:text-gray-400'
      default:
        return 'bg-gray-100 text-gray-800 dark:bg-gray-700 dark:text-gray-400'
    }
  }

  private getStatusLabel(status: string): string {
    switch (status) {
      case 'complete':
        return '✅ Complete'
      case 'advanced':
        return '🟢 Advanced'
      case 'in-progress':
        return '🟡 In Progress'
      case 'early':
        return '🔴 Early'
      case 'not-started':
        return '⚪ Not Started'
      default:
        return status
    }
  }

  private renderOverallGauge(): string {
    if (!this.data) return ''

    const { percentage, implemented, total, lastUpdated } = this.data.overall

    return `
      <div class="bg-white dark:bg-gray-800 rounded-lg shadow-lg p-6">
        <h2 class="text-2xl font-bold text-gray-900 dark:text-gray-100 mb-2">
          Core SQL:1999 Compliance
        </h2>
        <p class="text-sm text-gray-600 dark:text-gray-400 mb-6">
          Track implementation progress toward full Core SQL:1999 specification compliance
        </p>

        <div class="flex flex-col items-center mb-6">
          <!-- Circular Progress Gauge -->
          <div class="relative w-40 h-40 mb-4">
            <svg class="transform -rotate-90 w-40 h-40">
              <circle
                cx="80"
                cy="80"
                r="70"
                stroke="currentColor"
                stroke-width="12"
                fill="transparent"
                class="text-gray-200 dark:text-gray-700"
              />
              <circle
                cx="80"
                cy="80"
                r="70"
                stroke="currentColor"
                stroke-width="12"
                fill="transparent"
                stroke-dasharray="${2 * Math.PI * 70}"
                stroke-dashoffset="${2 * Math.PI * 70 * (1 - percentage / 100)}"
                class="text-blue-500 transition-all duration-1000"
                stroke-linecap="round"
              />
            </svg>
            <div class="absolute inset-0 flex items-center justify-center">
              <div class="text-center">
                <div class="text-4xl font-bold text-gray-900 dark:text-gray-100">
                  ${percentage}%
                </div>
                <div class="text-xs text-gray-600 dark:text-gray-400">
                  Complete
                </div>
              </div>
            </div>
          </div>

          <!-- Feature Count -->
          <div class="text-center">
            <div class="text-2xl font-semibold text-gray-700 dark:text-gray-300">
              ${implemented} of ${total} features
            </div>
            <div class="text-sm text-gray-500 dark:text-gray-500 mt-1">
              Last updated: ${lastUpdated}
            </div>
          </div>
        </div>
      </div>
    `
  }

  private renderCategoryRow(category: ComplianceCategory): string {
    const statusColor = this.getStatusColor(category.status)
    const statusLabel = this.getStatusLabel(category.status)

    return `
      <tr class="hover:bg-gray-50 dark:hover:bg-gray-700/50 transition-colors">
        <td class="px-4 py-4">
          <span class="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium ${statusColor}">
            ${statusLabel}
          </span>
        </td>
        <td class="px-4 py-4 font-medium text-gray-900 dark:text-gray-100">
          ${this.escapeHtml(category.name)}
        </td>
        <td class="px-4 py-4 text-sm text-gray-600 dark:text-gray-400">
          ${this.escapeHtml(category.description)}
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

  private renderCategoryBreakdown(): string {
    if (!this.data) return ''

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
              ${this.data.categories.map(cat => this.renderCategoryRow(cat)).join('')}
            </tbody>
          </table>
        </div>
      </div>
    `
  }

  private escapeHtml(text: string): string {
    const div = document.createElement('div')
    div.textContent = text
    return div.innerHTML
  }

  private renderError(): void {
    this.root.innerHTML = `
      <div class="bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-800 rounded-lg p-6">
        <h3 class="text-lg font-semibold text-red-800 dark:text-red-400 mb-2">
          Failed to Load Compliance Data
        </h3>
        <p class="text-sm text-red-700 dark:text-red-500">
          Could not load compliance metrics. Please check that compliance.json is available.
        </p>
      </div>
    `
  }

  private render(): void {
    if (!this.data) {
      this.root.innerHTML = `
        <div class="flex items-center justify-center p-12">
          <div class="text-center">
            <div class="animate-spin rounded-full h-12 w-12 border-b-2 border-blue-500 mx-auto mb-4"></div>
            <p class="text-gray-600 dark:text-gray-400">Loading compliance data...</p>
          </div>
        </div>
      `
      return
    }

    this.root.innerHTML = `
      <div class="space-y-6">
        ${this.renderOverallGauge()}
        ${this.renderCategoryBreakdown()}

        <!-- Link to Roadmap -->
        <div class="bg-blue-50 dark:bg-blue-900/20 border border-blue-200 dark:border-blue-800 rounded-lg p-6">
          <h3 class="text-lg font-semibold text-blue-900 dark:text-blue-300 mb-2">
            📋 Implementation Roadmap
          </h3>
          <p class="text-sm text-blue-800 dark:text-blue-400 mb-3">
            For detailed implementation timeline and completion estimates, see the WORK_PLAN.md document.
          </p>
          <a
            href="https://github.com/rjwalters/nistmemsql/blob/main/WORK_PLAN.md"
            target="_blank"
            rel="noopener noreferrer"
            class="inline-flex items-center text-sm font-medium text-blue-600 dark:text-blue-400 hover:text-blue-800 dark:hover:text-blue-300 transition-colors"
          >
            View Full Roadmap →
          </a>
        </div>
      </div>
    `
  }

  public mount(parent: HTMLElement): void {
    parent.appendChild(this.root)
  }

  public unmount(): void {
    this.root.remove()
  }

  public getRoot(): HTMLElement {
    return this.root
  }
}
