interface ComplianceCategory {
  name: string
  percentage: number
  implemented: number
  total: number
  status: string
  description: string
}

interface FeatureNode {
  id: string
  name: string
  status: 'implemented' | 'planned' | 'not-started'
  children?: FeatureNode[]
  docLink?: string
}

interface TestStatistics {
  total: number
  passing: number
  coverage: number
  linesOfCode: number
  byModule: Record<string, number>
}

interface RoadmapPhase {
  name: string
  percentage: number
  status: string
  description: string
}

interface RecentUpdate {
  date: string
  feature: string
  tests: number
  pr: number
}

interface DatabaseComparison {
  database: string
  percentage: number
  target?: number
  notes: string
}

interface ComplianceData {
  overall: {
    percentage: number
    implemented: number
    total: number
    lastUpdated: string
  }
  categories: ComplianceCategory[]
  features?: FeatureNode[]
  testStatistics?: TestStatistics
  roadmap?: {
    phases: RoadmapPhase[]
    estimatedCompletion: string
    currentFocus: string
  }
  recentUpdates?: RecentUpdate[]
  comparison?: DatabaseComparison[]
}

export class ComplianceDashboard {
  private root: HTMLElement
  private data: ComplianceData | null = null
  private searchQuery: string = ''
  private statusFilter: string | null = null
  private expandedNodes: Set<string> = new Set()

  constructor() {
    this.root = document.createElement('div')
    this.root.className = 'compliance-dashboard'
    this.loadData()

    // Initialize all nodes as expanded
    this.expandedNodes.add('all')
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

  private getFeatureStatusIcon(status: string): string {
    switch (status) {
      case 'implemented':
        return '✅'
      case 'planned':
        return '🔵'
      case 'not-started':
        return '⚪'
      default:
        return '❓'
    }
  }

  private getFeatureStatusColor(status: string): string {
    switch (status) {
      case 'implemented':
        return 'text-green-600 dark:text-green-400'
      case 'planned':
        return 'text-blue-600 dark:text-blue-400'
      case 'not-started':
        return 'text-gray-400 dark:text-gray-600'
      default:
        return 'text-gray-400'
    }
  }

  private shouldShowNode(node: FeatureNode): boolean {
    // Filter by status
    if (this.statusFilter && node.status !== this.statusFilter) {
      // Check if any children match
      if (node.children) {
        return node.children.some(child => this.shouldShowNode(child))
      }
      return false
    }

    // Filter by search query
    if (
      this.searchQuery &&
      !node.name.toLowerCase().includes(this.searchQuery.toLowerCase())
    ) {
      // Check if any children match
      if (node.children) {
        return node.children.some(child => this.shouldShowNode(child))
      }
      return false
    }

    return true
  }

  private toggleNodeState(nodeId: string): void {
    if (this.expandedNodes.has(nodeId)) {
      this.expandedNodes.delete(nodeId)
    } else {
      this.expandedNodes.add(nodeId)
    }
    this.render()
  }

  private renderFeatureNode(node: FeatureNode, depth: number = 0): string {
    if (!this.shouldShowNode(node)) {
      return ''
    }

    const hasChildren = node.children && node.children.length > 0
    const isExpanded = this.expandedNodes.has(node.id) || this.expandedNodes.has('all')
    const icon = this.getFeatureStatusIcon(node.status)
    const colorClass = this.getFeatureStatusColor(node.status)
    const indentClass = depth > 0 ? `ml-${depth * 4}` : ''

    const html = `
      <div class="${indentClass} py-2 border-b border-gray-100 dark:border-gray-700/50 last:border-0">
        <div class="flex items-center gap-2 group">
          ${
            hasChildren
              ? `
            <button
              class="text-gray-500 hover:text-gray-700 dark:text-gray-400 dark:hover:text-gray-200 transition-colors focus:outline-none"
              onclick="window.complianceDashboard?.toggleNode('${node.id}')"
              aria-expanded="${isExpanded}"
              aria-label="${isExpanded ? 'Collapse' : 'Expand'} ${this.escapeHtml(node.name)}"
            >
              <span class="inline-block w-4 text-center transform transition-transform ${isExpanded ? 'rotate-90' : ''}">
                ▶
              </span>
            </button>
          `
              : '<span class="w-4"></span>'
          }
          <span class="text-lg" role="img" aria-label="${node.status}">${icon}</span>
          <span class="font-medium text-gray-900 dark:text-gray-100 ${colorClass}">
            ${this.escapeHtml(node.name)}
          </span>
          ${
            node.docLink
              ? `
            <a
              href="${node.docLink}"
              target="_blank"
              rel="noopener noreferrer"
              class="text-blue-600 dark:text-blue-400 hover:text-blue-800 dark:hover:text-blue-300 opacity-0 group-hover:opacity-100 transition-opacity"
              aria-label="Documentation for ${this.escapeHtml(node.name)}"
            >
              📖
            </a>
          `
              : ''
          }
        </div>
        ${
          hasChildren && isExpanded
            ? `
          <div class="mt-1">
            ${node.children!.map(child => this.renderFeatureNode(child, depth + 1)).join('')}
          </div>
        `
            : ''
        }
      </div>
    `
    return html
  }

  private handleSearchInput(event: Event): void {
    const input = event.target as HTMLInputElement
    this.searchQuery = input.value
    this.render()
  }

  private handleStatusFilter(status: string | null): void {
    this.statusFilter = status
    this.render()
  }

  private renderFeatureMatrix(): string {
    if (!this.data || !this.data.features) return ''

    return `
      <div class="bg-white dark:bg-gray-800 rounded-lg shadow-lg p-6">
        <h3 class="text-xl font-semibold text-gray-900 dark:text-gray-100 mb-4">
          Feature Implementation Matrix
        </h3>
        <p class="text-sm text-gray-600 dark:text-gray-400 mb-6">
          Explore implemented SQL features with hierarchical organization
        </p>

        <!-- Search and Filter Controls -->
        <div class="flex flex-col md:flex-row gap-3 mb-6">
          <div class="flex-1">
            <input
              type="text"
              placeholder="Search features..."
              class="w-full px-4 py-2 border border-gray-300 dark:border-gray-600 rounded-lg
                     bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100
                     placeholder-gray-500 dark:placeholder-gray-400
                     focus:ring-2 focus:ring-blue-500 focus:border-transparent"
              oninput="window.complianceDashboard?.handleSearch(event)"
              aria-label="Search features"
            />
          </div>
          <div class="flex gap-2">
            <button
              class="px-4 py-2 rounded-lg transition-colors focus:ring-2 focus:ring-blue-500
                     ${this.statusFilter === null ? 'bg-blue-500 text-white' : 'bg-gray-200 dark:bg-gray-700 text-gray-700 dark:text-gray-300 hover:bg-gray-300 dark:hover:bg-gray-600'}"
              onclick="window.complianceDashboard?.handleFilter(null)"
            >
              All
            </button>
            <button
              class="px-4 py-2 rounded-lg transition-colors focus:ring-2 focus:ring-green-500
                     ${this.statusFilter === 'implemented' ? 'bg-green-500 text-white' : 'bg-gray-200 dark:bg-gray-700 text-gray-700 dark:text-gray-300 hover:bg-gray-300 dark:hover:bg-gray-600'}"
              onclick="window.complianceDashboard?.handleFilter('implemented')"
            >
              ✅ Implemented
            </button>
            <button
              class="px-4 py-2 rounded-lg transition-colors focus:ring-2 focus:ring-blue-500
                     ${this.statusFilter === 'planned' ? 'bg-blue-500 text-white' : 'bg-gray-200 dark:bg-gray-700 text-gray-700 dark:text-gray-300 hover:bg-gray-300 dark:hover:bg-gray-600'}"
              onclick="window.complianceDashboard?.handleFilter('planned')"
            >
              🔵 Planned
            </button>
            <button
              class="px-4 py-2 rounded-lg transition-colors focus:ring-2 focus:ring-gray-500
                     ${this.statusFilter === 'not-started' ? 'bg-gray-500 text-white' : 'bg-gray-200 dark:bg-gray-700 text-gray-700 dark:text-gray-300 hover:bg-gray-300 dark:hover:bg-gray-600'}"
              onclick="window.complianceDashboard?.handleFilter('not-started')"
            >
              ⚪ Not Started
            </button>
          </div>
        </div>

        <!-- Feature Tree -->
        <div class="space-y-1" role="tree" aria-label="SQL Feature Implementation Tree">
          ${this.data.features.map(feature => this.renderFeatureNode(feature, 0)).join('')}
        </div>

        <!-- Legend -->
        <div class="mt-6 pt-4 border-t border-gray-200 dark:border-gray-700">
          <div class="flex flex-wrap gap-4 text-sm text-gray-600 dark:text-gray-400">
            <div class="flex items-center gap-2">
              <span>✅</span>
              <span>Implemented</span>
            </div>
            <div class="flex items-center gap-2">
              <span>🔵</span>
              <span>Planned</span>
            </div>
            <div class="flex items-center gap-2">
              <span>⚪</span>
              <span>Not Started</span>
            </div>
            <div class="flex items-center gap-2">
              <span>📖</span>
              <span>Documentation Available</span>
            </div>
          </div>
        </div>
      </div>
    `
  }

  private renderTestStatistics(): string {
    if (!this.data || !this.data.testStatistics) return ''

    const stats = this.data.testStatistics
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
                  <span class="text-sm font-medium text-gray-700 dark:text-gray-300">${this.escapeHtml(module)}</span>
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
                    aria-label="Coverage for ${this.escapeHtml(module)}: ${coverage}%"
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

  private renderRoadmapTimeline(): string {
    if (!this.data?.roadmap) return ''

    const { phases, estimatedCompletion, currentFocus } = this.data.roadmap

    return `
      <div class="bg-white dark:bg-gray-800 rounded-lg shadow p-6">
        <h3 class="text-xl font-semibold text-gray-900 dark:text-gray-100 mb-1">
          🗺️ Roadmap Timeline
        </h3>
        <p class="text-sm text-gray-600 dark:text-gray-400 mb-4">
          ${this.escapeHtml(currentFocus)} • Est. Completion: ${this.escapeHtml(estimatedCompletion)}
        </p>

        <div class="space-y-4">
          ${phases.map(phase => `
            <div>
              <div class="flex justify-between items-center mb-2">
                <div class="flex items-center gap-2">
                  <span class="font-medium text-sm text-gray-900 dark:text-gray-100">
                    ${this.escapeHtml(phase.name)}
                  </span>
                  <span class="text-xs px-2 py-1 rounded ${this.getStatusColor(phase.status)}">
                    ${this.getStatusLabel(phase.status)}
                  </span>
                </div>
                <span class="text-sm font-semibold text-gray-900 dark:text-gray-100">
                  ${phase.percentage}%
                </span>
              </div>
              <div class="h-2 bg-gray-200 dark:bg-gray-700 rounded-full overflow-hidden">
                <div
                  class="h-full ${phase.percentage === 100 ? 'bg-green-500' : phase.percentage > 0 ? 'bg-blue-500' : 'bg-gray-300 dark:bg-gray-600'} transition-all duration-500"
                  style="width: ${phase.percentage}%"
                  role="progressbar"
                  aria-valuenow="${phase.percentage}"
                  aria-valuemin="0"
                  aria-valuemax="100"
                  aria-label="${this.escapeHtml(phase.name)}: ${phase.percentage}%"
                ></div>
              </div>
              <p class="text-xs text-gray-600 dark:text-gray-400 mt-1">
                ${this.escapeHtml(phase.description)}
              </p>
            </div>
          `).join('')}
        </div>

        <div class="mt-4 pt-4 border-t border-gray-200 dark:border-gray-700">
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

  private renderRecentUpdates(): string {
    if (!this.data?.recentUpdates) return ''

    return `
      <div class="bg-white dark:bg-gray-800 rounded-lg shadow p-6">
        <h3 class="text-xl font-semibold text-gray-900 dark:text-gray-100 mb-4">
          📰 Recent Updates
        </h3>

        <div class="space-y-3">
          ${this.data.recentUpdates.map(update => {
            const date = new Date(update.date)
            const formattedDate = date.toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' })

            return `
              <div class="flex items-start gap-3 pb-3 border-b border-gray-200 dark:border-gray-700 last:border-0 last:pb-0">
                <div class="flex-shrink-0 w-20 text-sm text-gray-600 dark:text-gray-400">
                  ${formattedDate}
                </div>
                <div class="flex-1">
                  <div class="font-medium text-gray-900 dark:text-gray-100 text-sm">
                    ${this.escapeHtml(update.feature)}
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

  private renderDatabaseComparison(): string {
    if (!this.data?.comparison) return ''

    const maxPercentage = Math.max(...this.data.comparison.map(db => db.percentage))

    return `
      <div class="bg-white dark:bg-gray-800 rounded-lg shadow p-6">
        <h3 class="text-xl font-semibold text-gray-900 dark:text-gray-100 mb-1">
          📊 SQL:1999 Core Compliance Comparison
        </h3>
        <p class="text-sm text-gray-600 dark:text-gray-400 mb-6">
          Approximate compliance estimates based on published documentation
        </p>

        <div class="space-y-4">
          ${this.data.comparison.map(db => {
            const isNistMemSQL = db.database === 'nistmemsql'
            const displayPercentage = isNistMemSQL && db.target ? db.target : db.percentage
            const progressPercentage = db.percentage

            return `
              <div>
                <div class="flex justify-between items-center mb-2">
                  <div class="flex items-center gap-2">
                    <span class="font-medium text-sm text-gray-900 dark:text-gray-100 ${isNistMemSQL ? 'font-bold' : ''}">
                      ${this.escapeHtml(db.database)}
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
                        aria-label="${this.escapeHtml(db.database)} current: ${progressPercentage}%"
                      ></div>
                      <div
                        class="h-full border-2 border-blue-300 dark:border-blue-600 bg-transparent absolute"
                        style="width: ${displayPercentage}%"
                        aria-label="${this.escapeHtml(db.database)} target: ${displayPercentage}%"
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
                      aria-label="${this.escapeHtml(db.database)}: ${displayPercentage}%"
                    ></div>
                  `}
                </div>
                <p class="text-xs text-gray-600 dark:text-gray-400 mt-1">
                  ${this.escapeHtml(db.notes)}
                </p>
              </div>
            `
          }).join('')}
        </div>
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
        ${this.renderRoadmapTimeline()}
        ${this.renderRecentUpdates()}
        ${this.renderDatabaseComparison()}
        ${this.renderFeatureMatrix()}
        ${this.renderTestStatistics()}
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

  // Public methods for window callbacks
  public handleSearch(event: Event): void {
    this.handleSearchInput(event)
  }

  public handleFilter(status: 'implemented' | 'planned' | 'not-started' | null): void {
    this.handleStatusFilter(status)
  }

  public toggleNode(nodeId: string): void {
    this.toggleNodeState(nodeId)
  }
}
