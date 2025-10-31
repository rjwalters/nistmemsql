import { getFeatureStatusIcon, getFeatureStatusColor, escapeHtml } from './utils'
import type { FeatureNode } from './ComplianceDashboard'
import type { ComplianceDashboardState } from './ComplianceDashboardState'

export class FeatureMatrixComponent {
  constructor(private state: ComplianceDashboardState) {}

  private renderFeatureNode(node: FeatureNode, depth: number = 0): string {
    if (!this.state.shouldShowNode(node)) {
      return ''
    }

    const hasChildren = node.children && node.children.length > 0
    const isExpanded = this.state.isNodeExpanded(node.id)
    const icon = getFeatureStatusIcon(node.status)
    const colorClass = getFeatureStatusColor(node.status)
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
              aria-label="${isExpanded ? 'Collapse' : 'Expand'} ${escapeHtml(node.name)}"
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
            ${escapeHtml(node.name)}
          </span>
          ${
            node.docLink
              ? `
            <a
              href="${node.docLink}"
              target="_blank"
              rel="noopener noreferrer"
              class="text-blue-600 dark:text-blue-400 hover:text-blue-800 dark:hover:text-blue-300 opacity-0 group-hover:opacity-100 transition-opacity"
              aria-label="Documentation for ${escapeHtml(node.name)}"
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

  render(features: FeatureNode[]): string {
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
                     ${this.state.getStatusFilter() === null ? 'bg-blue-500 text-white' : 'bg-gray-200 dark:bg-gray-700 text-gray-700 dark:text-gray-300 hover:bg-gray-300 dark:hover:bg-gray-600'}"
              onclick="window.complianceDashboard?.handleFilter(null)"
            >
              All
            </button>
            <button
              class="px-4 py-2 rounded-lg transition-colors focus:ring-2 focus:ring-green-500
                     ${this.state.getStatusFilter() === 'implemented' ? 'bg-green-500 text-white' : 'bg-gray-200 dark:bg-gray-700 text-gray-700 dark:text-gray-300 hover:bg-gray-300 dark:hover:bg-gray-600'}"
              onclick="window.complianceDashboard?.handleFilter('implemented')"
            >
              ✅ Implemented
            </button>
            <button
              class="px-4 py-2 rounded-lg transition-colors focus:ring-2 focus:ring-blue-500
                     ${this.state.getStatusFilter() === 'planned' ? 'bg-blue-500 text-white' : 'bg-gray-200 dark:bg-gray-700 text-gray-700 dark:text-gray-300 hover:bg-gray-300 dark:hover:bg-gray-600'}"
              onclick="window.complianceDashboard?.handleFilter('planned')"
            >
              🔵 Planned
            </button>
            <button
              class="px-4 py-2 rounded-lg transition-colors focus:ring-2 focus:ring-gray-500
                     ${this.state.getStatusFilter() === 'not-started' ? 'bg-gray-500 text-white' : 'bg-gray-200 dark:bg-gray-700 text-gray-700 dark:text-gray-300 hover:bg-gray-300 dark:hover:bg-gray-600'}"
              onclick="window.complianceDashboard?.handleFilter('not-started')"
            >
              ⚪ Not Started
            </button>
          </div>
        </div>

        <!-- Feature Tree -->
        <div class="space-y-1" role="tree" aria-label="SQL Feature Implementation Tree">
          ${features.map(feature => this.renderFeatureNode(feature, 0)).join('')}
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
}
