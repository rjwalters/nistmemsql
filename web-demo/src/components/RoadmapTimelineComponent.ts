import { getStatusColor, getStatusLabel, escapeHtml } from './utils'

interface RoadmapPhase {
  name: string
  percentage: number
  status: string
  description: string
}

interface RoadmapData {
  phases: RoadmapPhase[]
  estimatedCompletion: string
  currentFocus: string
}

export class RoadmapTimelineComponent {
  render(data: RoadmapData): string {
    const { phases, estimatedCompletion, currentFocus } = data

    return `
      <div class="bg-white dark:bg-gray-800 rounded-lg shadow p-6">
        <h3 class="text-xl font-semibold text-gray-900 dark:text-gray-100 mb-1">
          🗺️ Roadmap Timeline
        </h3>
        <p class="text-sm text-gray-600 dark:text-gray-400 mb-4">
          ${escapeHtml(currentFocus)} • Est. Completion: ${escapeHtml(estimatedCompletion)}
        </p>

        <div class="space-y-4">
          ${phases.map(phase => `
            <div>
              <div class="flex justify-between items-center mb-2">
                <div class="flex items-center gap-2">
                  <span class="font-medium text-sm text-gray-900 dark:text-gray-100">
                    ${escapeHtml(phase.name)}
                  </span>
                  <span class="text-xs px-2 py-1 rounded ${getStatusColor(phase.status)}">
                    ${getStatusLabel(phase.status)}
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
                  aria-label="${escapeHtml(phase.name)}: ${phase.percentage}%"
                ></div>
              </div>
              <p class="text-xs text-gray-600 dark:text-gray-400 mt-1">
                ${escapeHtml(phase.description)}
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
}
