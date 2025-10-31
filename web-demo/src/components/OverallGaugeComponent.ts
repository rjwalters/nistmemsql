interface OverallData {
  percentage: number
  implemented: number
  total: number
  lastUpdated: string
}

export class OverallGaugeComponent {
  render(data: OverallData): string {
    const { percentage, implemented, total, lastUpdated } = data

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
}
