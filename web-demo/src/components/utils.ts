// Utility functions for the Compliance Dashboard

export function getStatusColor(status: string): string {
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

export function getStatusLabel(status: string): string {
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

export function escapeHtml(text: string): string {
  const div = document.createElement('div')
  div.textContent = text
  return div.innerHTML
}

export function getFeatureStatusIcon(status: string): string {
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

export function getFeatureStatusColor(status: string): string {
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
