/**
 * Utility functions for rendering conformance report UI
 */

/**
 * Get status color based on pass rate
 */
export function getStatusColor(passRate: number): string {
  if (passRate >= 80) return '#10b981' // green
  if (passRate >= 60) return '#84cc16' // lime
  if (passRate >= 40) return '#eab308' // yellow
  if (passRate >= 20) return '#f97316' // orange
  return '#ef4444' // red
}

/**
 * Get status text based on pass rate
 */
export function getStatusText(passRate: number): string {
  if (passRate >= 80) return 'Excellent'
  if (passRate >= 60) return 'Good'
  if (passRate >= 40) return 'Fair'
  if (passRate >= 20) return 'Poor'
  return 'Needs Work'
}

/**
 * Escape HTML special characters
 */
export function escapeHtml(str: string): string {
  const div = document.createElement('div')
  div.textContent = str
  return div.innerHTML
}
