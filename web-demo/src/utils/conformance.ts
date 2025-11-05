/**
 * Utility to fetch and display conformance pass rate
 */

interface ConformanceData {
  total: number
  passed: number
  failed: number
  errors: number
  pass_rate: number
}

/**
 * Fetch the current conformance pass rate from the badge data
 */
export async function fetchConformanceRate(): Promise<number> {
  try {
    // Add cache-busting parameter to prevent CDN from serving stale 404s
    const cacheBust = Math.floor(Date.now() / 60000) // Update every minute
    const response = await fetch(`/vibesql/badges/sqltest_results.json?v=${cacheBust}`)
    if (!response.ok) {
      console.warn('Failed to load conformance data, using fallback')
      return 85.4 // Fallback value
    }
    const data = (await response.json()) as ConformanceData
    return data.pass_rate
  } catch (error) {
    console.warn('Error fetching conformance data:', error)
    return 85.4 // Fallback value
  }
}

/**
 * Update footer elements with the current conformance pass rate
 */
export async function updateConformanceFooter(): Promise<void> {
  const passRate = await fetchConformanceRate()

  // Find all footer elements that display conformance info
  const footerElements = document.querySelectorAll('footer p')

  footerElements.forEach(el => {
    const text = el.textContent || ''
    if (text.includes('sqltest conformance:')) {
      // Replace the percentage with the dynamic value
      el.textContent = text.replace(
        /sqltest conformance: [\d.]+%/,
        `sqltest conformance: ${passRate.toFixed(1)}%`
      )
    }
  })
}
