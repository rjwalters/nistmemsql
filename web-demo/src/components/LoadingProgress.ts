/**
 * Loading progress indicator component
 * Shows visual feedback during application initialization
 */

export interface LoadingStep {
  id: string
  label: string
  progress: number // 0-100
  status: 'pending' | 'loading' | 'complete' | 'error'
  error?: string
}

export class LoadingProgressComponent {
  private container: HTMLElement
  private steps: Map<string, LoadingStep>
  private onCompleteCallback?: () => void

  constructor(containerId: string = 'loading-progress') {
    const container = document.getElementById(containerId)
    if (!container) {
      throw new Error(`Loading progress container #${containerId} not found`)
    }
    this.container = container
    this.steps = new Map()
    this.render()
  }

  /**
   * Add a loading step
   */
  addStep(id: string, label: string): void {
    this.steps.set(id, {
      id,
      label,
      progress: 0,
      status: 'pending',
    })
    this.render()
  }

  /**
   * Update step progress
   */
  updateStep(id: string, progress: number, status?: LoadingStep['status']): void {
    const step = this.steps.get(id)
    if (!step) return

    step.progress = Math.min(100, Math.max(0, progress))
    if (status) {
      step.status = status
    }
    this.render()
  }

  /**
   * Mark step as complete
   */
  completeStep(id: string): void {
    console.log(`[LoadingProgress] Completing step: ${id}`)
    this.updateStep(id, 100, 'complete')
    console.log(`[LoadingProgress] All complete? ${this.isComplete()}`)
  }

  /**
   * Mark step as error
   */
  errorStep(id: string, error: string): void {
    const step = this.steps.get(id)
    if (!step) return

    step.status = 'error'
    step.error = error
    this.render()
  }

  /**
   * Check if all steps are complete
   */
  isComplete(): boolean {
    return Array.from(this.steps.values()).every(step => step.status === 'complete')
  }

  /**
   * Set callback for when all steps complete
   */
  onComplete(callback: () => void): void {
    this.onCompleteCallback = callback
  }

  /**
   * Hide the loading indicator with fade-out animation
   */
  hide(): void {
    console.log('[LoadingProgress] Hiding loading indicator')
    this.container.style.opacity = '0'
    setTimeout(() => {
      console.log('[LoadingProgress] Setting display to none')
      this.container.style.display = 'none'
    }, 300)
  }

  /**
   * Render the loading progress UI
   */
  private render(): void {
    const steps = Array.from(this.steps.values())
    const allComplete = steps.every(step => step.status === 'complete')

    // Calculate overall progress
    const totalProgress = steps.length > 0 ? steps.reduce((sum, step) => sum + step.progress, 0) / steps.length : 0

    this.container.innerHTML = `
      <div class="loading-progress-overlay">
        <div class="loading-progress-card">
          <div class="loading-progress-header">
            <h2 class="loading-progress-title">Loading VibeSQL</h2>
            <div class="loading-progress-percent">${Math.round(totalProgress)}%</div>
          </div>

          <div class="loading-progress-bar-container">
            <div class="loading-progress-bar" style="width: ${totalProgress}%"></div>
          </div>

          <div class="loading-progress-steps">
            ${steps
              .map(
                step => `
              <div class="loading-progress-step loading-progress-step--${step.status}">
                <div class="loading-progress-step-icon">
                  ${this.getStepIcon(step.status)}
                </div>
                <div class="loading-progress-step-content">
                  <div class="loading-progress-step-label">${step.label}</div>
                  ${step.error ? `<div class="loading-progress-step-error">${step.error}</div>` : ''}
                </div>
                ${step.status === 'loading' ? '<div class="loading-progress-step-spinner"></div>' : ''}
              </div>
            `
              )
              .join('')}
          </div>
        </div>
      </div>
    `

    // Trigger complete callback if all steps done
    if (allComplete && this.onCompleteCallback) {
      console.log('[LoadingProgress] All steps complete, triggering callback in 500ms')
      setTimeout(() => {
        console.log('[LoadingProgress] Calling onComplete callback')
        this.onCompleteCallback?.()
      }, 500)
    }
  }

  private getStepIcon(status: LoadingStep['status']): string {
    switch (status) {
      case 'complete':
        return `<svg class="w-5 h-5 text-green-500" fill="none" stroke="currentColor" viewBox="0 0 24 24">
          <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M5 13l4 4L19 7"></path>
        </svg>`
      case 'error':
        return `<svg class="w-5 h-5 text-red-500" fill="none" stroke="currentColor" viewBox="0 0 24 24">
          <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12"></path>
        </svg>`
      case 'loading':
        return `<svg class="w-5 h-5 text-blue-500" fill="none" stroke="currentColor" viewBox="0 0 24 24">
          <circle class="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" stroke-width="4"></circle>
          <path class="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z"></path>
        </svg>`
      default:
        return `<svg class="w-5 h-5 text-gray-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
          <circle cx="12" cy="12" r="10" stroke="currentColor" stroke-width="2"></circle>
        </svg>`
    }
  }
}
