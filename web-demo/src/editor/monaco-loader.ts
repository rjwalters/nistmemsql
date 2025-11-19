/**
 * Monaco Editor lazy loader
 *
 * This module handles on-demand loading of Monaco Editor to improve initial page load time.
 * Monaco is only loaded when the user first focuses on the SQL editor.
 */

// Use dynamic import type to avoid static dependency
export type Monaco = typeof import('monaco-editor')
export type MonacoEditor = import('monaco-editor').editor.IStandaloneCodeEditor

let monacoInstance: Monaco | null = null
let monacoLoadPromise: Promise<Monaco> | null = null

/**
 * Configure Monaco Environment before loading
 * This sets up web workers to avoid UI freezes
 */
function configureMonacoEnvironment(): void {
  // Configure Monaco to use web workers properly
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  ;(window as any).MonacoEnvironment = {
    getWorker(_: unknown, label: string) {
      // Create workers using dynamic imports for Vite compatibility
      const getWorkerModule = (moduleUrl: string, label: string) => {
        return new Worker(
          new URL(
            moduleUrl,
            import.meta.url
          ),
          {
            name: label,
            type: 'module'
          }
        )
      }

      switch (label) {
        case 'json':
          return getWorkerModule(
            'monaco-editor/esm/vs/language/json/json.worker?worker',
            label
          )
        case 'css':
        case 'scss':
        case 'less':
          return getWorkerModule(
            'monaco-editor/esm/vs/language/css/css.worker?worker',
            label
          )
        case 'html':
        case 'handlebars':
        case 'razor':
          return getWorkerModule(
            'monaco-editor/esm/vs/language/html/html.worker?worker',
            label
          )
        case 'typescript':
        case 'javascript':
          return getWorkerModule(
            'monaco-editor/esm/vs/language/typescript/ts.worker?worker',
            label
          )
        default:
          return getWorkerModule(
            'monaco-editor/esm/vs/editor/editor.worker?worker',
            label
          )
      }
    }
  }
}

/**
 * Lazy load Monaco Editor on first use
 * Returns cached instance on subsequent calls
 */
export async function loadMonaco(): Promise<Monaco> {
  // Return cached instance if already loaded
  if (monacoInstance) {
    return monacoInstance
  }

  // Return in-flight promise if loading
  if (monacoLoadPromise) {
    return monacoLoadPromise
  }

  // Configure Monaco environment before loading
  configureMonacoEnvironment()

  // Start loading Monaco
  console.log('[Monaco Loader] Starting Monaco Editor load...')
  const startTime = performance.now()

  monacoLoadPromise = import('monaco-editor')
    .then(m => {
      monacoInstance = m
      const loadTime = performance.now() - startTime
      console.log(`[Monaco Loader] Monaco Editor loaded in ${loadTime.toFixed(2)}ms`)
      return m
    })
    .catch(err => {
      console.error('[Monaco Loader] Failed to load Monaco Editor:', err)
      // Clear promise to allow retry
      monacoLoadPromise = null
      throw err
    })

  return monacoLoadPromise
}

/**
 * Check if Monaco is already loaded
 */
export function isMonacoLoaded(): boolean {
  return monacoInstance !== null
}
