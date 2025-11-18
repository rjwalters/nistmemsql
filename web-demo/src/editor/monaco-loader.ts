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

  // Start loading Monaco
  console.log('[Monaco Loader] Starting Monaco Editor load...')
  const startTime = performance.now()

  monacoLoadPromise = import('monaco-editor').then(m => {
    monacoInstance = m
    const loadTime = performance.now() - startTime
    console.log(`[Monaco Loader] Monaco Editor loaded in ${loadTime.toFixed(2)}ms`)
    return m
  })

  return monacoLoadPromise
}

/**
 * Check if Monaco is already loaded
 */
export function isMonacoLoaded(): boolean {
  return monacoInstance !== null
}
