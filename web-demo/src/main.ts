// Main entry point for NIST MemSQL web demo
import './styles/main.css'
import { initTheme } from './theme'

console.log('NIST MemSQL Demo - TypeScript + Vite + WASM + Tailwind');

// Initialize theme system (dark mode support)
const theme = initTheme()
console.log(`Theme initialized: ${theme.current}`)

const app = document.getElementById('app');
if (app) {
  app.innerHTML = `
    <div class="min-h-screen bg-surface-light dark:bg-surface-dark text-gray-900 dark:text-gray-100 p-8">
      <div class="max-w-4xl mx-auto">
        <div class="flex justify-between items-center mb-8">
          <h1 class="text-4xl font-bold text-primary-light dark:text-primary-dark">NIST MemSQL Demo</h1>
          <button id="theme-toggle" class="btn-primary">
            Toggle ${theme.current === 'dark' ? '☀️ Light' : '🌙 Dark'} Mode
          </button>
        </div>

        <div class="bg-white dark:bg-gray-800 rounded-lg shadow-lg p-6 mb-6">
          <h2 class="text-2xl font-semibold mb-4">Modern Build Infrastructure</h2>
          <ul class="space-y-2">
            <li class="flex items-center">
              <span class="text-green-500 mr-2">✅</span>
              <span>Vite dev server</span>
            </li>
            <li class="flex items-center">
              <span class="text-green-500 mr-2">✅</span>
              <span>TypeScript with strict mode</span>
            </li>
            <li class="flex items-center">
              <span class="text-green-500 mr-2">✅</span>
              <span>WASM plugin configured</span>
            </li>
            <li class="flex items-center">
              <span class="text-green-500 mr-2">✅</span>
              <span>Hot Module Replacement (HMR)</span>
            </li>
            <li class="flex items-center">
              <span class="text-green-500 mr-2">✅</span>
              <span>Tailwind CSS with dark mode</span>
            </li>
          </ul>
        </div>

        <div class="bg-blue-50 dark:bg-blue-900/20 border border-blue-200 dark:border-blue-800 rounded-lg p-4">
          <p class="text-sm">
            <strong>Next steps:</strong> WASM bindings and SQL query interface
          </p>
        </div>
      </div>
    </div>
  `;

  // Setup theme toggle button
  const toggleBtn = document.getElementById('theme-toggle')
  if (toggleBtn) {
    toggleBtn.addEventListener('click', () => {
      const newTheme = theme.toggle() ? 'dark' : 'light'
      toggleBtn.textContent = `Toggle ${newTheme === 'dark' ? '☀️ Light' : '🌙 Dark'} Mode`
    })
  }
}

// Export for HMR
export {};
