// Import Tailwind CSS
import './styles/main.css';

// Main entry point for NIST MemSQL web demo
console.log('NIST MemSQL Demo - TypeScript + Vite + WASM');

// Dark mode management
type Theme = 'light' | 'dark';

function getSystemTheme(): Theme {
  return window.matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'light';
}

function getSavedTheme(): Theme | null {
  const saved = localStorage.getItem('theme');
  return saved === 'light' || saved === 'dark' ? saved : null;
}

function applyTheme(theme: Theme): void {
  if (theme === 'dark') {
    document.documentElement.classList.add('dark');
  } else {
    document.documentElement.classList.remove('dark');
  }
  localStorage.setItem('theme', theme);
}

function toggleTheme(): void {
  const currentTheme = document.documentElement.classList.contains('dark') ? 'dark' : 'light';
  const newTheme: Theme = currentTheme === 'dark' ? 'light' : 'dark';
  applyTheme(newTheme);
}

// Initialize theme on load
const initialTheme = getSavedTheme() ?? getSystemTheme();
applyTheme(initialTheme);

// Listen for system theme changes
window.matchMedia('(prefers-color-scheme: dark)').addEventListener('change', (e) => {
  if (!getSavedTheme()) {
    applyTheme(e.matches ? 'dark' : 'light');
  }
});

// Render UI
const app = document.getElementById('app');
if (app) {
  app.innerHTML = `
    <div class="min-h-screen p-8">
      <div class="max-w-4xl mx-auto">
        <div class="flex justify-between items-center mb-8">
          <h1 class="text-4xl font-bold text-blue-600 dark:text-blue-400">
            NIST MemSQL Demo
          </h1>
          <button
            id="theme-toggle"
            class="btn-primary"
            aria-label="Toggle dark mode"
          >
            <span class="dark:hidden">🌙 Dark</span>
            <span class="hidden dark:inline">☀️ Light</span>
          </button>
        </div>

        <div class="bg-white dark:bg-gray-800 rounded-lg shadow-lg p-6 mb-6">
          <h2 class="text-2xl font-semibold mb-4">Modern Build Infrastructure</h2>
          <p class="mb-4 text-gray-700 dark:text-gray-300">
            Development environment initialized successfully with dark mode support!
          </p>
          <ul class="space-y-2">
            <li class="flex items-center text-gray-800 dark:text-gray-200">
              <span class="mr-2">✅</span> Vite dev server
            </li>
            <li class="flex items-center text-gray-800 dark:text-gray-200">
              <span class="mr-2">✅</span> TypeScript with strict mode
            </li>
            <li class="flex items-center text-gray-800 dark:text-gray-200">
              <span class="mr-2">✅</span> WASM plugin configured
            </li>
            <li class="flex items-center text-gray-800 dark:text-gray-200">
              <span class="mr-2">✅</span> Hot Module Replacement (HMR)
            </li>
            <li class="flex items-center text-gray-800 dark:text-gray-200">
              <span class="mr-2">✅</span> Tailwind CSS with dark mode
            </li>
          </ul>
        </div>

        <div class="bg-white dark:bg-gray-800 rounded-lg shadow-lg p-6">
          <p class="text-gray-600 dark:text-gray-400 italic">
            Ready for WASM bindings and SQL query interface.
          </p>
        </div>
      </div>
    </div>
  `;

  // Attach theme toggle listener
  const toggleButton = document.getElementById('theme-toggle');
  if (toggleButton) {
    toggleButton.addEventListener('click', toggleTheme);
  }
}

// Export for HMR
export {};
