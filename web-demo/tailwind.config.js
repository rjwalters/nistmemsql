/** @type {import('tailwindcss').Config} */
export default {
  content: [
    './index.html',
    './src/**/*.{js,ts,jsx,tsx,html}',
  ],
  darkMode: 'class', // Use class strategy for manual toggle + system preference
  theme: {
    extend: {
      colors: {
        primary: {
          light: '#3b82f6',
          dark: '#60a5fa',
        },
        surface: {
          light: '#ffffff',
          dark: '#1f2937',
        },
        foreground: {
          DEFAULT: '#111827', // gray-900
          light: '#111827',
          dark: '#f9fafb', // gray-50
        },
        muted: {
          DEFAULT: '#6b7280', // gray-500
          light: '#6b7280',
          dark: '#9ca3af', // gray-400
        },
        background: {
          DEFAULT: '#ffffff',
          light: '#ffffff',
          dark: '#1f2937', // gray-800
        },
        card: {
          DEFAULT: '#ffffff',
          light: '#ffffff',
          dark: '#374151', // gray-700
        },
        border: {
          DEFAULT: '#d1d5db', // gray-300
          light: '#d1d5db',
          dark: '#4b5563', // gray-600
        },
      },
    },
  },
  plugins: [],
}
