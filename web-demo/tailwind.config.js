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
      },
    },
  },
  plugins: [],
}
