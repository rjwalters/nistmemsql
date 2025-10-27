// Main entry point for NIST MemSQL web demo

const app = document.getElementById('app')
if (app) {
  app.innerHTML = `
    <h1>NIST MemSQL Demo</h1>
    <p>Modern build infrastructure initialized successfully!</p>
    <ul>
      <li>✅ Vite dev server</li>
      <li>✅ TypeScript with strict mode</li>
      <li>✅ WASM plugin configured</li>
      <li>✅ Hot Module Replacement (HMR)</li>
    </ul>
    <p><em>Ready for WASM bindings and SQL query interface.</em></p>
  `
}

// Export for HMR
export {}
