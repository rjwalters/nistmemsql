import { chromium } from 'playwright';

(async () => {
  const browser = await chromium.launch();
  const page = await browser.newPage();

  const errors = [];
  const consoleMessages = [];

  // Listen for console messages
  page.on('console', msg => {
    consoleMessages.push(`[${msg.type()}]: ${msg.text()}`);
  });

  // Listen for page errors
  page.on('pageerror', error => {
    errors.push(`PAGE ERROR: ${error.message}`);
  });

  // Listen for network failures
  page.on('requestfailed', request => {
    errors.push(`REQUEST FAILED: ${request.url()} - ${request.failure()?.errorText}`);
  });

  console.log('Navigating to deployed site...');
  await page.goto('https://rjwalters.github.io/nistmemsql/', {
    waitUntil: 'networkidle',
    timeout: 30000
  });

  // Wait for initialization
  await page.waitForTimeout(5000);

  console.log('\n=== Console Messages ===');
  consoleMessages.forEach(msg => console.log(msg));

  console.log('\n=== Errors ===');
  if (errors.length === 0) {
    console.log('No errors detected');
  } else {
    errors.forEach(err => console.error(err));
  }

  // Check what WASM files loaded
  const wasmCheck = await page.evaluate(() => {
    const scripts = Array.from(document.scripts).map(s => s.src);
    return {
      hasWasmScript: scripts.some(s => s.includes('wasm')),
      allScripts: scripts
    };
  });

  console.log('\n=== WASM Check ===');
  console.log('Has WASM script:', wasmCheck.hasWasmScript);
  console.log('Scripts:', wasmCheck.allScripts.filter(s => s.includes('nistmemsql')));

  await browser.close();
})();
