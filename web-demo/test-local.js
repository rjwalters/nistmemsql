import { chromium } from 'playwright';

(async () => {
  const browser = await chromium.launch();
  const page = await browser.newPage();

  // Listen for console messages
  page.on('console', msg => {
    console.log(`[BROWSER ${msg.type()}]:`, msg.text());
  });

  // Listen for page errors
  page.on('pageerror', error => {
    console.error('[PAGE ERROR]:', error.message);
  });

  console.log('Navigating to local dev server...');
  await page.goto('http://localhost:5173/', {
    waitUntil: 'networkidle',
    timeout: 30000
  });

  console.log('\n=== Waiting for database to initialize ===');
  await page.waitForTimeout(3000);

  // Try to click the Execute Query button
  console.log('\n=== Attempting to execute query ===');
  try {
    await page.click('#execute-btn', { timeout: 5000 });
    await page.waitForTimeout(2000);

    // Check results
    const resultsText = await page.evaluate(() => {
      const results = document.querySelector('#results');
      return results ? results.textContent : 'No results element found';
    });

    console.log('\n=== Query Results ===');
    console.log(resultsText.substring(0, 500));
  } catch (error) {
    console.log('Error executing query:', error.message);
  }

  await browser.close();
})();
