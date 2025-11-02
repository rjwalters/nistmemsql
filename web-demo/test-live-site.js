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
    console.error('[STACK]:', error.stack);
  });

  // Listen for network failures
  page.on('requestfailed', request => {
    console.error('[REQUEST FAILED]:', request.url(), request.failure()?.errorText);
  });

  console.log('Navigating to page...');
  await page.goto('https://rjwalters.github.io/nistmemsql/', {
    waitUntil: 'networkidle',
    timeout: 30000
  });

  console.log('\n=== Checking for critical resources ===');

  // Check what JS files are loaded
  const scripts = await page.evaluate(() => {
    return Array.from(document.scripts).map(s => ({
      src: s.src,
      inline: !s.src
    }));
  });

  console.log('Script tags:', JSON.stringify(scripts, null, 2));

  // Wait a bit for the app to initialize
  await page.waitForTimeout(3000);

  // Try to click the Execute Query button
  console.log('\n=== Attempting to execute query ===');
  try {
    await page.click('#execute-btn', { timeout: 5000 });
    await page.waitForTimeout(2000);
  } catch (error) {
    console.log('Could not click execute button:', error.message);
  }

  // Check if database is initialized
  const dbStatus = await page.evaluate(() => {
    return {
      hasDatabase: typeof window.database !== 'undefined',
      hasQuery: window.database && typeof window.database.query === 'function',
      windowKeys: Object.keys(window).filter(k => k.includes('db') || k.includes('Database'))
    };
  });

  console.log('\n=== Database status ===');
  console.log(JSON.stringify(dbStatus, null, 2));

  await browser.close();
})();
