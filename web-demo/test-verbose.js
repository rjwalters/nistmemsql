import { chromium } from 'playwright';

(async () => {
  const browser = await chromium.launch();
  const page = await browser.newPage();

  // Capture ALL console output
  page.on('console', msg => {
    const type = msg.type();
    const text = msg.text();
    console.log(`[CONSOLE ${type.toUpperCase()}]:`, text);
  });

  page.on('pageerror', error => {
    console.error('[PAGE ERROR]:', error.message);
    console.error(error.stack);
  });

  page.on('requestfailed', request => {
    console.error('[FAILED]:', request.url(), request.failure()?.errorText);
  });

  console.log('Loading page...\n');

  try {
    await page.goto('https://rjwalters.github.io/nistmemsql/', {
      waitUntil: 'networkidle',
      timeout: 30000
    });

    console.log('\n=== Waiting 10 seconds for initialization ===\n');
    await page.waitForTimeout(10000);

    // Try to get error from page context
    const pageState = await page.evaluate(() => {
      return {
        hasMonaco: typeof window.monaco !== 'undefined',
        hasRequire: typeof window.require !== 'undefined',
        locationHref: window.location.href
      };
    });

    console.log('\n=== Page State ===');
    console.log(JSON.stringify(pageState, null, 2));

  } catch (error) {
    console.error('Error:', error.message);
  }

  await page.waitForTimeout(5000);
  await browser.close();
})();
