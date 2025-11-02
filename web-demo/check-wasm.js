import { chromium } from 'playwright';

(async () => {
  const browser = await chromium.launch();
  const page = await browser.newPage();

  // Collect all network requests
  const requests = [];
  page.on('request', request => {
    requests.push({
      url: request.url(),
      type: request.resourceType()
    });
  });

  const responses = [];
  page.on('response', response => {
    responses.push({
      url: response.url(),
      status: response.status(),
      type: response.request().resourceType()
    });
  });

  await page.goto('https://rjwalters.github.io/nistmemsql/', {
    waitUntil: 'networkidle',
    timeout: 30000
  });

  await page.waitForTimeout(5000);

  console.log('\n=== WASM Files ===');
  const wasmFiles = responses.filter(r => r.url.endsWith('.wasm'));
  console.log(JSON.stringify(wasmFiles, null, 2));

  console.log('\n=== JavaScript Bundles ===');
  const jsFiles = responses.filter(r => r.type === 'script').slice(0, 10);
  console.log(JSON.stringify(jsFiles, null, 2));

  console.log('\n=== Failed Requests ===');
  const failedResponses = responses.filter(r => r.status >= 400);
  console.log(JSON.stringify(failedResponses, null, 2));

  await browser.close();
})();
