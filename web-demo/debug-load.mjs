#!/usr/bin/env node
import { chromium } from 'playwright';

const URL = 'http://localhost:5173/vibesql/';
const TIMEOUT = 30000; // 30 seconds

console.log('🔍 Starting headless browser debug session...\n');

const browser = await chromium.launch({ headless: true });
const context = await browser.newContext();
const page = await context.newPage();

// Capture console logs
const logs = [];
page.on('console', msg => {
  const text = msg.text();
  const type = msg.type();
  logs.push({ type, text, timestamp: new Date().toISOString() });

  // Use emoji for different log types
  const emoji = {
    'log': '📝',
    'info': 'ℹ️',
    'warn': '⚠️',
    'error': '❌',
    'debug': '🐛'
  }[type] || '📋';

  console.log(`${emoji} [${type}] ${text}`);
});

// Capture page errors
page.on('pageerror', error => {
  console.error('❌ [PAGE ERROR]', error.message);
  logs.push({ type: 'pageerror', text: error.message, stack: error.stack });
});

// Capture failed requests
page.on('requestfailed', request => {
  console.error('❌ [REQUEST FAILED]', request.url(), request.failure()?.errorText);
});

try {
  console.log(`🌐 Navigating to ${URL}...\n`);
  await page.goto(URL, { waitUntil: 'domcontentloaded', timeout: TIMEOUT });

  console.log('\n⏳ Waiting for app to load (checking for loading indicator to disappear)...\n');

  // Check if loading progress exists
  const loadingExists = await page.locator('#loading-progress').count() > 0;
  console.log(`📊 Loading progress element exists: ${loadingExists}`);

  if (loadingExists) {
    // Wait for loading indicator to be hidden (display: none or opacity: 0)
    try {
      await page.waitForFunction(
        () => {
          const el = document.getElementById('loading-progress');
          if (!el) return true;
          const style = window.getComputedStyle(el);
          return style.display === 'none' || style.opacity === '0';
        },
        { timeout: TIMEOUT }
      );
      console.log('\n✅ Loading indicator disappeared - app loaded successfully!\n');
    } catch (e) {
      console.log('\n⏱️ Timeout waiting for loading indicator to disappear\n');

      // Get current state
      const loadingState = await page.evaluate(() => {
        const el = document.getElementById('loading-progress');
        if (!el) return { exists: false };

        const style = window.getComputedStyle(el);
        return {
          exists: true,
          display: style.display,
          opacity: style.opacity,
          visible: style.display !== 'none' && style.opacity !== '0',
          innerHTML: el.innerHTML.substring(0, 500) // First 500 chars
        };
      });

      console.log('📊 Loading indicator state:', JSON.stringify(loadingState, null, 2));
    }
  }

  // Take a screenshot
  await page.screenshot({ path: '/Users/rwalters/GitHub/vibesql/web-demo/debug-screenshot.png', fullPage: true });
  console.log('\n📸 Screenshot saved to debug-screenshot.png\n');

  // Get final page state
  const finalState = await page.evaluate(() => {
    return {
      title: document.title,
      readyState: document.readyState,
      activeElement: document.activeElement?.tagName,
      editorExists: !!document.getElementById('editor'),
      resultsExists: !!document.getElementById('results'),
      loadingProgressVisible: (() => {
        const el = document.getElementById('loading-progress');
        if (!el) return false;
        const style = window.getComputedStyle(el);
        return style.display !== 'none' && style.opacity !== '0';
      })()
    };
  });

  console.log('📊 Final page state:', JSON.stringify(finalState, null, 2));

} catch (error) {
  console.error('\n❌ Error during page load:', error.message);
  await page.screenshot({ path: '/Users/rwalters/GitHub/vibesql/web-demo/debug-screenshot-error.png', fullPage: true });
} finally {
  await browser.close();

  // Summary
  console.log('\n' + '='.repeat(60));
  console.log('📋 Debug Session Summary');
  console.log('='.repeat(60));
  console.log(`Total console messages: ${logs.length}`);
  console.log(`Errors: ${logs.filter(l => l.type === 'error' || l.type === 'pageerror').length}`);
  console.log(`Warnings: ${logs.filter(l => l.type === 'warn').length}`);

  // Find the last bootstrap/loading message
  const lastBootstrap = logs.filter(l => l.text.includes('[Bootstrap]') || l.text.includes('[LoadingProgress]')).pop();
  if (lastBootstrap) {
    console.log(`\n🎯 Last initialization message: ${lastBootstrap.text}`);
  }

  console.log('\n✅ Debug session complete\n');
}
