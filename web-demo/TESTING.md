# Web Demo Testing Guide

This document describes how to test the VibeSQL web demo, including automated testing with headless browsers.

## Headless Browser Testing

### Overview

We use [Playwright](https://playwright.dev/) for automated browser testing. This allows us to:
- Test the app loading sequence
- Capture console logs and errors
- Take screenshots for visual verification
- Verify UI state programmatically
- Debug issues without manual browser interaction

### Setup

Playwright is already installed as a dev dependency. To install browsers:

```bash
pnpm install
npx playwright install chromium
```

### Quick Start

**Run the debug script:**

```bash
# Start the dev server in one terminal
pnpm run dev

# In another terminal, run the headless browser test
node debug-load.mjs
```

The script will:
1. Navigate to the app
2. Capture all console logs (including our debug messages)
3. Wait for the loading indicator to disappear
4. Take a screenshot (`debug-screenshot.png`)
5. Report the final page state

### Debug Script (`debug-load.mjs`)

The `debug-load.mjs` script provides comprehensive observability:

**Console Logs:** All browser console messages are captured and displayed with emoji prefixes:
- 📝 `[log]` - Regular console.log
- ℹ️ `[info]` - console.info
- ⚠️ `[warn]` - console.warn
- ❌ `[error]` - console.error
- 🐛 `[debug]` - console.debug

**Network Monitoring:** Failed requests are logged automatically

**Page Errors:** JavaScript exceptions are captured and displayed

**Screenshots:** Saved to `debug-screenshot.png` for visual verification

**State Inspection:** Final page state includes:
- Document ready state
- Element existence checks
- Loading indicator visibility
- Active element

### Expected Output

When the app loads successfully, you should see:

```
✅ Loading indicator disappeared - app loaded successfully!

📊 Final page state: {
  "title": "VibeSQL - AI-Powered SQL:1999 Database",
  "readyState": "complete",
  "editorExists": true,
  "resultsExists": true,
  "loadingProgressVisible": false
}
```

### Debugging Loading Issues

If the app gets stuck during loading:

1. **Run the debug script** - It will show exactly where initialization stops
2. **Check the last log message** - Look for the last `[Bootstrap]` or `[LoadingProgress]` message
3. **View the screenshot** - `debug-screenshot.png` shows the visual state
4. **Check incomplete steps** - The script reports which loading steps didn't complete

**Example debug output:**
```
🎯 Last initialization message: [Bootstrap] Initializing examples...
```
This tells you the app hung while initializing the examples component.

### Customizing the Debug Script

You can modify `debug-load.mjs` to:

**Change the URL:**
```javascript
const URL = 'http://localhost:5173/vibesql/';
```

**Adjust timeout:**
```javascript
const TIMEOUT = 30000; // 30 seconds
```

**Take screenshots at specific points:**
```javascript
await page.screenshot({ path: 'before-load.png' });
// ... do something ...
await page.screenshot({ path: 'after-load.png' });
```

**Execute custom checks:**
```javascript
const customState = await page.evaluate(() => {
  return {
    editorContent: document.querySelector('#editor')?.textContent,
    examplesCount: document.querySelectorAll('.example-item').length,
    // ... any other checks
  };
});
console.log('Custom state:', customState);
```

### Manual Testing

**Development server:**
```bash
pnpm run dev
# Open http://localhost:5173/vibesql/
```

**Production build:**
```bash
pnpm run build
pnpm run preview
# Open http://localhost:4173/vibesql/
```

### Unit Tests

Run unit tests with Vitest:

```bash
# Run all tests
pnpm test

# Watch mode
pnpm test:watch

# Coverage report
pnpm test:coverage

# UI mode
pnpm test:ui
```

### Integration Tests

Integration tests use the same Playwright infrastructure:

```bash
# Run integration tests (example)
node debug-load.mjs
```

To add more integration tests, create new scripts following the pattern in `debug-load.mjs`.

## Troubleshooting

### Playwright Installation Issues

If `npx playwright install` fails:

```bash
# Clear cache and reinstall
rm -rf ~/Library/Caches/ms-playwright
npx playwright install --force chromium
```

### Port Already in Use

If the dev server won't start:

```bash
# Kill processes on port 5173
lsof -ti:5173 | xargs kill -9

# Or use a different port
pnpm run dev --port 5174
```

### Script Hangs

If `debug-load.mjs` hangs:

1. Check if the dev server is running
2. Verify the URL is correct
3. Increase the timeout
4. Check for JavaScript errors in the console output

### Screenshot Not Generated

If no screenshot appears:

- Check file permissions in the web-demo directory
- Verify the path in the script is absolute
- Look for error messages in the script output

## Best Practices

1. **Run debug script after significant changes** to the initialization flow
2. **Commit screenshots** showing successful load states for reference
3. **Add console logs** with prefixes like `[Bootstrap]` for easy filtering
4. **Use the loading progress component** for all async initialization steps
5. **Test both dev and production builds** before deploying

## CI/CD Integration

To run headless tests in CI:

```yaml
# Example GitHub Actions workflow
- name: Install Playwright
  run: npx playwright install --with-deps chromium

- name: Start dev server
  run: pnpm run dev &

- name: Wait for server
  run: npx wait-on http://localhost:5173

- name: Run headless tests
  run: node debug-load.mjs

- name: Upload screenshot
  if: always()
  uses: actions/upload-artifact@v3
  with:
    name: debug-screenshot
    path: web-demo/debug-screenshot.png
```

## Resources

- [Playwright Documentation](https://playwright.dev/)
- [Vitest Documentation](https://vitest.dev/)
- [Web Demo Source](./src/)
