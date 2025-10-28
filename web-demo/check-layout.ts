import { chromium } from 'playwright'

const url = 'https://rjwalters.github.io/nistmemsql/'

async function checkLayout() {
  console.log('🔍 Checking layout of results area...\n')

  const browser = await chromium.launch({ headless: false })
  const context = await browser.newContext()
  const page = await context.newPage()

  await page.goto(url, { waitUntil: 'networkidle' })
  await page.waitForTimeout(3000)

  // Check the editor and results div dimensions
  const dimensions = await page.evaluate(() => {
    const editor = document.querySelector('#editor')
    const results = document.querySelector('#results')
    const editorRect = editor?.getBoundingClientRect()
    const resultsRect = results?.getBoundingClientRect()

    // Check Monaco instances
    const models = (window as any).monaco?.editor?.getModels()
    const editors = (window as any).monaco?.editor?.getEditors()

    return {
      editor: {
        exists: !!editor,
        rect: editorRect ? {
          width: editorRect.width,
          height: editorRect.height,
          top: editorRect.top,
          left: editorRect.left,
        } : null,
        computedStyle: editor ? {
          display: window.getComputedStyle(editor).display,
          width: window.getComputedStyle(editor).width,
          height: window.getComputedStyle(editor).height,
        } : null,
      },
      results: {
        exists: !!results,
        rect: resultsRect ? {
          width: resultsRect.width,
          height: resultsRect.height,
          top: resultsRect.top,
          left: resultsRect.left,
        } : null,
        computedStyle: results ? {
          display: window.getComputedStyle(results).display,
          width: window.getComputedStyle(results).width,
          height: window.getComputedStyle(results).height,
        } : null,
      },
      monaco: {
        modelsCount: models?.length || 0,
        editorsCount: editors?.length || 0,
      },
    }
  })

  console.log('📐 Layout Information:')
  console.log(JSON.stringify(dimensions, null, 2))

  // Click execute and check again
  console.log('\n🖱️  Clicking Execute...\n')
  await page.click('#execute-btn')
  await page.waitForTimeout(1000)

  const afterExecution = await page.evaluate(() => {
    const results = document.querySelector('#results')
    const resultsRect = results?.getBoundingClientRect()

    // Get Monaco editor content
    const models = (window as any).monaco?.editor?.getModels()

    return {
      resultsRect: resultsRect ? {
        width: resultsRect.width,
        height: resultsRect.height,
      } : null,
      resultsMonacoContent: models && models.length > 1
        ? models[1].getValue().substring(0, 200)
        : 'NO_CONTENT',
      resultsMonacoLines: models && models.length > 1
        ? models[1].getLineCount()
        : 0,
    }
  })

  console.log('📊 After Execution:')
  console.log(JSON.stringify(afterExecution, null, 2))

  console.log('\n💡 Browser left open. Press Ctrl+C to close.\n')
  await page.waitForTimeout(120000)

  await browser.close()
}

checkLayout().catch(console.error)
