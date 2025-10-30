#!/bin/bash
set -e

# Generate SQL:1999 Conformance Report as HTML for GitHub Pages

SQLTEST_RESULTS="target/sqltest_results.json"
OUTPUT="conformance-report.html"

# Get metrics
if [ -f "$SQLTEST_RESULTS" ]; then
    TOTAL=$(grep -o '"total":[[:space:]]*[0-9]*' "$SQLTEST_RESULTS" | sed 's/"total":[[:space:]]*//g')
    PASSED=$(grep -o '"passed":[[:space:]]*[0-9]*' "$SQLTEST_RESULTS" | sed 's/"passed":[[:space:]]*//g')
    FAILED=$(grep -o '"failed":[[:space:]]*[0-9]*' "$SQLTEST_RESULTS" | sed 's/"failed":[[:space:]]*//g')
    ERRORS=$(grep -o '"errors":[[:space:]]*[0-9]*' "$SQLTEST_RESULTS" | sed 's/"errors":[[:space:]]*//g')
    PASS_RATE_RAW=$(grep -o '"pass_rate":[[:space:]]*[0-9.]*' "$SQLTEST_RESULTS" | sed 's/"pass_rate":[[:space:]]*//g')
    PASS_RATE=$(printf "%.1f" "$PASS_RATE_RAW")
else
    TOTAL=0
    PASSED=0
    FAILED=0
    ERRORS=0
    PASS_RATE="0.0"
fi

COMMIT=$(git rev-parse --short HEAD 2>/dev/null || echo 'unknown')
TIMESTAMP=$(date -u +"%Y-%m-%d %H:%M:%S UTC")

# Determine color based on pass rate
if (( $(echo "$PASS_RATE >= 80" | bc -l 2>/dev/null || echo "0") )); then
    STATUS_COLOR="#10b981"  # green
    STATUS_TEXT="Excellent"
elif (( $(echo "$PASS_RATE >= 60" | bc -l 2>/dev/null || echo "0") )); then
    STATUS_COLOR="#84cc16"  # lime
    STATUS_TEXT="Good"
elif (( $(echo "$PASS_RATE >= 40" | bc -l 2>/dev/null || echo "0") )); then
    STATUS_COLOR="#eab308"  # yellow
    STATUS_TEXT="Fair"
elif (( $(echo "$PASS_RATE >= 20" | bc -l 2>/dev/null || echo "0") )); then
    STATUS_COLOR="#f97316"  # orange
    STATUS_TEXT="Poor"
else
    STATUS_COLOR="#ef4444"  # red
    STATUS_TEXT="Needs Work"
fi

# Generate failing tests section if there are errors
ERROR_SECTION=""
if [ "$ERRORS" != "0" ] && [ "$ERRORS" != "" ]; then
    # Extract error details if jq is available
    ERROR_DETAILS=""
    if command -v jq >/dev/null 2>&1 && [ -f "$SQLTEST_RESULTS" ]; then
        # Generate HTML for each failing test
        ERROR_DETAILS=$(jq -r '.error_tests[] |
          "<div class=\"bg-gray-50 dark:bg-gray-900 rounded-lg p-4 border border-gray-200 dark:border-gray-700\">" +
          "<div class=\"flex items-start justify-between mb-2\">" +
          "<span class=\"font-mono text-xs text-blue-600 dark:text-blue-400 font-semibold\">" + .id + "</span>" +
          "</div>" +
          "<div class=\"font-mono text-sm text-gray-800 dark:text-gray-200 mb-2 bg-white dark:bg-gray-800 p-2 rounded overflow-x-auto\">" + (.sql | gsub("\""; "&quot;")) + "</div>" +
          "<div class=\"text-xs text-red-600 dark:text-red-400\"><span class=\"font-semibold\">Error:</span> " + (.error | gsub("\""; "&quot;")) + "</div>" +
          "</div>"' "$SQLTEST_RESULTS" 2>/dev/null | tr '\n' ' ')
    fi

    if [ -z "$ERROR_DETAILS" ]; then
        ERROR_DETAILS="<div class=\"text-gray-600 dark:text-gray-400 text-sm\">Detailed test failures available in CI artifacts. Run <code class=\"bg-gray-100 dark:bg-gray-800 px-2 py-1 rounded\">cargo test --test sqltest_conformance -- --nocapture</code> locally to see full details.</div>"
    fi

    ERROR_SECTION="    <!-- Failing Tests -->
    <div class=\"bg-white dark:bg-gray-800 rounded-lg shadow-md border border-gray-200 dark:border-gray-700 p-8\">
      <h2 class=\"text-2xl font-bold text-gray-900 dark:text-white mb-6\">Failing Tests</h2>

      <p class=\"text-gray-700 dark:text-gray-300 mb-4\">
        The following tests are currently failing. Click to expand details.
      </p>

      <details class=\"mt-4\">
        <summary class=\"cursor-pointer text-blue-600 dark:text-blue-400 hover:underline font-medium\">
          View failing test details ($ERRORS tests)
        </summary>

        <div class=\"mt-4 space-y-3 max-h-96 overflow-y-auto\">
          $ERROR_DETAILS
        </div>
      </details>
    </div>"
fi

# Generate HTML directly with values
cat > "$OUTPUT" <<EOF
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>SQL:1999 Conformance Report - NIST MemSQL</title>
  <script src="https://cdn.tailwindcss.com"></script>
  <style>
    .fade-in { animation: fadeIn 0.5s ease-in; }
    @keyframes fadeIn {
      from { opacity: 0; transform: translateY(10px); }
      to { opacity: 1; transform: translateY(0); }
    }
  </style>
</head>
<body class="bg-gray-50 dark:bg-gray-900 min-h-screen">
  <!-- Header -->
  <header class="bg-white dark:bg-gray-800 shadow-md border-b border-gray-200 dark:border-gray-700">
    <div class="max-w-6xl mx-auto px-4 py-6">
      <div class="flex items-center justify-between flex-wrap gap-4">
        <div>
          <h1 class="text-3xl font-bold text-gray-900 dark:text-white">
            SQL:1999 Conformance Report
          </h1>
          <p class="text-sm text-gray-600 dark:text-gray-400 mt-1">
            NIST MemSQL Database Implementation
          </p>
        </div>
        <div class="flex gap-3">
          <a
            href="https://github.com/rjwalters/nistmemsql"
            target="_blank"
            rel="noopener noreferrer"
            class="px-4 py-2 bg-gray-800 dark:bg-gray-700 text-white rounded-lg hover:bg-gray-700 dark:hover:bg-gray-600 transition-colors"
          >
            View on GitHub
          </a>
          <a
            href="https://rjwalters.github.io/nistmemsql/"
            class="px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition-colors"
          >
            Try Demo
          </a>
        </div>
      </div>
    </div>
  </header>

  <!-- Main Content -->
  <main class="max-w-6xl mx-auto px-4 py-8 space-y-8">
    <!-- Metadata -->
    <div class="bg-white dark:bg-gray-800 rounded-lg shadow-md border border-gray-200 dark:border-gray-700 p-6">
      <div class="grid grid-cols-1 md:grid-cols-3 gap-4 text-sm">
        <div>
          <span class="text-gray-600 dark:text-gray-400">Generated:</span>
          <span class="ml-2 text-gray-900 dark:text-white font-medium">$TIMESTAMP</span>
        </div>
        <div>
          <span class="text-gray-600 dark:text-gray-400">Commit:</span>
          <a
            href="https://github.com/rjwalters/nistmemsql/commit/$COMMIT"
            target="_blank"
            class="ml-2 text-blue-600 dark:text-blue-400 hover:underline font-mono"
          >
            $COMMIT
          </a>
        </div>
        <div>
          <span class="text-gray-600 dark:text-gray-400">Status:</span>
          <span class="ml-2 font-medium" style="color: $STATUS_COLOR">$STATUS_TEXT</span>
        </div>
      </div>
    </div>

    <!-- Summary Cards -->
    <div class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-5 gap-6 fade-in">
      <!-- Pass Rate Card (Featured) -->
      <div class="md:col-span-2 lg:col-span-2 bg-gradient-to-br from-blue-500 to-blue-700 rounded-lg shadow-lg p-8 text-white">
        <div class="text-sm font-semibold uppercase tracking-wider opacity-90 mb-2">Pass Rate</div>
        <div class="text-5xl font-bold mb-2">$PASS_RATE%</div>
        <div class="text-sm opacity-75">of $TOTAL total tests</div>
        <div class="mt-6 bg-white/20 rounded-full h-3 overflow-hidden">
          <div
            class="bg-white h-full rounded-full transition-all duration-500"
            style="width: $PASS_RATE%"
          ></div>
        </div>
      </div>

      <!-- Passed Tests -->
      <div class="bg-white dark:bg-gray-800 rounded-lg shadow-md border border-gray-200 dark:border-gray-700 p-6">
        <div class="text-xs font-semibold uppercase tracking-wider text-gray-600 dark:text-gray-400 mb-2">Passed</div>
        <div class="text-3xl font-bold text-green-600 dark:text-green-400 mb-1">$PASSED</div>
        <div class="text-2xl">✅</div>
      </div>

      <!-- Failed Tests -->
      <div class="bg-white dark:bg-gray-800 rounded-lg shadow-md border border-gray-200 dark:border-gray-700 p-6">
        <div class="text-xs font-semibold uppercase tracking-wider text-gray-600 dark:text-gray-400 mb-2">Failed</div>
        <div class="text-3xl font-bold text-red-600 dark:text-red-400 mb-1">$FAILED</div>
        <div class="text-2xl">❌</div>
      </div>

      <!-- Errors -->
      <div class="bg-white dark:bg-gray-800 rounded-lg shadow-md border border-gray-200 dark:border-gray-700 p-6">
        <div class="text-xs font-semibold uppercase tracking-wider text-gray-600 dark:text-gray-400 mb-2">Errors</div>
        <div class="text-3xl font-bold text-orange-600 dark:text-orange-400 mb-1">$ERRORS</div>
        <div class="text-2xl">⚠️</div>
      </div>
    </div>

    <!-- Test Coverage Details -->
    <div class="bg-white dark:bg-gray-800 rounded-lg shadow-md border border-gray-200 dark:border-gray-700 p-8">
      <h2 class="text-2xl font-bold text-gray-900 dark:text-white mb-6">Test Coverage</h2>

      <p class="text-gray-700 dark:text-gray-300 mb-6">
        Tests from <a href="https://github.com/elliotchance/sqltest" target="_blank" class="text-blue-600 dark:text-blue-400 hover:underline">sqltest</a>
        - upstream-recommended SQL:1999 conformance test suite.
      </p>

      <div class="grid grid-cols-1 md:grid-cols-2 gap-4">
        <div>
          <h3 class="text-lg font-semibold text-gray-900 dark:text-white mb-3">Core Features (E-Series)</h3>
          <ul class="space-y-2 text-sm text-gray-700 dark:text-gray-300">
            <li><span class="font-mono text-xs bg-gray-100 dark:bg-gray-700 px-2 py-1 rounded">E011</span> Numeric data types</li>
            <li><span class="font-mono text-xs bg-gray-100 dark:bg-gray-700 px-2 py-1 rounded">E021</span> Character string types</li>
            <li><span class="font-mono text-xs bg-gray-100 dark:bg-gray-700 px-2 py-1 rounded">E031</span> Identifiers</li>
            <li><span class="font-mono text-xs bg-gray-100 dark:bg-gray-700 px-2 py-1 rounded">E051</span> Basic query specification</li>
            <li><span class="font-mono text-xs bg-gray-100 dark:bg-gray-700 px-2 py-1 rounded">E061</span> Basic predicates and search conditions</li>
            <li><span class="font-mono text-xs bg-gray-100 dark:bg-gray-700 px-2 py-1 rounded">E071</span> Basic query expressions</li>
            <li><span class="font-mono text-xs bg-gray-100 dark:bg-gray-700 px-2 py-1 rounded">E081</span> Basic privileges</li>
            <li><span class="font-mono text-xs bg-gray-100 dark:bg-gray-700 px-2 py-1 rounded">E091</span> Set functions</li>
          </ul>
        </div>
        <div>
          <h3 class="text-lg font-semibold text-gray-900 dark:text-white mb-3">Additional Features</h3>
          <ul class="space-y-2 text-sm text-gray-700 dark:text-gray-300">
            <li><span class="font-mono text-xs bg-gray-100 dark:bg-gray-700 px-2 py-1 rounded">E101</span> Basic data manipulation</li>
            <li><span class="font-mono text-xs bg-gray-100 dark:bg-gray-700 px-2 py-1 rounded">E111</span> Single row SELECT statement</li>
            <li><span class="font-mono text-xs bg-gray-100 dark:bg-gray-700 px-2 py-1 rounded">E121</span> Basic cursor support</li>
            <li><span class="font-mono text-xs bg-gray-100 dark:bg-gray-700 px-2 py-1 rounded">E131</span> Null value support</li>
            <li><span class="font-mono text-xs bg-gray-100 dark:bg-gray-700 px-2 py-1 rounded">E141</span> Basic integrity constraints</li>
            <li><span class="font-mono text-xs bg-gray-100 dark:bg-gray-700 px-2 py-1 rounded">E151</span> Transaction support</li>
            <li><span class="font-mono text-xs bg-gray-100 dark:bg-gray-700 px-2 py-1 rounded">E161</span> SQL comments</li>
            <li><span class="font-mono text-xs bg-gray-100 dark:bg-gray-700 px-2 py-1 rounded">F031</span> Basic schema manipulation</li>
          </ul>
        </div>
      </div>
    </div>

    <!-- Failing Tests Section (if any) -->
$ERROR_SECTION

    <!-- How to Run Tests Locally -->
    <div class="bg-white dark:bg-gray-800 rounded-lg shadow-md border border-gray-200 dark:border-gray-700 p-8">
      <h2 class="text-2xl font-bold text-gray-900 dark:text-white mb-6">Running Tests Locally</h2>

      <div class="bg-gray-100 dark:bg-gray-900 rounded-lg p-4 font-mono text-sm overflow-x-auto">
        <div class="text-gray-700 dark:text-gray-300">
          <div class="text-green-600 dark:text-green-400"># Run all conformance tests</div>
          <div>cargo test --test sqltest_conformance -- --nocapture</div>
          <div class="mt-4 text-green-600 dark:text-green-400"># Generate coverage report</div>
          <div>cargo coverage</div>
          <div class="mt-2 text-green-600 dark:text-green-400"># Open coverage report</div>
          <div>open target/llvm-cov/html/index.html</div>
        </div>
      </div>
    </div>
  </main>

  <!-- Footer -->
  <footer class="mt-12 text-center text-sm text-gray-600 dark:text-gray-400 pb-8">
    <p>NIST MemSQL - SQL:1999 Compliant Database in WebAssembly</p>
    <p class="mt-2">
      <a href="https://github.com/rjwalters/nistmemsql" target="_blank" class="text-blue-600 dark:text-blue-400 hover:underline">
        GitHub Repository
      </a>
      •
      <a href="https://rjwalters.github.io/nistmemsql/" class="text-blue-600 dark:text-blue-400 hover:underline">
        Live Demo
      </a>
    </p>
  </footer>
</body>
</html>
EOF

echo "✅ HTML conformance report generated: $OUTPUT"
