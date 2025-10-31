#!/usr/bin/env bash
# Run SQLLogicTest suite and generate analysis reports
#
# Usage:
#   ./scripts/run_sqllogictest_analysis.sh [--timeout SECONDS]
#
# Options:
#   --timeout SECONDS    Timeout for test run (default: 120)
#
# Outputs:
#   - Console: Analysis report
#   - target/sqllogictest_analysis.json: JSON summary
#   - target/sqllogictest_analysis.md: Markdown report
#   - target/sqllogictest_raw.log: Raw test output

set -euo pipefail

# Default timeout (2 minutes)
TIMEOUT=120

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --timeout)
            TIMEOUT="$2"
            shift 2
            ;;
        --help)
            echo "Usage: $0 [--timeout SECONDS]"
            echo ""
            echo "Run SQLLogicTest suite and generate analysis reports."
            echo ""
            echo "Options:"
            echo "  --timeout SECONDS    Timeout for test run (default: 120)"
            echo "  --help              Show this help message"
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            echo "Run '$0 --help' for usage information."
            exit 1
            ;;
    esac
done

# Change to repository root
cd "$(dirname "$0")/.."

# Ensure target directory exists
mkdir -p target

# Find Python interpreter
if command -v python3 &> /dev/null; then
    PYTHON=python3
elif [ -f ".venv/bin/python" ]; then
    PYTHON=.venv/bin/python
elif [ -f "$HOME/GitHub/.venv/bin/python" ]; then
    PYTHON="$HOME/GitHub/.venv/bin/python"
else
    PYTHON=python
fi

echo "Running SQLLogicTest suite (timeout: ${TIMEOUT}s)..."
echo "This will take a while..."
echo ""

# Run tests and capture output
# We use 'set +e' to prevent the script from exiting if tests fail
set +e
timeout "${TIMEOUT}" cargo test --test sqllogictest_suite run_sqllogictest_suite -- --nocapture 2>&1 | tee target/sqllogictest_raw.log | "$PYTHON" scripts/analyze_sqllogictest.py
EXIT_CODE=$?
set -e

# Check if we timed out
if [ $EXIT_CODE -eq 124 ]; then
    echo ""
    echo "⚠️  Tests timed out after ${TIMEOUT} seconds"
    echo "   You may want to increase the timeout with: $0 --timeout <seconds>"
fi

echo ""
echo "📊 Analysis complete!"
echo ""
echo "Generated files:"
echo "  - target/sqllogictest_raw.log       (raw test output)"
echo "  - target/sqllogictest_analysis.json (JSON summary)"
echo "  - target/sqllogictest_analysis.md   (markdown report)"
echo ""
echo "View the markdown report:"
echo "  cat target/sqllogictest_analysis.md"
