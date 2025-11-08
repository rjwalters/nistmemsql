#!/bin/bash
# Direct test of a single SQLLogicTest file using sqllogictest CLI
# This bypasses the random test selection and tests a specific file

REPO_ROOT=$(cd "$(dirname "$0")/.." && pwd)
TEST_DIR="${REPO_ROOT}/third_party/sqllogictest/test"

if [ -z "$1" ]; then
    echo "Usage: $0 <test_file>"
    echo "Example: $0 index/delete/10/slt_good_0.test"
    exit 1
fi

TEST_FILE="$1"

# Convert various formats to relative path
if [[ "$TEST_FILE" == *"::"* ]]; then
    TEST_FILE=$(echo "$TEST_FILE" | sed 's/::/\//g').test
fi

TEST_FILE=$(echo "$TEST_FILE" | xargs)

if [[ "$TEST_FILE" == /* ]]; then
    TEST_FILE=$(echo "$TEST_FILE" | sed "s|^$TEST_DIR/||")
fi

FULL_PATH="${TEST_DIR}/${TEST_FILE}"

if [ ! -f "$FULL_PATH" ]; then
    echo "Error: Test file not found: $FULL_PATH"
    exit 1
fi

echo "Testing: $TEST_FILE"
echo "Full path: $FULL_PATH"
echo ""

# Check if sqllogictest CLI is available
if ! command -v sqllogictest &> /dev/null; then
    echo "Note: sqllogictest CLI not found. Install with: cargo install sqllogictest-cli"
    echo ""
    echo "For now, you can review the test file manually:"
    echo "  head -50 '$FULL_PATH'"
    exit 1
fi

# Run the test
sqllogictest "$FULL_PATH"
