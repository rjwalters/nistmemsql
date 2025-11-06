#!/bin/bash
# Script to run the 4 failing test files from issue #898

set -e

echo "Building test runner..."
cargo build --test sqllogictest_suite 2>&1 | tail -20

echo ""
echo "========================================="
echo "Running 4 failing tests from issue #898"
echo "========================================="
echo ""

# Set environment to run just these specific files
export SQLLOGICTEST_TIME_BUDGET=60
export SQLLOGICTEST_SEED=12345

# Run the test suite which will test these files
timeout 120 cargo test --test sqllogictest_suite run_sqllogictest_suite -- --nocapture 2>&1 | tee /tmp/issue_898_test_output.log

echo ""
echo "Test output saved to /tmp/issue_898_test_output.log"
