#!/bin/bash
# Test script to run the 4 failing test files from issue #898

set -e

echo "Testing the 4 files mentioned in issue #898..."
echo ""

# Test files
FILES=(
    "third_party/sqllogictest/test/select1.test"
    "third_party/sqllogictest/test/select3.test"
    "third_party/sqllogictest/test/select4.test"
    "third_party/sqllogictest/test/index/orderby_nosort/10/slt_good_29.test"
)

for file in "${FILES[@]}"; do
    echo "Testing: $file"
    # Run the test with a timeout
    if timeout 30 cargo test --test sqllogictest_runner -- --exact "$file" 2>&1 | tee "/tmp/test_$(basename $file).log"; then
        echo "  ✓ PASSED"
    else
        echo "  ✗ FAILED - see /tmp/test_$(basename $file).log"
    fi
    echo ""
done
