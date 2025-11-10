#!/bin/bash
# Extract only the tables needed for the first 4-table join query

INPUT="third_party/sqllogictest/test/select5.test"
OUTPUT="tests/select5_samples/select5_minimal.test"

echo "# Minimal select5 test - 4-table join with correct data" > "$OUTPUT"
echo "" >> "$OUTPUT"

# Extract CREATE TABLE and INSERT statements for t29, t31, t51, t55
for table in 29 31 51 55; do
    # Extract CREATE TABLE with statement ok
    awk "/^statement ok$/ {getline; if (/^CREATE TABLE t$table\(/) {print \"statement ok\"; print; while (getline && !/^\)$/) print; print; exit}}" "$INPUT" >> "$OUTPUT"
    echo "" >> "$OUTPUT"
    
    # Extract all INSERT statements for this table with statement ok
    awk "/^statement ok$/ {getline; if (/^INSERT INTO t$table VALUES/) {print \"statement ok\"; print; print \"\"}}" "$INPUT" >> "$OUTPUT"
done

# Extract the first 4-table join query
echo "# 4-table join query" >> "$OUTPUT"
sed -n '/^query TTTT valuesort join-4-1$/,/^$/p' "$INPUT" | head -12 >> "$OUTPUT"

echo "Created minimal test: $OUTPUT"
wc -l "$OUTPUT"
