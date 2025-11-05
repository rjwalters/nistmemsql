#!/bin/bash
set -e

# prepare_deployment.sh - Prepare GitHub Pages deployment directory
#
# Usage: ./prepare_deployment.sh <output_dir> <web_demo_dist> <badges_dir> [benchmark_results.json] [benchmark_history.json]
#
# This script consolidates deployment preparation logic that was duplicated across
# ci-and-deploy.yml, boost-sqllogictest.yml, and redeploy-web-demo.yml

OUTPUT_DIR="${1:?Output directory required}"
WEB_DEMO_DIST="${2:?Web demo dist directory required}"
BADGES_DIR="${3:?Badges directory required}"
BENCHMARK_RESULTS="${4:-}"
BENCHMARK_HISTORY="${5:-}"

echo "=== Preparing Deployment ==="
echo "Output: $OUTPUT_DIR"
echo "Web demo: $WEB_DEMO_DIST"
echo "Badges: $BADGES_DIR"
echo ""

# Create deployment directory structure
mkdir -p "$OUTPUT_DIR"
mkdir -p "$OUTPUT_DIR/badges"
mkdir -p "$OUTPUT_DIR/benchmarks"

# Copy web demo build output
if [ -d "$WEB_DEMO_DIST" ]; then
  echo "Copying web demo from $WEB_DEMO_DIST..."
  cp -r "$WEB_DEMO_DIST"/* "$OUTPUT_DIR/"
  echo "✓ Copied web demo"
else
  echo "⚠️  WARNING: Web demo dist directory not found: $WEB_DEMO_DIST"
  exit 1
fi

# Copy badges
if [ -d "$BADGES_DIR" ]; then
  echo "Copying badges from $BADGES_DIR..."
  cp "$BADGES_DIR"/* "$OUTPUT_DIR/badges/" 2>/dev/null || echo "⚠️  No badge files found"
  echo "✓ Copied badge files"
else
  echo "⚠️  Badges directory not found: $BADGES_DIR"
fi

# Copy benchmark results with verification
if [ -n "$BENCHMARK_RESULTS" ] && [ -f "$BENCHMARK_RESULTS" ]; then
  echo "Copying benchmark results from $BENCHMARK_RESULTS..."
  cp "$BENCHMARK_RESULTS" "$OUTPUT_DIR/benchmarks/benchmark_results.json"
  echo "✓ Copied benchmark_results.json"
else
  echo "⚠️  Benchmark results not provided or not found"
fi

if [ -n "$BENCHMARK_HISTORY" ] && [ -f "$BENCHMARK_HISTORY" ]; then
  echo "Copying benchmark history from $BENCHMARK_HISTORY..."
  cp "$BENCHMARK_HISTORY" "$OUTPUT_DIR/benchmarks/benchmark_history.json"
  echo "✓ Copied benchmark_history.json"
else
  echo "⚠️  Benchmark history not provided or not found"
fi

echo ""
echo "=== Deployment Structure ==="
ls -la "$OUTPUT_DIR/" | head -20

echo ""
echo "=== Badges Directory ==="
ls -la "$OUTPUT_DIR/badges/" 2>/dev/null || echo "No badges directory"

echo ""
echo "=== Benchmarks Directory ==="
ls -la "$OUTPUT_DIR/benchmarks/" 2>/dev/null || echo "No benchmarks directory"

echo ""
echo "✓ Deployment preparation complete"
