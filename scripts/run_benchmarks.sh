#!/bin/bash
set -e

echo "=== Running nistmemsql Benchmarks ==="
echo ""

# Ensure Python bindings are installed
echo "Building optimized nistmemsql with PyO3..."
cd crates/python-bindings
maturin build --release
pip install --force-reinstall target/wheels/nistmemsql-*.whl
cd ../..

# Install benchmark dependencies
echo "Installing benchmark dependencies..."
pip install -r benchmarks/requirements.txt

# Run benchmarks
echo "Running benchmark suite..."
pytest benchmarks/ \
    --benchmark-only \
    --benchmark-json=benchmark_results.json \
    --benchmark-autosave

# Generate comparison report
echo "Generating comparison report..."
python scripts/compare_performance.py

echo ""
echo "=== Benchmarks Complete ==="
echo "Results saved to: benchmark_results.json"
