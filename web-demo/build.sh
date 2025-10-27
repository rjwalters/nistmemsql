#!/bin/bash

# Build script for NistMemSQL WASM Demo
# This script builds the WASM module and prepares it for the web demo

set -e

echo "Building NistMemSQL WASM module..."

# Navigate to repository root (one level up from web-demo)
cd "$(dirname "$0")/.."

# Build the WASM package
wasm-pack build \
    --target web \
    --out-dir ../web-demo/pkg \
    crates/wasm-bindings

echo ""
echo "✅ Build complete!"
echo ""
echo "To test locally, run:"
echo "  cd web-demo"
echo "  python3 -m http.server 8080"
echo ""
echo "Then open: http://localhost:8080"
