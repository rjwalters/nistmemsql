#!/bin/bash
set -e

echo "Building vibesql for WebAssembly..."

# Check if wasm-pack is installed
if ! command -v wasm-pack &> /dev/null; then
    echo "Error: wasm-pack not found. Install with: cargo install wasm-pack"
    exit 1
fi

# Build for web target
echo "Building with wasm-pack..."
wasm-pack build crates/vibesql-wasm-bindings \
    --target web \
    --release \
    --out-dir ../../web-demo/public/pkg

echo "Build complete! Output in web-demo/public/pkg/"

# Show file sizes
echo ""
echo "File sizes:"
ls -lh web-demo/public/pkg/*.wasm 2>/dev/null || echo "No .wasm files found"

# Calculate gzipped size if gzip is available
if command -v gzip &> /dev/null && [ -f web-demo/public/pkg/vibesql_wasm_bg.wasm ]; then
    echo ""
    echo "Gzipped size (for network transfer):"
    gzip -c web-demo/public/pkg/vibesql_wasm_bg.wasm | wc -c | awk '{printf "  %.2f MB\n", $1/1024/1024}'
fi

echo ""
echo "Next steps:"
echo "  1. cd web-demo"
echo "  2. npm install"
echo "  3. npm run dev"
