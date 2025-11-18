#!/usr/bin/env bash
#
# Build WASM bindings for the web demo
#
# This script builds vibesql's WebAssembly bindings with OPFS support for
# persistent browser storage.
#
# Browser Compatibility:
# - Chrome 86+
# - Firefox 111+
# - Safari 15.2+
#
# Usage:
#   ./scripts/build-wasm.sh          # Build in release mode
#   ./scripts/build-wasm.sh --dev    # Build in dev mode (faster, larger)
#

set -e

# Change to repository root
cd "$(dirname "$0")/.."

# Parse arguments
BUILD_MODE="release"
EXTRA_FLAGS=""

while [[ $# -gt 0 ]]; do
  case $1 in
    --dev)
      BUILD_MODE="dev"
      shift
      ;;
    --debug)
      BUILD_MODE="dev"
      EXTRA_FLAGS="--debug"
      shift
      ;;
    *)
      echo "Unknown option: $1"
      echo "Usage: $0 [--dev|--debug]"
      exit 1
      ;;
  esac
done

echo "🔧 Building WASM bindings in $BUILD_MODE mode..."

# Check if wasm-pack is installed
if ! command -v wasm-pack &> /dev/null; then
    echo "❌ wasm-pack not found. Install it with:"
    echo "   curl https://rustwasm.github.io/wasm-pack/installer/init.sh -sSf | sh"
    exit 1
fi

# Clean old build artifacts
echo "🧹 Cleaning old WASM files..."
rm -rf web-demo/public/pkg

# Build WASM bindings
# Note: OPFS support is automatically included for wasm32 target
echo "📦 Building WASM bindings..."
if [[ "$BUILD_MODE" == "release" ]]; then
    # Use size-optimized profile settings via environment variables
    # These match the wasm-release profile defined in root Cargo.toml
    echo "🎯 Using size-optimized build settings (wasm-release profile)"
    CARGO_PROFILE_RELEASE_OPT_LEVEL=z \
    CARGO_PROFILE_RELEASE_LTO=fat \
    CARGO_PROFILE_RELEASE_CODEGEN_UNITS=1 \
    CARGO_PROFILE_RELEASE_STRIP=true \
    CARGO_PROFILE_RELEASE_PANIC=abort \
    wasm-pack build \
        --target web \
        --out-dir "$(pwd)/web-demo/public/pkg" \
        --release \
        crates/vibesql-wasm-bindings
else
    wasm-pack build \
        --target web \
        --out-dir "$(pwd)/web-demo/public/pkg" \
        $EXTRA_FLAGS \
        crates/vibesql-wasm-bindings
fi

echo "✅ WASM bindings built successfully!"
echo ""
echo "📍 Output: web-demo/public/pkg/"
echo ""
echo "🚀 Next steps:"
echo "   cd web-demo"
echo "   pnpm install"
echo "   pnpm dev"
echo ""
echo "💾 OPFS persistent storage is enabled for wasm32 target"
echo "   Data will persist across browser sessions in supported browsers"
