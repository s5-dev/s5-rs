#!/bin/bash
# Build s5_wasm for WebAssembly and generate JS bindings using wasm-pack

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "Building s5_wasm with wasm-pack..."
wasm-pack build --target web --release --scope redsolver

echo "Build complete! Output in pkg/"
ls -la pkg/

echo ""
echo "To publish to npm:"
echo "  cd pkg && npm publish --access public"
