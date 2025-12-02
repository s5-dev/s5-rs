#!/bin/bash
# Build s5_wasm for WebAssembly and generate JS bindings

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR/../.."

echo "Building s5_wasm for wasm32-unknown-unknown..."
cargo build -p s5_wasm --target wasm32-unknown-unknown --release

echo "Generating JavaScript bindings..."
wasm-bindgen \
    --target web \
    --out-dir bindings/s5_wasm/pkg \
    target/wasm32-unknown-unknown/release/s5_wasm.wasm

echo "Optimizing WASM with wasm-opt (if available)..."
if command -v wasm-opt &> /dev/null; then
    wasm-opt -Oz -o bindings/s5_wasm/pkg/s5_wasm_bg.wasm.opt bindings/s5_wasm/pkg/s5_wasm_bg.wasm
    mv bindings/s5_wasm/pkg/s5_wasm_bg.wasm.opt bindings/s5_wasm/pkg/s5_wasm_bg.wasm
    echo "WASM optimized!"
else
    echo "wasm-opt not found, skipping optimization"
fi

echo "Build complete! Output in bindings/s5_wasm/pkg/"
ls -la bindings/s5_wasm/pkg/
