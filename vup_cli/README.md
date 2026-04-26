# Vup Vault

Personal backup, sync, and archive tool built on S5 and powered by Sia.

## Overview

Vup Vault indexes local directories into an encrypted FS5 filesystem, backs them up to Sia (via indexd) or any S5-compatible store, and syncs between devices. All data is encrypted client-side (XChaCha20-Poly1305) before leaving the device.

This crate provides the `vup` CLI and the `vup_cli` library.

## Build

```bash
cargo build -p vup_cli
```

## Usage

```bash
# Set up a new vault (generates recovery phrase, configure targets)
vup config

# Add directories to track
vup add ~/Documents ~/Photos

# Show vault status (re-indexes tracked sources)
vup status

# Back up to a configured target
vup backup --target sia
```

## Test

```bash
cargo test -p vup_cli
```
