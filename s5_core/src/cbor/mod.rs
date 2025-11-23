//! CBOR token-level utilities used by S5.
//!
//! This module exposes a lightweight, token-based representation of CBOR
//! values (`Value`). It is primarily intended for diagnostics, tooling and
//! cases where a generic CBOR structure is needed. It does not define any
//! protocol-level wire formats on its own.

pub mod value;
