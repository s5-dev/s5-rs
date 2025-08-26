//! The hash type used by S5 (blake3, 32 bytes)
//!
//! Implementation from Iroh (MIT OR Apache-2.0)
//! https://github.com/n0-computer/

use std::io::{BufReader, Read};

use bao_tree::{BlockSize, io::outboard::PreOrderOutboard};

/// Block size used by s5, 2^6*1024 = 64KiB
pub const S5_BLOCK_SIZE: BlockSize = BlockSize::from_chunk_log(6);

/// Synchronously compute the outboard of a file, and return hash and outboard.
///
/// It is assumed that the file is not modified while this is running.
///
/// If it is modified while or after this is running, the outboard will be
/// invalid, so any attempt to compute a slice from it will fail.
///
/// If the size of the file is changed while this is running, an error will be
/// returned.
///
/// The computed outboard is without length prefix.
pub fn compute_outboard(
    read: impl Read,
    size: u64,
    // TODO Implement progress
    _progress: impl Fn(u64) -> std::io::Result<()> + Send + Sync + 'static,
) -> std::io::Result<(s5_core::Hash, Option<Vec<u8>>)> {
    use bao_tree::io::sync::CreateOutboard;
    let buf_size = usize::try_from(size).unwrap_or(usize::MAX).min(1024 * 1024);
    let reader = BufReader::with_capacity(buf_size, read);
    let ob = PreOrderOutboard::<Vec<u8>>::create_sized(reader, size, S5_BLOCK_SIZE)?;
    let root = ob.root.into();
    let data = ob.data;
    let data = if !data.is_empty() { Some(data) } else { None };
    Ok((root, data))
}
