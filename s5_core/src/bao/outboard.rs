//! The bao outboard format used by S5
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
    progress: impl Fn(u64) -> std::io::Result<()> + Send + Sync + 'static,
) -> std::io::Result<(crate::Hash, Option<Vec<u8>>)> {
    use bao_tree::io::sync::CreateOutboard;
    let buf_size = usize::try_from(size).unwrap_or(usize::MAX).min(1024 * 1024);

    struct ProgressReader<R, F> {
        inner: R,
        progress: F,
        current: u64,
    }

    impl<R: Read, F: Fn(u64) -> std::io::Result<()>> Read for ProgressReader<R, F> {
        fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
            let n = self.inner.read(buf)?;
            self.current += n as u64;
            (self.progress)(self.current)?;
            Ok(n)
        }
    }

    let reader = ProgressReader {
        inner: read,
        progress,
        current: 0,
    };

    let reader = BufReader::with_capacity(buf_size, reader);
    let ob = PreOrderOutboard::<Vec<u8>>::create_sized(reader, size, S5_BLOCK_SIZE)?;
    let root = ob.root.into();
    let data = ob.data;
    let data = if !data.is_empty() { Some(data) } else { None };
    Ok((root, data))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    #[test]
    fn test_compute_outboard_progress() {
        let data = vec![0u8; 1024 * 1024]; // 1MB
        let progress_calls = Arc::new(Mutex::new(Vec::new()));
        let progress_calls_clone = progress_calls.clone();

        let (hash, outboard) = compute_outboard(&data[..], data.len() as u64, move |p| {
            progress_calls_clone.lock().unwrap().push(p);
            Ok(())
        })
        .unwrap();

        assert_ne!(hash, crate::Hash::EMPTY);
        assert!(outboard.is_some());

        let calls = progress_calls.lock().unwrap();
        assert!(!calls.is_empty());
        assert_eq!(*calls.last().unwrap(), 1024 * 1024);
    }
}
