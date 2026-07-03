//! Debounce loop driving [`WritableFs::flush_overlay`].
//!
//! A writable mount accumulates writes in the overlay; somebody has to
//! periodically fold the overlay into a fresh snapshot and tell the
//! daemon to publish the new HEAD. This module owns the *when* —
//! consume a stream of write notifications from the FS, wait for the
//! activity to quiet down for a configurable interval, then call
//! `flush_overlay()` and hand the resulting snapshot to a
//! caller-supplied callback. The callback is where the publish RPC
//! lives; this module deliberately knows nothing about the daemon.
//!
//! Lifecycle:
//!   1. Block until the first write notification.
//!   2. Sleep for `idle_after`. Each new notification during the
//!      sleep restarts the timer (so a burst of writes coalesces
//!      into one flush).
//!   3. Call `flush_overlay()`. If it returns a snapshot, hand it to
//!      `on_flush`. Loop back to step 1.
//!
//! Cancellation:
//!   `until` is a future provided by the caller (e.g. a Ctrl-C signal,
//!   a `MountHandle`, a `oneshot::Receiver`). When it resolves, the
//!   loop exits cleanly without losing any in-progress flush.

use std::time::Duration;

use s5_fs_v2::snapshot::Snapshot;
use tracing::{info, warn};

use crate::write::WritableFs;

/// Run the debounce loop until `until` resolves. See module docs for
/// the lifecycle. The function consumes a [`WritableFs`] clone — the
/// caller keeps the original (or another clone) for any direct
/// `flush_overlay` use.
///
/// `on_flush` receives each freshly-persisted snapshot. Errors from
/// the callback are logged but don't break the loop — the next
/// notification will restart the cycle.
pub async fn run<F, Fut>(
    fs: WritableFs,
    idle_after: Duration,
    mut on_flush: F,
    until: impl std::future::Future<Output = ()>,
) where
    F: FnMut(Snapshot) -> Fut + Send,
    Fut: std::future::Future<Output = anyhow::Result<()>> + Send,
{
    info!(
        idle_ms = idle_after.as_millis() as u64,
        "s5_fuse debounce loop starting"
    );
    let signal = fs.write_signal();
    let driver = async {
        loop {
            // Block for the first write of a new burst.
            signal.notified().await;

            // Idle-wait: extend the timer on every fresh notification.
            loop {
                tokio::select! {
                    biased;
                    _ = signal.notified() => continue,
                    _ = tokio::time::sleep(idle_after) => break,
                }
            }

            // The activity window has quieted; persist whatever we have.
            match fs.flush_overlay().await {
                Ok(Some(snapshot)) => {
                    if let Err(err) = on_flush(snapshot).await {
                        // `{:#}` walks the anyhow context chain so the
                        // root cause (typically the daemon-side error)
                        // surfaces alongside the wrapping context, not
                        // just the wrapping line that fired the warn.
                        warn!(
                            error = format!("{err:#}"),
                            "debounce on_flush callback failed"
                        );
                    }
                }
                Ok(None) => {
                    // Nothing to persist — overlay was empty (the
                    // notification might have been a setattr that
                    // didn't produce committed state yet).
                }
                Err(err) => {
                    warn!(
                        error = format!("{err:#}"),
                        "flush_overlay failed during debounce"
                    );
                }
            }
        }
    };

    tokio::select! {
        _ = driver => {}
        _ = until => {
            info!("s5_fuse debounce loop: cancellation signal received");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use s5_core::blob::BlobStore;
    use s5_fs_v2::node::{
        BlobPipeline, CompressionStrategy, EncryptionStrategy, PaddingStrategy, TraversalContext,
    };
    use s5_fs_v2::snapshot::{KEY_SLOT_LEAF, KEY_SLOT_NODE};
    use s5_store_local::{LocalStore, LocalStoreConfig};
    use std::collections::BTreeMap;
    use std::sync::Arc;
    use tokio::sync::oneshot;

    fn empty_writable_fs() -> WritableFs {
        let store_dir = tempfile::tempdir().unwrap();
        let store = BlobStore::new(LocalStore::create(LocalStoreConfig {
            base_path: store_dir.path().to_string_lossy().into_owned(),
        }));
        std::mem::forget(store_dir);

        let mut keys = BTreeMap::new();
        keys.insert(KEY_SLOT_LEAF, [42u8; 32]);
        keys.insert(KEY_SLOT_NODE, [43u8; 32]);
        let pad = Some(PaddingStrategy { block_size: 4096 });
        let leaf_pipeline = BlobPipeline {
            compression: Some(CompressionStrategy::Zstd),
            padding: pad.clone(),
            encryption: Some((EncryptionStrategy::DeterministicChaCha20, KEY_SLOT_LEAF)),
            skip_when_unhelpful: None,
        };
        let node_pipeline = BlobPipeline {
            compression: Some(CompressionStrategy::Zstd),
            padding: pad,
            encryption: Some((EncryptionStrategy::DeterministicChaCha20, KEY_SLOT_NODE)),
            skip_when_unhelpful: None,
        };
        let ctx = TraversalContext {
            keys: Some(keys),
            leaf: Some(leaf_pipeline),
            node: Some(node_pipeline),
            chunking: None,
        };
        let read_store: Arc<dyn s5_core::BlobsRead> = Arc::new(store.clone());
        let snapshot = Snapshot::empty(read_store, ctx);
        WritableFs::new(snapshot, store)
    }

    /// Pulse the write signal a few times in quick succession, then
    /// stay quiet. The debounce loop should fire flush_overlay()
    /// exactly once after the idle window elapses.
    #[tokio::test]
    async fn debounce_coalesces_burst_into_one_flush() -> anyhow::Result<()> {
        let fs = empty_writable_fs();
        // Stage some real overlay state so flush has something to persist.
        fs.commit_buffer("a", b"alpha".to_vec())
            .await
            .map_err(|e| anyhow::anyhow!("commit a: {e:?}"))?;
        fs.commit_buffer("b", b"beta".to_vec())
            .await
            .map_err(|e| anyhow::anyhow!("commit b: {e:?}"))?;

        let (flush_tx, flush_rx) = std::sync::mpsc::channel::<()>();
        let (cancel_tx, cancel_rx) = oneshot::channel::<()>();

        let fs_for_loop = fs.clone();
        let handle = tokio::spawn(async move {
            run(
                fs_for_loop,
                Duration::from_millis(80),
                move |_snapshot| {
                    let tx = flush_tx.clone();
                    async move {
                        let _ = tx.send(());
                        Ok(())
                    }
                },
                async move {
                    let _ = cancel_rx.await;
                },
            )
            .await;
        });

        // Burst of pulses (simulating writes faster than the debounce
        // window): the loop should coalesce them into one flush.
        let signal = fs.write_signal();
        for _ in 0..5 {
            signal.notify_one();
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        // Wait long enough for the idle window to elapse + flush to run.
        tokio::time::sleep(Duration::from_millis(250)).await;

        // Should have received exactly one flush.
        let mut count = 0;
        while flush_rx.try_recv().is_ok() {
            count += 1;
        }
        assert_eq!(count, 1, "expected exactly one debounced flush");

        // Cancel and join.
        let _ = cancel_tx.send(());
        let _ = tokio::time::timeout(Duration::from_millis(200), handle).await;

        Ok(())
    }

    /// A second burst after the first flush should produce a second
    /// flush — the loop keeps going.
    #[tokio::test]
    async fn debounce_loops_for_subsequent_bursts() -> anyhow::Result<()> {
        let fs = empty_writable_fs();
        fs.commit_buffer("a", b"alpha".to_vec())
            .await
            .map_err(|e| anyhow::anyhow!("commit a: {e:?}"))?;

        let (flush_tx, flush_rx) = std::sync::mpsc::channel::<()>();
        let (cancel_tx, cancel_rx) = oneshot::channel::<()>();

        let fs_for_loop = fs.clone();
        let handle = tokio::spawn(async move {
            run(
                fs_for_loop,
                Duration::from_millis(60),
                move |_snapshot| {
                    let tx = flush_tx.clone();
                    async move {
                        let _ = tx.send(());
                        Ok(())
                    }
                },
                async move {
                    let _ = cancel_rx.await;
                },
            )
            .await;
        });

        let signal = fs.write_signal();
        signal.notify_one();
        tokio::time::sleep(Duration::from_millis(180)).await; // first flush

        // Stage more overlay state and pulse again.
        fs.commit_buffer("b", b"beta".to_vec())
            .await
            .map_err(|e| anyhow::anyhow!("commit b: {e:?}"))?;
        signal.notify_one();
        tokio::time::sleep(Duration::from_millis(180)).await; // second flush

        let mut count = 0;
        while flush_rx.try_recv().is_ok() {
            count += 1;
        }
        assert_eq!(count, 2, "expected one flush per burst");

        let _ = cancel_tx.send(());
        let _ = tokio::time::timeout(Duration::from_millis(200), handle).await;

        Ok(())
    }
}
