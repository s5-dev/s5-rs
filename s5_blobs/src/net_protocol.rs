use std::collections::HashMap;
use std::sync::Arc;

use iroh::endpoint::Connection;
use iroh::protocol::{AcceptError, ProtocolHandler};
use irpc_iroh::read_request;
use s5_core::blob::BlobsRead;
use s5_core::pins::{PinContext, Pins};
use s5_core::{Hash, blob::BlobStore};

use crate::config::PeerConfigBlobs;
use crate::rpc::{
    DeleteBlob, DownloadBlob, PinBlob, Query, QueryResponse, RpcMessage, RpcProto, UploadBlob,
};

const CHUNK_SIZE: usize = 64 * 1024; // 64k

#[derive(Clone)]
pub struct BlobsServer {
    stores: Arc<HashMap<String, BlobStore>>, // named stores (read + write)
    /// Read-only sources that can be queried and downloaded from, but not written to.
    /// These are checked alongside `stores` when a peer has the source name in `readable_stores`.
    read_sources: Arc<HashMap<String, Arc<dyn BlobsRead>>>,
    // Keyed by stringified remote id (Display or Debug form).
    // The map may also contain a special "*" wildcard entry which
    // is used when no exact peer id match is found.
    peer_cfg: Arc<HashMap<String, PeerConfigBlobs>>, // per-peer ACLs
    /// Optional pinning backend used to enforce that uploads,
    /// downloads and deletes are scoped to the calling node
    /// (`PinContext::NodeId`).
    pinner: Option<Arc<dyn Pins>>,
}

impl std::fmt::Debug for BlobsServer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BlobsServer")
            .field("stores", &self.stores.keys().collect::<Vec<_>>())
            .field(
                "read_sources",
                &self.read_sources.keys().collect::<Vec<_>>(),
            )
            .field("peer_cfg", &self.peer_cfg.keys().collect::<Vec<_>>())
            .field("pinner", &self.pinner.is_some())
            .finish()
    }
}

impl BlobsServer {
    pub fn new(
        stores: HashMap<String, BlobStore>,
        peer_cfg: HashMap<String, PeerConfigBlobs>,
        pinner: Option<Arc<dyn Pins>>,
    ) -> Self {
        Self {
            stores: Arc::new(stores),
            read_sources: Arc::new(HashMap::new()),
            peer_cfg: Arc::new(peer_cfg),
            pinner,
        }
    }

    /// Creates a new BlobsServer with both read-write stores and read-only sources.
    ///
    /// Read-only sources (like `LocalLinksStore`) can be queried and downloaded from
    /// but cannot receive uploads. They are referenced by name in `readable_stores`.
    pub fn with_read_sources(
        stores: HashMap<String, BlobStore>,
        read_sources: HashMap<String, Arc<dyn BlobsRead>>,
        peer_cfg: HashMap<String, PeerConfigBlobs>,
        pinner: Option<Arc<dyn Pins>>,
    ) -> Self {
        Self {
            stores: Arc::new(stores),
            read_sources: Arc::new(read_sources),
            peer_cfg: Arc::new(peer_cfg),
            pinner,
        }
    }

    fn cfg_for(&self, node_key: &str) -> Option<&PeerConfigBlobs> {
        // First try an exact match for this peer's id; if not present,
        // fall back to a wildcard entry ("*") if configured.
        self.peer_cfg
            .get(node_key)
            .or_else(|| self.peer_cfg.get("*"))
    }
}

impl ProtocolHandler for BlobsServer {
    async fn accept(&self, conn: Connection) -> Result<(), AcceptError> {
        let node_id = conn.remote_id();
        let node_key = node_id.to_string();
        let node_id_bytes: [u8; 32] = *node_id.as_bytes();

        tracing::info!(
            peer = %node_id.fmt_short(),
            "blobs: accepted connection"
        );

        let mut request_count = 0u64;
        while let Some(msg) = read_request::<RpcProto>(&conn).await? {
            request_count += 1;
            match msg {
                RpcMessage::Query(msg) => {
                    let irpc::WithChannels { inner, tx, .. } = msg;
                    let _ = handle_query(self, &node_key, inner, tx).await;
                }
                RpcMessage::UploadBlob(msg) => {
                    let irpc::WithChannels { inner, rx, tx, .. } = msg;
                    let _ = handle_upload(self, &node_key, node_id_bytes, inner, rx, tx).await;
                }
                RpcMessage::DownloadBlob(msg) => {
                    let irpc::WithChannels { inner, tx, .. } = msg;
                    let _ = handle_download(self, &node_key, node_id_bytes, inner, tx).await;
                }
                RpcMessage::DeleteBlob(msg) => {
                    let irpc::WithChannels { inner, tx, .. } = msg;
                    let _ = handle_delete(self, &node_key, node_id_bytes, inner, tx).await;
                }
                RpcMessage::PinBlob(msg) => {
                    let irpc::WithChannels { inner, tx, .. } = msg;
                    let _ = handle_pin(self, &node_key, node_id_bytes, inner, tx).await;
                }
            }
        }

        tracing::info!(
            peer = %node_id.fmt_short(),
            request_count,
            "blobs: connection closed"
        );
        conn.closed().await;
        Ok(())
    }
}

async fn handle_pin(
    server: &BlobsServer,
    node_key: &str,
    node_id_bytes: [u8; 32],
    req: PinBlob,
    tx: irpc::channel::oneshot::Sender<Result<bool, String>>,
) {
    let Some(cfg) = server.cfg_for(node_key) else {
        let _ = tx.send(Err("permission denied".into())).await;
        return;
    };
    // We use the same permission as upload for pinning
    let Some(store_name) = &cfg.store_uploads_in else {
        let _ = tx.send(Err("uploads (pinning) not allowed".into())).await;
        return;
    };
    let Some(store) = server.stores.get(store_name) else {
        let _ = tx.send(Err("invalid upload store".into())).await;
        return;
    };

    let hash: Hash = req.hash.into();

    // Check if blob exists
    match store.contains(hash).await {
        Ok(true) => {
            // Blob exists, try to pin it
            if let Some(pinner) = &server.pinner
                && let Err(e) = pinner
                    .pin_hash(hash, PinContext::NodeId(node_id_bytes))
                    .await
            {
                let _ = tx.send(Err(format!("pinning failed: {e}"))).await;
                return;
            }
            let _ = tx.send(Ok(true)).await;
        }
        Ok(false) => {
            // Blob not found
            let _ = tx.send(Ok(false)).await;
        }
        Err(e) => {
            let _ = tx.send(Err(format!("store error: {e}"))).await;
        }
    }
}

async fn handle_query(
    server: &BlobsServer,
    node_key: &str,
    query: Query,
    tx: irpc::channel::oneshot::Sender<QueryResponse>,
) {
    // TODO: If/when target_types is added, support additional targets (e.g. Obao6) in queries/answers.
    let mut resp = QueryResponse::default();

    if let Some(cfg) = server.cfg_for(node_key) {
        if query.blinded {
            // Blinded query: hash field contains blake3(actual_hash)
            // We need to find a blob where blake3(blob_hash) == query.hash
            // Note: blinded queries only work on full BlobStores (need list_hashes)
            let blinded_hash = query.hash;

            for name in &cfg.readable_stores {
                if let Some(store) = server.stores.get(name) {
                    // Try to find matching blob by checking blinded hashes
                    if let Some(actual_hash) = find_blob_by_blinded_hash(store, blinded_hash).await
                    {
                        resp.exists = true;
                        resp.actual_hash = Some(*actual_hash.as_bytes());

                        if resp.size.is_none()
                            && let Ok(sz) = store.size(actual_hash).await
                        {
                            resp.size = Some(sz);
                        }

                        if let Ok(mut locs) = store.provide(actual_hash).await {
                            resp.locations.append(&mut locs);
                        }
                        break; // Found it
                    }
                }
            }
        } else {
            // Normal query: hash field is the actual hash
            let hash: Hash = query.hash.into();

            for name in &cfg.readable_stores {
                // Check full stores first (they can provide locations)
                if let Some(store) = server.stores.get(name)
                    && let Ok(true) = store.contains(hash).await
                {
                    resp.exists = true;
                    if resp.size.is_none()
                        && let Ok(sz) = store.size(hash).await
                    {
                        resp.size = Some(sz);
                    }

                    if let Ok(mut locs) = store.provide(hash).await {
                        // TODO: optionally filter by query.location_types
                        resp.locations.append(&mut locs);
                    }
                }
                // Also check read-only sources
                else if let Some(source) = server.read_sources.get(name)
                    && let Ok(true) = source.blob_contains(hash).await
                {
                    resp.exists = true;
                    if resp.size.is_none()
                        && let Ok(sz) = source.blob_get_size(hash).await
                    {
                        resp.size = Some(sz);
                    }
                    // Read-only sources don't provide locations
                }
            }
        }
    }

    let _ = tx.send(resp).await;
}

/// Finds a blob by its blinded hash (blake3(actual_hash)).
///
/// This iterates over the store's blobs and computes blinded hashes on-the-fly.
/// For large stores, this could be slow - consider adding a blinded hash index
/// if this becomes a bottleneck.
async fn find_blob_by_blinded_hash(store: &BlobStore, blinded_hash: [u8; 32]) -> Option<Hash> {
    // Get list of all blob hashes in the store
    let hashes = match store.list_hashes().await {
        Ok(h) => h,
        Err(_) => return None,
    };

    for hash in hashes {
        // Compute blinded hash: blake3(hash)
        let computed_blinded = blake3::hash(hash.as_bytes());
        if computed_blinded.as_bytes() == &blinded_hash {
            return Some(hash);
        }
    }

    None
}

async fn handle_upload(
    server: &BlobsServer,
    node_key: &str,
    node_id_bytes: [u8; 32],
    req: UploadBlob,
    rx: irpc::channel::mpsc::Receiver<bytes::Bytes>,
    tx: irpc::channel::oneshot::Sender<Result<(), String>>,
) {
    let Some(cfg) = server.cfg_for(node_key) else {
        let _ = tx.send(Err("permission denied".into())).await;
        return;
    };
    let Some(store_name) = &cfg.store_uploads_in else {
        let _ = tx.send(Err("uploads not allowed".into())).await;
        return;
    };
    let Some(store) = server.stores.get(store_name) else {
        let _ = tx.send(Err("invalid upload store".into())).await;
        return;
    };

    // Adapt rx into the expected Stream type for import_stream, owning the receiver.
    let stream = futures_util::stream::unfold(rx, |mut rx| async move {
        match rx.recv().await {
            Ok(Some(chunk)) => Some((Ok::<bytes::Bytes, std::io::Error>(chunk), rx)),
            _ => None,
        }
    });

    // TODO(remote-blobs): once RemoteBlobStore fully owns hashing and
    // outboard computation/verification, consider tightening this path
    // so the server can rely more directly on remote-side guarantees.
    match store.import_stream(Box::new(Box::pin(stream))).await {
        Ok(blob) => {
            let got_hash = blob.hash;
            let got_size = blob.size;
            if got_hash.as_bytes() != &req.expected_hash || got_size != req.size {
                let _ = store.delete(got_hash).await; // best-effort cleanup on mismatch
                let _ = tx.send(Err("hash/size mismatch".into())).await;
            } else {
                if let Some(pinner) = &server.pinner
                    && let Err(e) = pinner
                        .pin_hash(got_hash, PinContext::NodeId(node_id_bytes))
                        .await
                {
                    let _ = store.delete(got_hash).await;
                    let _ = tx.send(Err(format!("pinning failed: {e}"))).await;
                    return;
                }
                let _ = tx.send(Ok(())).await;
            }
        }
        Err(e) => {
            let _ = tx.send(Err(format!("upload failed: {e}"))).await;
        }
    }
}

async fn handle_download(
    server: &BlobsServer,
    node_key: &str,
    node_id_bytes: [u8; 32],
    req: DownloadBlob,
    tx: irpc::channel::mpsc::Sender<bytes::Bytes>,
) {
    let hash: Hash = req.hash.into();
    let hash_short = hash.fmt_short();

    tracing::info!(
        peer = node_key,
        hash = hash_short,
        "handle_download: request received"
    );

    let Some(cfg) = server.cfg_for(node_key) else {
        tracing::warn!(
            peer = node_key,
            hash = hash_short,
            "download denied: no peer config"
        );
        return;
    };

    // Find first readable source containing the blob (stores or read-only sources)
    let mut size_opt: Option<u64> = None;
    let mut source_opt: Option<Arc<dyn BlobsRead>> = None;
    let mut from_read_source = false;
    let mut source_name: Option<String> = None;

    for name in &cfg.readable_stores {
        // Check full stores first
        if let Some(store) = server.stores.get(name) {
            match store.contains(hash).await {
                Ok(true) => {
                    if let Ok(sz) = store.size(hash).await {
                        size_opt = Some(sz);
                    }
                    source_opt = Some(Arc::new(store.clone()) as Arc<dyn BlobsRead>);
                    source_name = Some(name.clone());
                    break;
                }
                Ok(false) => {}
                Err(e) => {
                    tracing::info!(
                        store = name,
                        hash = hash_short,
                        error = %e,
                        "store.contains failed"
                    );
                }
            }
        }
        // Also check read-only sources
        if let Some(source) = server.read_sources.get(name) {
            match source.blob_contains(hash).await {
                Ok(true) => {
                    if let Ok(sz) = source.blob_get_size(hash).await {
                        size_opt = Some(sz);
                    }
                    source_opt = Some(source.clone());
                    from_read_source = true;
                    source_name = Some(name.clone());
                    break;
                }
                Ok(false) => {}
                Err(e) => {
                    tracing::info!(
                        source = name,
                        hash = hash_short,
                        error = %e,
                        "read_source.blob_contains failed"
                    );
                }
            }
        }
    }

    // Pin check: only required for blobs from regular stores.
    // Read sources are explicitly published data — any peer configured
    // to read them should be able to download without pinning.
    if !from_read_source && !cfg.skip_pin_check {
        if let Some(pinner) = &server.pinner {
            let is_pinned = pinner
                .is_pinned(hash, PinContext::NodeId(node_id_bytes))
                .await
                .unwrap_or(false);

            if !is_pinned {
                tracing::info!(
                    peer = node_key,
                    hash = hash_short,
                    "download denied: not pinned"
                );
                return; // Not pinned by this user, deny download
            }
        }
    }

    let Some(source) = source_opt else {
        tracing::info!(
            peer = node_key,
            hash = hash_short,
            readable_stores = ?cfg.readable_stores,
            num_stores = server.stores.len(),
            num_read_sources = server.read_sources.len(),
            "download: blob not found in any readable store"
        );
        return;
    };
    let Some(size) = size_opt else {
        tracing::warn!(
            peer = node_key,
            hash = hash_short,
            source = ?source_name,
            "download: blob exists but size unknown"
        );
        return;
    };

    if req.offset > size {
        tracing::warn!(
            peer = node_key,
            hash = hash_short,
            offset = req.offset,
            size,
            "download: offset beyond blob size"
        );
        return;
    }
    // TODO: If requests carry chunk bitmaps, use them to shape the read plan and coalesce chunks.
    let to_send = match req.max_len {
        Some(m) => m.min(size - req.offset),
        None => size - req.offset,
    };

    tracing::info!(
        peer = node_key,
        hash = hash_short,
        source = ?source_name,
        size,
        to_send,
        from_read_source,
        "download: sending blob"
    );

    let mut sent: u64 = 0;
    while sent < to_send {
        let want = std::cmp::min(CHUNK_SIZE as u64, to_send - sent);
        match source
            .blob_download_slice(hash, req.offset + sent, Some(want))
            .await
        {
            Ok(bytes) => {
                if bytes.is_empty() {
                    tracing::warn!(
                        hash = hash_short,
                        sent,
                        to_send,
                        offset = req.offset + sent,
                        "download: got empty slice mid-transfer"
                    );
                    break;
                }
                if tx.send(bytes.clone()).await.is_err() {
                    tracing::info!(
                        hash = hash_short,
                        sent,
                        "download: peer disconnected"
                    );
                    break;
                }
                sent += bytes.len() as u64;
            }
            Err(e) => {
                tracing::warn!(
                    hash = hash_short,
                    sent,
                    to_send,
                    error = %e,
                    "download: slice read failed"
                );
                break;
            }
        }
    }
}

async fn handle_delete(
    server: &BlobsServer,
    node_key: &str,
    node_id_bytes: [u8; 32],
    req: DeleteBlob,
    tx: irpc::channel::oneshot::Sender<Result<bool, String>>,
) {
    let Some(cfg) = server.cfg_for(node_key) else {
        let _ = tx.send(Err("permission denied".into())).await;
        return;
    };
    // If user can upload, they can delete their own pins.
    if cfg.store_uploads_in.is_none() {
        let _ = tx.send(Err("delete not allowed".into())).await;
        return;
    }

    let hash: Hash = req.hash.into();

    if let Some(pinner) = &server.pinner {
        match pinner
            .unpin_hash(hash, PinContext::NodeId(node_id_bytes))
            .await
        {
            Ok(orphaned) => {
                if orphaned {
                    for store in server.stores.values() {
                        let _ = store.delete(hash).await;
                    }
                    let _ = tx.send(Ok(true)).await;
                } else {
                    let _ = tx.send(Ok(false)).await;
                }
            }
            Err(e) => {
                let _ = tx.send(Err(format!("unpin failed: {e}"))).await;
            }
        }
    } else {
        let _ = tx.send(Err("pinning not enabled".into())).await;
    }
}
