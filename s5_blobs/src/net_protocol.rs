use std::collections::HashMap;
use std::sync::Arc;

use iroh::endpoint::Connection;
use iroh::protocol::{AcceptError, ProtocolHandler};
use irpc_iroh::read_request;
use s5_core::pins::{PinContext, Pins};
use s5_core::{BlobStore, Hash};

use crate::config::PeerConfigBlobs;
use crate::rpc::{
    DeleteBlob, DownloadBlob, Query, QueryResponse, RpcMessage, RpcProto, UploadBlob,
};

const CHUNK_SIZE: usize = 64 * 1024; // 64k

#[derive(Debug, Clone)]
pub struct BlobsServer {
    stores: Arc<HashMap<String, BlobStore>>, // named stores
    // Keyed by stringified remote id (Display or Debug form).
    // The map may also contain a special "*" wildcard entry which
    // is used when no exact peer id match is found.
    peer_cfg: Arc<HashMap<String, PeerConfigBlobs>>, // per-peer ACLs
    /// Optional pinning backend used to enforce that uploads,
    /// downloads and deletes are scoped to the calling node
    /// (`PinContext::NodeId`).
    pinner: Option<Arc<dyn Pins>>,
}

impl BlobsServer {
    pub fn new(
        stores: HashMap<String, BlobStore>,
        peer_cfg: HashMap<String, PeerConfigBlobs>,
        pinner: Option<Arc<dyn Pins>>,
    ) -> Self {
        Self {
            stores: Arc::new(stores),
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
        // Use remote_id if available on this iroh version
        let node_id = conn.remote_id()?;
        log::debug!("s5_blobs: accepted connection from {node_id}");
        // Use the EndpointId display string as the canonical key for
        // ACL lookups so that the same string can be used consistently
        // in logs, configs, and client code.
        let node_key = node_id.to_string();
        let node_id_bytes: [u8; 32] = *node_id.as_bytes();

        while let Some(msg) = read_request::<RpcProto>(&conn).await? {
            match msg {
                RpcMessage::Query(msg) => {
                    let irpc::WithChannels { inner, tx, .. } = msg;
                    let _ =
                        handle_query(&node_key, &self.stores, self.cfg_for(&node_key), inner, tx)
                            .await;
                }
                RpcMessage::UploadBlob(msg) => {
                    let irpc::WithChannels { inner, rx, tx, .. } = msg;
                    let _ = handle_upload(
                        node_id_bytes,
                        &self.stores,
                        self.cfg_for(&node_key),
                        self.pinner.clone(),
                        inner,
                        rx,
                        tx,
                    )
                    .await;
                }
                RpcMessage::DownloadBlob(msg) => {
                    let irpc::WithChannels { inner, tx, .. } = msg;
                    let _ = handle_download(
                        &node_key,
                        node_id_bytes,
                        &self.stores,
                        self.cfg_for(&node_key),
                        self.pinner.clone(),
                        inner,
                        tx,
                    )
                    .await;
                }
                RpcMessage::DeleteBlob(msg) => {
                    let irpc::WithChannels { inner, tx, .. } = msg;
                    let _ = handle_delete(
                        &node_key,
                        node_id_bytes,
                        &self.stores,
                        self.cfg_for(&node_key),
                        self.pinner.clone(),
                        inner,
                        tx,
                    )
                    .await;
                }
            }
        }
        conn.closed().await;
        Ok(())
    }
}

async fn handle_query(
    _node_key: &str,
    stores: &HashMap<String, BlobStore>,
    cfg: Option<&PeerConfigBlobs>,
    query: Query,
    tx: irpc::channel::oneshot::Sender<QueryResponse>,
) {
    // TODO: If/when target_types is added, support additional targets (e.g. Obao6) in queries/answers.
    let hash: Hash = query.hash.into();
    let mut resp = QueryResponse::default();

    if let Some(cfg) = cfg {
        for name in &cfg.readable_stores {
            if let Some(store) = stores.get(name)
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
        }
    }

    let _ = tx.send(resp).await;
}

async fn handle_upload(
    node_id_bytes: [u8; 32],
    stores: &HashMap<String, BlobStore>,
    cfg: Option<&PeerConfigBlobs>,
    pinner: Option<Arc<dyn Pins>>,
    req: UploadBlob,
    rx: irpc::channel::mpsc::Receiver<bytes::Bytes>,
    tx: irpc::channel::oneshot::Sender<Result<(), String>>,
) {
    let Some(cfg) = cfg else {
        let _ = tx.send(Err("permission denied".into())).await;
        return;
    };
    let Some(store_name) = &cfg.store_uploads_in else {
        let _ = tx.send(Err("uploads not allowed".into())).await;
        return;
    };
    let Some(store) = stores.get(store_name) else {
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
                if let Some(pinner) = pinner
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
    _node_key: &str,
    node_id_bytes: [u8; 32],
    stores: &HashMap<String, BlobStore>,
    cfg: Option<&PeerConfigBlobs>,
    pinner: Option<Arc<dyn Pins>>,
    req: DownloadBlob,
    tx: irpc::channel::mpsc::Sender<bytes::Bytes>,
) {
    let Some(cfg) = cfg else {
        return;
    };
    let hash: Hash = req.hash.into();

    if let Some(pinner) = pinner {
        match pinner
            .is_pinned(hash, PinContext::NodeId(node_id_bytes))
            .await
        {
            Ok(true) => {}
            _ => return, // Not pinned by this user, deny download
        }
    }

    // find first readable store containing the blob
    let mut size_opt = None;
    let mut store_opt: Option<&BlobStore> = None;
    for name in &cfg.readable_stores {
        if let Some(s) = stores.get(name)
            && let Ok(true) = s.contains(hash).await
        {
            if let Ok(sz) = s.size(hash).await {
                size_opt = Some(sz);
            }
            store_opt = Some(s);
            break;
        }
    }
    let Some(store) = store_opt else {
        return;
    };
    let Some(size) = size_opt else {
        return;
    };

    if req.offset > size {
        return;
    }
    // TODO: If requests carry chunk bitmaps, use them to shape the read plan and coalesce chunks.
    let to_send = match req.max_len {
        Some(m) => m.min(size - req.offset),
        None => size - req.offset,
    };

    let mut sent: u64 = 0;
    while sent < to_send {
        let want = std::cmp::min(CHUNK_SIZE as u64, to_send - sent);
        match store
            .read_as_bytes(hash, req.offset + sent, Some(want))
            .await
        {
            Ok(bytes) => {
                if bytes.is_empty() {
                    break;
                }
                if tx.send(bytes.clone()).await.is_err() {
                    break;
                }
                sent += bytes.len() as u64;
            }
            Err(_) => break,
        }
    }
}

async fn handle_delete(
    _node_key: &str,
    node_id_bytes: [u8; 32],
    stores: &HashMap<String, BlobStore>,
    cfg: Option<&PeerConfigBlobs>,
    pinner: Option<Arc<dyn Pins>>,
    req: DeleteBlob,
    tx: irpc::channel::oneshot::Sender<Result<bool, String>>,
) {
    let Some(cfg) = cfg else {
        let _ = tx.send(Err("permission denied".into())).await;
        return;
    };
    // If user can upload, they can delete their own pins.
    if cfg.store_uploads_in.is_none() {
        let _ = tx.send(Err("delete not allowed".into())).await;
        return;
    }

    let hash: Hash = req.hash.into();

    if let Some(pinner) = pinner {
        match pinner
            .unpin_hash(hash, PinContext::NodeId(node_id_bytes))
            .await
        {
            Ok(orphaned) => {
                if orphaned {
                    for store in stores.values() {
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
