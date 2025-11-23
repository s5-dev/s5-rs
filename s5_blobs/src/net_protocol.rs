use std::collections::HashMap;
use std::sync::Arc;

use iroh::protocol::{AcceptError, ProtocolHandler};
use iroh::endpoint::Connection;
use irpc_iroh::read_request;
use s5_core::{BlobStore, Hash};

use crate::config::PeerConfigBlobs;
use crate::rpc::{DownloadBlob, Query, QueryResponse, RpcMessage, RpcProto, UploadBlob};

const CHUNK_SIZE: usize = 64 * 1024; // 64k

#[derive(Debug, Clone)]
pub struct BlobsServer {
    stores: Arc<HashMap<String, BlobStore>>, // named stores
    // Keyed by stringified remote id (Display or Debug form).
    peer_cfg: Arc<HashMap<String, PeerConfigBlobs>>, // per-peer ACLs
}

impl BlobsServer {
    pub fn new(
        stores: HashMap<String, BlobStore>,
        peer_cfg: HashMap<String, PeerConfigBlobs>,
    ) -> Self {
        Self {
            stores: Arc::new(stores),
            peer_cfg: Arc::new(peer_cfg),
        }
    }

    fn cfg_for(&self, node_key: &str) -> Option<&PeerConfigBlobs> {
        self.peer_cfg.get(node_key)
    }
}

impl ProtocolHandler for BlobsServer {
    async fn accept(&self, conn: Connection) -> Result<(), AcceptError> {
        // Use remote_id if available on this iroh version
        let node_id = conn.remote_id()?;
        log::debug!("s5_blobs: accepted connection from {node_id:?}");
        let node_key = format!("{node_id:?}");

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
                        &node_key,
                        &self.stores,
                        self.cfg_for(&node_key),
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
                        &self.stores,
                        self.cfg_for(&node_key),
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
    let hash: Hash = query.hash.into();
    let mut resp = QueryResponse::default();

    if let Some(cfg) = cfg {
        for name in &cfg.readable_stores {
            if let Some(store) = stores.get(name) {
                if let Ok(true) = store.contains(hash).await {
                    resp.exists = true;
                    if resp.size.is_none() {
                        if let Ok(sz) = store.size(hash).await {
                            resp.size = Some(sz);
                        }
                    }
                    if let Ok(mut locs) = store.provide(hash).await {
                        // TODO: optionally filter by query.location_types
                        resp.locations.append(&mut locs);
                    }
                }
            }
        }
    }

    let _ = tx.send(resp).await;
}

async fn handle_upload(
    _node_key: &str,
    stores: &HashMap<String, BlobStore>,
    cfg: Option<&PeerConfigBlobs>,
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

    match store.import_stream(Box::new(Box::pin(stream))).await {
        Ok((got_hash, got_size)) => {
            if got_hash.as_bytes() != &req.expected_hash || got_size != req.size {
                let _ = store.delete(got_hash).await; // best-effort cleanup on mismatch
                let _ = tx.send(Err("hash/size mismatch".into())).await;
            } else {
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
    stores: &HashMap<String, BlobStore>,
    cfg: Option<&PeerConfigBlobs>,
    req: DownloadBlob,
    tx: irpc::channel::mpsc::Sender<bytes::Bytes>,
) {
    let Some(cfg) = cfg else {
        return;
    };
    let hash: Hash = req.hash.into();

    // find first readable store containing the blob
    let mut size_opt = None;
    let mut store_opt: Option<&BlobStore> = None;
    for name in &cfg.readable_stores {
        if let Some(s) = stores.get(name) {
            if let Ok(true) = s.contains(hash).await {
                if let Ok(sz) = s.size(hash).await {
                    size_opt = Some(sz);
                }
                store_opt = Some(s);
                break;
            }
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
