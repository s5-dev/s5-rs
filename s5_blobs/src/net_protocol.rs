use std::sync::Arc;

use iroh::{
    Endpoint,
    endpoint::Connection,
    protocol::{AcceptError, ProtocolHandler},
};
use s5_core::{BlobStore, api::blobs::store};

use crate::protocol::{Announce, Request, Response};

#[derive(Debug)]
pub(crate) struct BlobsInner {
    pub(crate) store: BlobStore,
    pub(crate) endpoint: Endpoint,
}

/// A protocol handler for the blobs protocol.
#[derive(Debug, Clone)]
pub struct BlobsProtocol {
    pub(crate) inner: Arc<BlobsInner>,
}

impl BlobsProtocol {
    pub fn new(store: BlobStore, endpoint: Endpoint) -> Self {
        Self {
            inner: Arc::new(BlobsInner { store, endpoint }),
        }
    }
}

impl ProtocolHandler for BlobsProtocol {
    /// The `accept` method is called for each incoming connection for our ALPN.
    ///
    /// The returned future runs on a newly spawned tokio task, so it can run as long as
    /// the connection lasts.
    async fn accept(&self, connection: Connection) -> Result<(), AcceptError> {
        // We can get the remote's node id from the connection.
        let node_id = connection.remote_node_id()?;
        log::debug!("accepted connection from {node_id}");

        while let (mut send, mut recv) = connection.accept_bi().await? {
            let req_bytes = recv.read_to_end(64).await.map_err(AcceptError::from_err)?;

            let req: Request = minicbor::decode(&req_bytes).map_err(AcceptError::from_err)?;
            match req {
                Request::Query(query) => {
                    let res = if self.inner.store.contains(query.hash.into()).await.unwrap() {
                        let store_res = self.inner.store.provide(query.hash.into()).await.unwrap();
                        Response::Announce(Announce {
                            hash: query.hash,
                            locations: store_res,
                            // TODO add timestamps
                            timestamp: 0,
                            subsec_nanos: 0,
                        })
                    } else {
                        Response::NotFound(query.hash)
                    };
                    let res_bytes = minicbor::to_vec(res).map_err(AcceptError::from_err)?;
                    send.write_all(&res_bytes).await.unwrap();
                    send.finish()?;
                }
            }
        }

        // Wait until the remote closes the connection, which it does once it
        // received the response.
        connection.closed().await;

        Ok(())
    }
}
