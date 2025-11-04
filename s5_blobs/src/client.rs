use std::collections::BTreeSet;

use bytes::Bytes;
use iroh::Endpoint;
use irpc::Client as IrpcClient;
use irpc_iroh::IrohRemoteConnection;
use s5_core::Hash;

use crate::rpc::{DownloadBlob, Query, QueryResponse, RpcProto, UploadBlob};

#[derive(Clone)]
pub struct Client {
    inner: IrpcClient<RpcProto>,
}

impl Client {
    pub const ALPN: &'static [u8] = crate::rpc::ALPN;

    pub fn connect(endpoint: Endpoint, addr: impl Into<iroh::EndpointAddr>) -> Self {
        let conn = IrohRemoteConnection::new(endpoint, addr.into(), Self::ALPN.to_vec());
        Client {
            inner: IrpcClient::boxed(conn),
        }
    }

    pub async fn query(&self, hash: Hash, location_types: BTreeSet<u8>) -> Result<QueryResponse, irpc::Error> {
        self.inner
            .rpc(Query {
                hash: *hash.as_bytes(),
                location_types,
            })
            .await
    }

    pub async fn download(
        &self,
        hash: Hash,
        offset: u64,
        max_len: Option<u64>,
    ) -> Result<irpc::channel::mpsc::Receiver<Bytes>, irpc::Error> {
        self.inner
            .server_streaming(
                DownloadBlob {
                    hash: *hash.as_bytes(),
                    offset,
                    max_len,
                },
                8,
            )
            .await
    }

    pub async fn upload_begin(
        &self,
        expected_hash: Hash,
        size: u64,
        capacity: usize,
    ) -> Result<(
        irpc::channel::mpsc::Sender<Bytes>,
        irpc::channel::oneshot::Receiver<Result<(), String>>,
    ), irpc::Error> {
        self.inner
            .client_streaming(
                UploadBlob {
                    expected_hash: *expected_hash.as_bytes(),
                    size,
                },
                capacity,
            )
            .await
    }
}
