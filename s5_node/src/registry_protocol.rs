use std::{fmt, sync::Arc};

use anyhow::{Result, anyhow};
use bytes::Bytes;
use iroh::{
    endpoint::{Connection, Endpoint},
    protocol::{AcceptError, ProtocolHandler},
};
use irpc::{Client as IrpcClient, channel::oneshot, rpc_requests};
use irpc_iroh::{IrohRemoteConnection, read_request};

use s5_core::{RedbRegistry, RegistryApi, StreamKey, StreamMessage};
use serde::{Deserialize, Serialize};
use tracing::warn;

pub const ALPN: &[u8] = b"s5/registry/0";

#[derive(Debug, Serialize, Deserialize)]
pub struct GetRequest {
    pub key_type: u8,
    pub key_data: [u8; 32],
}

#[derive(Debug, Serialize, Deserialize)]
pub struct GetResponse {
    pub message: Option<Vec<u8>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SetRequest {
    pub message: Vec<u8>,
}

#[derive(Debug, Serialize, Deserialize)]
#[rpc_requests(message = RegistryRpcMessage)]
pub enum RpcProto {
    #[rpc(tx = oneshot::Sender<GetResponse>)]
    Get(GetRequest),
    #[rpc(tx = oneshot::Sender<Result<(), String>>)]
    Set(SetRequest),
}

#[derive(Clone)]
pub struct RegistryServer {
    registry: Arc<RedbRegistry>,
}

impl fmt::Debug for RegistryServer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RegistryServer").finish()
    }
}

impl RegistryServer {
    pub fn new(registry: RedbRegistry) -> Self {
        Self {
            registry: Arc::new(registry),
        }
    }

    async fn handle_get(&self, req: GetRequest) -> GetResponse {
        let key = match StreamKey::from_bytes(req.key_type, &req.key_data) {
            Ok(key) => key,
            Err(err) => {
                warn!("registry get: invalid key: {err}");
                return GetResponse { message: None };
            }
        };

        match self.registry.get(&key).await {
            Ok(Some(message)) => GetResponse {
                message: Some(message.serialize().to_vec()),
            },
            Ok(None) => GetResponse { message: None },
            Err(err) => {
                warn!("registry get error: {err}");
                GetResponse { message: None }
            }
        }
    }

    async fn handle_set(&self, req: SetRequest) -> std::result::Result<(), String> {
        let message =
            StreamMessage::deserialize(Bytes::from(req.message)).map_err(|err| err.to_string())?;

        self.registry
            .set(message)
            .await
            .map_err(|err| err.to_string())
    }
}

impl ProtocolHandler for RegistryServer {
    async fn accept(&self, conn: Connection) -> Result<(), AcceptError> {
        while let Some(msg) = read_request::<RpcProto>(&conn).await? {
            match msg {
                RegistryRpcMessage::Get(irpc::WithChannels { inner, tx, .. }) => {
                    let resp = self.handle_get(inner).await;
                    let _ = tx.send(resp).await;
                }
                RegistryRpcMessage::Set(irpc::WithChannels { inner, tx, .. }) => {
                    let result = self.handle_set(inner).await;
                    let _ = tx.send(result).await;
                }
            }
        }
        conn.closed().await;
        Ok(())
    }
}

#[derive(Clone)]
pub struct Client {
    inner: IrpcClient<RpcProto>,
}

impl Client {
    pub fn connect(endpoint: Endpoint, addr: impl Into<iroh::EndpointAddr>) -> Self {
        let conn = IrohRemoteConnection::new(endpoint, addr.into(), ALPN.to_vec());
        Client {
            inner: IrpcClient::boxed(conn),
        }
    }

    pub async fn get(&self, key: StreamKey) -> Result<Option<StreamMessage>> {
        let (key_type, key_bytes) = key.to_bytes();
        let mut key_data = [0u8; 32];
        key_data.copy_from_slice(key_bytes);

        let response = self.inner.rpc(GetRequest { key_type, key_data }).await?;

        if let Some(bytes) = response.message {
            let message = StreamMessage::deserialize(Bytes::from(bytes))
                .map_err(|err| anyhow!("failed to deserialize registry message: {err}"))?;
            Ok(Some(message))
        } else {
            Ok(None)
        }
    }

    pub async fn set(&self, message: StreamMessage) -> Result<()> {
        let bytes = message.serialize();
        match self
            .inner
            .rpc(SetRequest {
                message: bytes.to_vec(),
            })
            .await?
        {
            Ok(()) => Ok(()),
            Err(err) => Err(anyhow!(err.to_string())),
        }
    }
}

#[derive(Clone)]
pub struct RemoteRegistry {
    client: Client,
}

impl RemoteRegistry {
    pub fn connect(endpoint: Endpoint, addr: impl Into<iroh::EndpointAddr>) -> Self {
        Self {
            client: Client::connect(endpoint, addr),
        }
    }

    pub fn client(&self) -> &Client {
        &self.client
    }
}

#[async_trait::async_trait]
impl RegistryApi for RemoteRegistry {
    async fn get(&self, key: &StreamKey) -> Result<Option<StreamMessage>> {
        self.client.get(*key).await
    }

    async fn set(&self, message: StreamMessage) -> Result<()> {
        self.client.set(message).await
    }

    async fn delete(&self, _key: &StreamKey) -> Result<()> {
        // Remote deletion is not currently supported; treat as a no-op.
        Ok(())
    }
}
