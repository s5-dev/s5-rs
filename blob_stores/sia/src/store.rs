use crate::Error;
use crate::config::SiaStoreConfig;
use anyhow::anyhow;
use base64::Engine;
use bytes::Bytes;
use dashmap::DashMap;
use futures::Stream;
use futures::stream::TryStreamExt;
use hex::ToHex;
use http::{HeaderMap, HeaderValue};
use hyper::{Body, Client, client::HttpConnector};
use s5_core::blob::location::{BlobLocation, SiaFile, SiaFileHost, SiaFileSlab};
use s5_core::store::{Store, StoreFeatures, StoreResult};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct SiaStore {
    pub bucket: String,
    pub worker_object_api_url: String,
    pub worker_pinned_object_api_url: String,
    pub bus_accounts_fund_api_url: String,
    pub bus_hosts_api_url: String,
    pub bus_contracts_api_url: String,
    pub bus_objects_rename_api_url: String,
    pub bus_objects_api_url: String,
    auth_headers: HeaderMap,
    http_client: Arc<hyper::Client<hyper::client::HttpConnector>>,

    network_is_zen: bool,
    host_quic_address_cache: DashMap<String, Option<String>>,
    reqwest_client: reqwest::Client,
}

impl SiaStore {
    pub async fn create(config: SiaStoreConfig) -> StoreResult<Self> {
        // bucket: &str, worker_api_url: &str, bus_api_url: &str, password: &str
        let mut auth_headers: HeaderMap = HeaderMap::new();
        let auth_str =
            base64::engine::general_purpose::STANDARD.encode(format!(":{}", config.password));
        auth_headers.insert(
            "authorization",
            HeaderValue::from_str(&format!("Basic {}", auth_str))?,
        );

        let worker_api_url = config.worker_api_url;
        let bus_api_url = config.bus_api_url;
        let mut store = Self {
            bucket: config.bucket,
            http_client: Arc::new(hyper::Client::new()),
            auth_headers: auth_headers.clone(),
            worker_object_api_url: format!("{worker_api_url}/object"),
            worker_pinned_object_api_url: format!("{worker_api_url}/pinned"),
            bus_hosts_api_url: format!("{bus_api_url}/hosts"),
            bus_contracts_api_url: format!("{bus_api_url}/contracts"),
            bus_accounts_fund_api_url: format!("{bus_api_url}/accounts/fund"),
            bus_objects_rename_api_url: format!("{bus_api_url}/objects/rename"),
            bus_objects_api_url: format!("{bus_api_url}/objects"),
            network_is_zen: false,
            host_quic_address_cache: DashMap::new(),
            reqwest_client: reqwest::Client::new(),
        };

        let upload_settings_res = http_get(
            &store.http_client,
            &format!("{bus_api_url}/settings/upload"),
            &store.auth_headers,
        )
        .await?;
        let upload_settings: RenterdBusUploadSettingsRes =
            serde_json::from_slice(&upload_settings_res)?;

        if upload_settings.packing.enabled {
            return Err(Error::RenterdPackingEnabled.into());
        }
        if upload_settings.redundancy.min_shards > 1 {
            return Err(Error::RenterdPackingEnabled.into());
        }

        let state_res = http_get(
            &store.http_client,
            &format!("{bus_api_url}/state"),
            &store.auth_headers,
        )
        .await?;
        let bus_state: RenterdBusStateRes = serde_json::from_slice(&state_res)?;
        store.network_is_zen = bus_state.network == "zen";

        Ok(store)
    }

    fn pinned_object_url_for_path(&self, path: &str) -> String {
        format!(
            "{}/{}?bucket={}",
            &self.worker_pinned_object_api_url, path, self.bucket
        )
    }

    fn auth_with_range_header(&self, offset: u64, max_len: Option<u64>) -> StoreResult<HeaderMap> {
        let mut headers = self.auth_headers.clone();
        if offset > 0 {
            headers.insert(
                "Range",
                if let Some(max_len) = max_len {
                    format!("bytes={offset}-{}", max_len - offset - 1)
                } else {
                    format!("bytes={offset}-",)
                }
                .try_into()?,
            );
        }

        Ok(headers)
    }

    async fn get_address_for_hostkey(&self, hostkey: &str) -> StoreResult<Option<String>> {
        if self.host_quic_address_cache.contains_key(hostkey) {
            return Ok(self
                .host_quic_address_cache
                .get(hostkey)
                .unwrap()
                .to_owned());
        }
        let res = self
            .reqwest_client
            .post(if self.network_is_zen {
                "https://api.siascan.com/hosts?offset=0&limit=1"
            } else {
                "https://api.siascan.com/zen/hosts?offset=0&limit=1"
            })
            .body(format!("{{\"publicKeys\":[\"{}\"]}}", hostkey))
            .send()
            .await?
            .json::<Vec<SiascanHostRes>>()
            .await?;

        let mut addr = None;

        if let Some(first) = res.first() {
            for address in &first.v2_net_addresses {
                if address.protocol == "quic" {
                    addr = Some(address.address.to_owned());
                }
            }
        }

        self.host_quic_address_cache
            .insert(hostkey.to_owned(), addr.clone());
        Ok(addr)
    }

    async fn provide_sia_file(&self, path: &str) -> StoreResult<SiaFile> {
        let res = http_get(
            &self.http_client,
            &self.pinned_object_url_for_path(path),
            &self.auth_headers,
        )
        .await?;
        let o: SiaPinnedObjectRes = serde_json::from_slice(&res)?;

        // TODO make this more efficient
        let contracts_res = http_get(
            &self.http_client,
            &self.bus_contracts_api_url,
            &self.auth_headers,
        )
        .await?;
        let contracts: Vec<SiaRenterdBusContract> = serde_json::from_slice(&contracts_res)?;
        let contracts: Vec<&SiaRenterdBusContract> = contracts
            .iter()
            .filter(|c| matches!(c.usability, SiaRenterdBusContractUsability::Good))
            .collect();

        let first_slab = &o.slabs[0];

        let planned_u_sc_per_byte: f64 = 3.07e-3 * 1.0; // TODO adjust multiplier to make full file download possible?

        let size: u64 = o.slabs.iter().map(|slab| slab.length as u64).sum();

        let u_sc_needed_for_dl: u32 = (planned_u_sc_per_byte * (size as f64)).round() as u32;

        let mut hosts: BTreeMap<u8, SiaFileHost> = BTreeMap::new();
        let mut indexed_hostkeys: HashMap<String, u8> = HashMap::new();
        let mut slabs = vec![];

        for slab in &o.slabs {
            let mut s = SiaFileSlab {
                shard_roots: BTreeMap::new(),
                slab_encryption_key: slab.encryption_key,
            };
            for shard in &slab.sectors {
                let hostkey = &shard.host_key;
                if self.get_address_for_hostkey(hostkey).await?.is_none() {
                    log::debug!(
                        "host {} does not have web-compatible address, skipping",
                        hostkey
                    );
                    continue;
                }
                if !indexed_hostkeys.contains_key(hostkey) {
                    let host_id: u8 = indexed_hostkeys.len() as u8;

                    let mut ephemeral_account_private_key = [0u8; 32];
                    getrandom::fill(&mut ephemeral_account_private_key)
                        .map_err(|e| std::io::Error::other(e.to_string()))?;

                    let signing_key =
                        ed25519_dalek::SigningKey::from_bytes(&ephemeral_account_private_key);

                    let mut contract_id = None;
                    for c in &contracts {
                        if &c.host_key == hostkey {
                            contract_id = Some(c.id.clone());
                            break;
                        }
                    }
                    if contract_id.is_none() {
                        continue;
                    }

                    let pubkey_str: String = signing_key.verifying_key().encode_hex();
                    let fund_req = SiaRenterdBusApiFundRequest {
                        amount: format!("{}uS", u_sc_needed_for_dl),
                        account_id: pubkey_str.to_string(),
                        contract_id: contract_id.unwrap(),
                    }; // TODO Maybe fund all?
                    let fund_req_str = serde_json::to_string(&fund_req)?;

                    let fund_res = http_post(
                        &self.http_client,
                        &self.bus_accounts_fund_api_url,
                        &self.auth_headers,
                        Body::from(fund_req_str),
                    )
                    .await;

                    if let Err(err) = fund_res {
                        log::warn!("funding {hostkey} failed {}", err);
                        continue;
                    }

                    hosts.insert(
                        host_id,
                        SiaFileHost {
                            hostkey: hostkey.clone(),
                            v2_siamux_addresses: vec![
                                self.get_address_for_hostkey(hostkey).await?.unwrap(),
                            ],
                            ephemeral_account_private_key,
                        },
                    );

                    indexed_hostkeys.insert(hostkey.clone(), host_id);
                }
                let mut shard_root = [0u8; 32];
                hex::decode_to_slice(&shard.root, &mut shard_root)?;
                let host_id = *indexed_hostkeys
                    .get(hostkey)
                    .ok_or_else(|| Error::HostNotFoundOnSiascan)?; // Reusing error, though context is slightly different
                s.shard_roots.insert(host_id, shard_root);
            }
            slabs.push(s);
        }

        let loc = SiaFile {
            size,
            slab_size: first_slab.length,
            min_shards: first_slab.min_shards,
            hosts,
            file_encryption_key: o.encryption_key,
            slabs,
        };

        Ok(loc)
    }
}

#[async_trait::async_trait]
impl Store for SiaStore {
    fn features(&self) -> StoreFeatures {
        StoreFeatures {
            case_sensitive: false,
            recommended_max_dir_size: u64::MAX,
            supports_rename: true,
        }
    }
    async fn exists(&self, path: &str) -> StoreResult<bool> {
        let headers = self.auth_headers.clone();
        let url = format!(
            "{}/{}?bucket={}",
            self.worker_object_api_url, path, self.bucket
        );
        let res = http_req(
            &self.http_client,
            http::Method::HEAD,
            &url,
            headers,
            Body::empty(),
        )
        .await?;

        match res.status().as_u16() {
            200 => Ok(true),
            404 => Ok(false),
            status => Err(Error::HttpFail(status).into()),
        }
    }

    async fn delete(&self, path: &str) -> StoreResult<()> {
        let headers = self.auth_headers.clone();
        let url = format!(
            "{}/{}?bucket={}",
            self.worker_object_api_url, path, self.bucket
        );
        let res = http_req(
            &self.http_client,
            http::Method::DELETE,
            &url,
            headers,
            Body::empty(),
        )
        .await?;
        match res.status().as_u16() {
            200 => Ok(()),
            status => Err(Error::HttpFail(status).into()),
        }
    }

    async fn rename(&self, old_path: &str, new_path: &str) -> StoreResult<()> {
        let headers = self.auth_headers.clone();
        let req = SiaRenterdBusObjectsRenameRequest {
            bucket: self.bucket.to_owned(),
            from: old_path.to_owned(),
            to: new_path.to_owned(),
            mode: "single".to_owned(),
            force: false,
        };
        let res = http_req(
            &self.http_client,
            http::Method::POST,
            &self.bus_objects_rename_api_url,
            headers,
            Body::from(serde_json::to_string(&req)?),
        )
        .await?;
        match res.status().as_u16() {
            200 => Ok(()),
            status => Err(Error::HttpFail(status).into()),
        }
    }

    async fn put_stream(
        &self,
        path: &str,
        stream: Box<dyn Stream<Item = Result<Bytes, std::io::Error>> + Send + Unpin + 'static>,
    ) -> StoreResult<()> {
        let url = format!(
            "{}/{}?bucket={}",
            self.worker_object_api_url, path, self.bucket
        );
        let request = {
            let mut request = http::Request::builder().method(http::Method::PUT).uri(url);
            for (header, value) in self.auth_headers.iter() {
                request = request.header(header, value);
            }
            request.body(Body::wrap_stream(stream))?
        };
        let response = self.http_client.request(request).await?;
        if !response.status().is_success() {
            let status = response.status().as_u16();
            let text =
                String::from_utf8(hyper::body::to_bytes(response.into_body()).await?.into())?;
            return Err(Error::HttpFailWithBody(status, text).into());
        }
        Ok(())
    }

    async fn put_bytes(&self, path: &str, bytes: Bytes) -> StoreResult<()> {
        let url = format!(
            "{}/{}?bucket={}",
            self.worker_object_api_url, path, self.bucket
        );
        let request = {
            let mut request = http::Request::builder().method(http::Method::PUT).uri(url);
            for (header, value) in self.auth_headers.iter() {
                request = request.header(header, value);
            }
            request.body(Body::from(bytes))?
        };
        let response = self.http_client.request(request).await?;
        if !response.status().is_success() {
            let status = response.status().as_u16();
            let text =
                String::from_utf8(hyper::body::to_bytes(response.into_body()).await?.into())?;
            return Err(Error::HttpFailWithBody(status, text).into());
        }
        Ok(())
    }

    async fn open_read_stream(
        &self,
        path: &str,
        offset: u64,
        max_len: Option<u64>,
    ) -> StoreResult<Box<dyn Stream<Item = Result<Bytes, std::io::Error>> + Send + Unpin + 'static>>
    {
        let url = format!(
            "{}/{}?bucket={}",
            self.worker_object_api_url, path, self.bucket
        );
        let res = http_req(
            &self.http_client,
            http::Method::GET,
            &url,
            self.auth_with_range_header(offset, max_len)?,
            Body::empty(),
        )
        .await?;

        if !res.status().is_success() {
            return Err(Error::HttpFail(res.status().as_u16()).into());
        }
        let body = res.into_body().into_stream();
        let stream = body.map_err(std::io::Error::other);
        Ok(Box::new(stream))
    }

    async fn open_read_bytes(
        &self,
        path: &str,
        offset: u64,
        max_len: Option<u64>,
    ) -> StoreResult<Bytes> {
        let url = format!(
            "{}/{}?bucket={}",
            self.worker_object_api_url, path, self.bucket
        );
        let res = http_req(
            &self.http_client,
            http::Method::GET,
            &url,
            self.auth_with_range_header(offset, max_len)?,
            Body::empty(),
        )
        .await?;

        match res.status().as_u16() {
            200 => Ok(hyper::body::to_bytes(res.into_body()).await?),
            status => Err(Error::HttpFail(status).into()),
        }
    }

    async fn provide(&self, path: &str) -> StoreResult<Vec<BlobLocation>> {
        let loc = self.provide_sia_file(path).await?;

        Ok(vec![BlobLocation::SiaFile(loc)])
    }

    async fn size(&self, path: &str) -> StoreResult<u64> {
        let headers = self.auth_headers.clone();
        let url = format!(
            "{}/{}?bucket={}",
            self.worker_object_api_url, path, self.bucket
        );
        let res = http_req(
            &self.http_client,
            http::Method::HEAD,
            &url,
            headers,
            Body::empty(),
        )
        .await?;

        if !res.status().is_success() {
            return Err(Error::HttpFail(res.status().as_u16()).into());
        }

        let len = res
            .headers()
            .get("Content-Length")
            .ok_or_else(|| anyhow!("missing Content-Length header"))?
            .to_str()
            .map_err(|_| anyhow!("invalid Content-Length header"))?
            .parse::<u64>()
            .map_err(|_| anyhow!("invalid Content-Length header"))?;

        Ok(len)
    }

    async fn list(
        &self,
    ) -> StoreResult<Box<dyn Stream<Item = Result<String, std::io::Error>> + Send + Unpin + 'static>>
    {
        let url = format!("{}?bucket={}", self.bus_objects_api_url, self.bucket);
        let headers = self.auth_headers.clone();

        // TODO: Implement pagination if the API supports it (e.g. offset/limit or marker).
        // For now, we assume the API returns all objects or we just fetch the first batch.
        // Based on search results, it might support `offset` and `limit`.

        let res_bytes = http_get(&self.http_client, &url, &headers).await?;
        let response: SiaObjectsResponse = serde_json::from_slice(&res_bytes)?;

        let objects = response.objects.unwrap_or_default();
        let paths: Vec<String> = objects.into_iter().map(|o| o.name).collect();

        let stream = futures::stream::iter(paths.into_iter().map(Ok));
        Ok(Box::new(stream))
    }
}

// Helper functions

async fn http_req(
    client: &Client<HttpConnector>,
    method: http::Method,
    url: &str,
    headers: HeaderMap,
    body: Body,
) -> Result<http::Response<Body>, Error> {
    let request = {
        let mut request = http::Request::builder().method(method).uri(url);
        for (header, value) in headers.iter() {
            request = request.header(header, value);
        }
        request.body(body)?
    };
    Ok(client.request(request).await?)
}

async fn http_req_full(
    client: &Client<HttpConnector>,
    method: http::Method,
    url: &str,
    headers: HeaderMap,
    body: Body,
) -> Result<Bytes, Error> {
    let response = http_req(client, method, url, headers, body).await?;

    if !response.status().is_success() {
        let status = response.status().as_u16();
        let text = String::from_utf8(hyper::body::to_bytes(response.into_body()).await?.into())?;
        return Err(Error::HttpFailWithBody(status, text));
    }
    let body_bytes = hyper::body::to_bytes(response.into_body()).await?;
    Ok(body_bytes)
}

async fn http_get(
    client: &Client<HttpConnector>,
    url: &str,
    auth_headers: &HeaderMap,
) -> Result<Bytes, Error> {
    let headers = auth_headers.clone();
    http_req_full(client, http::Method::GET, url, headers, Body::empty()).await
}

async fn http_post(
    client: &Client<HttpConnector>,
    url: &str,
    auth_headers: &HeaderMap,
    body: Body,
) -> Result<Bytes, Error> {
    let mut headers = auth_headers.clone();
    headers.insert("content-type", HeaderValue::from_str("application/json")?);
    http_req_full(client, http::Method::POST, url, headers, body).await
}

// Models

#[derive(Serialize)]
struct SiaRenterdBusApiFundRequest {
    #[serde(rename = "accountId")]
    account_id: String,
    #[serde(rename = "contractID")]
    contract_id: String,
    amount: String,
}

#[derive(Serialize)]
struct SiaRenterdBusObjectsRenameRequest {
    bucket: String,
    from: String,
    to: String,
    mode: String,
    force: bool,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct SiaRenterdBusContract {
    id: String,
    host_key: String,
    usability: SiaRenterdBusContractUsability,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
enum SiaRenterdBusContractUsability {
    Good,
    Bad,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct SiaPinnedObjectRes {
    encryption_key: [u8; 32],
    slabs: Vec<SiaPinnedSlab>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct SiaPinnedSlab {
    encryption_key: [u8; 32],
    min_shards: u8,
    sectors: Vec<SiaPinnedSector>,
    // offset: u32,
    length: u32,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct SiaPinnedSector {
    root: String,
    host_key: String,
}

#[derive(Deserialize)]
struct RenterdBusStateRes {
    network: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct RenterdBusUploadSettingsRes {
    packing: RenterdBusUploadSettingsPacking,
    redundancy: RenterdBusUploadSettingsRedundancy,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct RenterdBusUploadSettingsPacking {
    enabled: bool,
    // slab_buffer_max_size_soft: u64,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct RenterdBusUploadSettingsRedundancy {
    pub min_shards: u8,
    // pub total_shards: u16,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct SiascanHostRes {
    pub v2_net_addresses: Vec<SiascanHostV2NetAddr>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct SiascanHostV2NetAddr {
    pub protocol: String,
    pub address: String,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
struct SiaObjectsResponse {
    pub objects: Option<Vec<SiaObjectMetadata>>,
    #[allow(dead_code)]
    pub has_more: Option<bool>,
    #[allow(dead_code)]
    pub next_marker: Option<String>,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
struct SiaObjectMetadata {
    pub name: String,
    #[allow(dead_code)]
    pub size: u64,
    #[allow(dead_code)]
    pub health: f64,
}
