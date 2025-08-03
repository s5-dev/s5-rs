use crate::Error;
use crate::temp::zen::get_address_for_hostkey;
use base64::Engine;
use bytes::Bytes;
use futures_core::Stream;
use hex::ToHex;
use http::{HeaderMap, HeaderValue};
use hyper::Body;
use s5_core::blob::location::{BlobLocation, SiaFile, SiaFileHost, SiaFileSlab};
use s5_core::{BlobStore, Hash};
use s5_utils::compute_outboard;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::{
    io::{self},
    path::PathBuf,
    sync::Arc,
};
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tokio_util::io::ReaderStream;

const BLOB_CHUNK_SIZE: u64 = 1 << 32;

#[derive(Debug, Clone)]
pub struct SiaBlobStore {
    pub bucket: String,
    pub worker_object_api_url: String, // usually http://localhost:9980/api/worker/object/
    pub worker_pinned_object_api_url: String,
    pub bus_accounts_fund_api_url: String, // usually http://localhost:9980/api/bus/accounts/fund
    pub bus_hosts_api_url: String,         // usually http://localhost:9980/api/bus/hosts
    pub bus_object_api_url: String,
    pub store_outboard: bool,
    auth_headers: HeaderMap,
    http_client: Arc<hyper::Client<hyper::client::HttpConnector>>,
}

impl SiaBlobStore {
    pub fn new(bucket: &str, worker_api_url: &str, bus_api_url: &str, password: &str) -> Self {
        let mut auth_headers: HeaderMap = HeaderMap::new();
        let auth_str = base64::engine::general_purpose::STANDARD.encode(format!(":{}", password));
        auth_headers.insert(
            "authorization",
            HeaderValue::from_str(&format!("Basic {}", auth_str)).unwrap(),
        );
        Self {
            bucket: bucket.to_string(),
            http_client: Arc::new(hyper::Client::new()),
            auth_headers,
            worker_object_api_url: format!("{}/object", worker_api_url),
            worker_pinned_object_api_url: format!("{}/pinned", worker_api_url),
            bus_object_api_url: format!("{}/object", bus_api_url),
            bus_hosts_api_url: format!("{}/hosts", bus_api_url),
            bus_accounts_fund_api_url: format!("{}/accounts/fund", bus_api_url),
            store_outboard: true,
        }
    }

    async fn finalize_import(&self, src: ImportSource) -> Result<(Hash, u64), Error> {
        let size = src.len()?;
        let (hash, outboard) = match &src {
            ImportSource::File(path) => {
                let file = std::fs::File::open(path)?;
                compute_outboard(file, size, move |_| Ok(()))?
            }
            ImportSource::Memory(bytes) => compute_outboard(bytes.as_ref(), size, |_| Ok(()))?,
        };
        let encoded = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(hash);

        if self.store_outboard {
            if let Some(out) = outboard {
                self.put_bytes(format!("obao4/{}", encoded), out).await?;
            }
        } else {
            log::debug!("skipping outboard");
        }
        log::debug!("uploading blob bytes...");

        match src {
            ImportSource::File(path) => {
                if size < BLOB_CHUNK_SIZE {
                    let file = tokio::fs::File::open(&path).await?;
                    let stream = ReaderStream::new(file);
                    self.put_stream(format!("blob3/{}", encoded), stream)
                        .await?;
                } else {
                    let mut offset: u64 = 0;
                    while offset < size {
                        let file = std::fs::File::open(&path)?;

                        let mut file = tokio::fs::File::from_std(file);

                        file.seek(io::SeekFrom::Start(offset)).await?;

                        let limited_reader = file.take(BLOB_CHUNK_SIZE);

                        let stream = ReaderStream::new(limited_reader);

                        let res = self
                            .put_stream(format!("blob3_split/{}/{}", encoded, offset), stream)
                            .await;
                        if res.is_ok() {
                            offset += BLOB_CHUNK_SIZE;
                        } else {
                            log::warn!("retry due to error {}", res.unwrap_err());
                        }
                    }
                }
            }
            ImportSource::Memory(bytes) => {
                let path = format!("blob3/{}", encoded);
                let url = format!(
                    "{}/{}?bucket={}",
                    self.worker_object_api_url, path, self.bucket
                );
                self.http_req(
                    http::Method::PUT,
                    &url,
                    self.auth_headers.clone(),
                    Body::from(bytes),
                )
                .await?;
            }
        };

        Ok((hash, size))
    }

    async fn put_stream<S>(&self, path: String, stream: S) -> Result<(), Error>
    where
        S: Stream<Item = std::io::Result<Bytes>> + Send + 'static,
    {
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

    async fn put_bytes(&self, path: String, bytes: Vec<u8>) -> Result<(), Error> {
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

    fn object_url_for_hash(&self, hash: Hash, offset: Option<u64>, pinned_meta: bool) -> String {
        let encoded = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(hash);
        let object_path = if let Some(offset) = offset {
            format!("blob3_split/{}/{}", encoded, offset)
        } else {
            format!("blob3/{}", encoded)
        };

        format!(
            "{}/{}?bucket={}",
            if pinned_meta {
                &self.worker_pinned_object_api_url
            } else {
                &self.bus_object_api_url
            },
            object_path,
            self.bucket
        )
    }
    async fn http_get(&self, url: &str) -> Result<Bytes, Error> {
        let headers = self.auth_headers.clone();
        self.http_req(http::Method::GET, url, headers, Body::empty())
            .await
    }

    async fn http_post(&self, url: &str, body: Body) -> Result<Bytes, Error> {
        let mut headers = self.auth_headers.clone();
        headers.insert("content-type", HeaderValue::from_str("application/json")?);
        self.http_req(http::Method::POST, url, headers, body).await
    }

    async fn http_req(
        &self,
        method: http::Method,
        url: &str,
        headers: HeaderMap,
        body: Body,
    ) -> Result<Bytes, Error> {
        let request = {
            let mut request = http::Request::builder().method(method).uri(url);
            for (header, value) in headers.iter() {
                request = request.header(header, value);
            }
            request.body(body)?
        };
        let response = self.http_client.request(request).await?;
        if !response.status().is_success() {
            let status = response.status().as_u16();
            let text =
                String::from_utf8(hyper::body::to_bytes(response.into_body()).await?.into())?;
            return Err(Error::HttpFailWithBody(status, text).into());
        }
        let body_bytes = hyper::body::to_bytes(response.into_body()).await?;
        Ok(body_bytes)
    }
    pub async fn provide(&self, hash: Hash) -> Result<Vec<BlobLocation>, Error> {
        let loc = self.provide_sia_file(hash, None).await;

        return loc.map(|loc| vec![BlobLocation::SiaFile(loc)]);
        /* if loc.is_ok() {
            return Ok(vec![BlobLocation::SiaFile(loc?)]);
        }; */

        let first_chunk_loc = self.provide_sia_file(hash, Some(0)).await;

        if first_chunk_loc.is_err() {
            return first_chunk_loc.map(|x| vec![BlobLocation::SiaFile(x)]);
        } else {
            todo!();
            /*             let fc = first_chunk_loc.unwrap();
            let mut offset: u64 = fc.size;
            let first_chunk = BlobLocation::Slice {
                offset: 0,
                length: fc.size,
                inner: Box::new(BlobLocation::SiaFile(fc)),
            };
            let mut chunks = vec![first_chunk];
            loop {
                let next_chunk_res = self.provide_sia_file(hash, Some(offset)).await;
                if next_chunk_res.is_err() {
                    break;
                } else {
                    let file = next_chunk_res.unwrap();
                    offset = offset + file.size;
                    chunks.push(BlobLocation::Slice {
                        offset: offset - file.size,
                        length: file.size,
                        inner: Box::new(BlobLocation::SiaFile(file)),
                    });
                }
            }
            return Ok(chunks); */
        }
    }

    async fn provide_sia_file(&self, hash: Hash, offset: Option<u64>) -> Result<SiaFile, Error> {
        let res = self
            .http_get(&self.object_url_for_hash(hash, offset, false))
            .await?;
        let o: SiaObjectRes = serde_json::from_slice(&res)?;

        let res = self
            .http_get(&self.object_url_for_hash(hash, offset, true))
            .await?;
        let po: SiaPinnedObjectRes = serde_json::from_slice(&res)?;

        /* let api_hosts_res = self.http_get(&self.bus_hosts_api_url).await?;
        let api_hosts_res: Vec<SiaRenterdBusApiHost> = serde_json::from_slice(&api_hosts_res)?;
        let api_hosts: BTreeMap<String, Option<String>> = api_hosts_res
            .into_iter()
            .map(|x| {
                let address = get_address_for_hostkey(&x.public_key);
                (x.public_key, address)
            })
            .collect(); */

        let first_slab = &o.slabs[0];

        let planned_u_sc_per_byte: f64 = 3.07e-3 * 1.0; // TODO adjust multiplier to make full file download possible?

        let u_sc_needed_for_dl: u32 = (planned_u_sc_per_byte * (o.size as f64)).round() as u32;

        let mut hosts: BTreeMap<u8, SiaFileHost> = BTreeMap::new();
        let mut indexed_hostkeys: HashMap<String, u8> = HashMap::new();
        let mut slabs = vec![];

        for slab in &o.slabs {
            let mut slab_encryption_key = [0u8; 32];
            hex::decode_to_slice(&slab.slab.encryption_key[5..], &mut slab_encryption_key)?;
            let mut s = SiaFileSlab {
                shard_roots: BTreeMap::new(),
                slab_encryption_key,
            };
            for shard in &slab.slab.shards {
                let hostkey = shard.contracts.keys().next().unwrap();
                if get_address_for_hostkey(hostkey).is_none() {
                    log::debug!(
                        "host {} does not have web-compatible address, skipping",
                        hostkey
                    );
                    continue;
                }
                if !indexed_hostkeys.contains_key(hostkey) {
                    let host_id: u8 = indexed_hostkeys.len() as u8;

                    let mut ephemeral_account_private_key = [0u8; 32];
                    getrandom::fill(&mut ephemeral_account_private_key).unwrap();

                    let signing_key =
                        ed25519_dalek::SigningKey::from_bytes(&ephemeral_account_private_key);

                    let pubkey_str: String = signing_key.verifying_key().encode_hex();
                    let fund_req = SiaRenterdBusApiFundRequest {
                        amount: format!("{}uS", u_sc_needed_for_dl),
                        account_id: format!("{}", pubkey_str),
                        contract_id: shard
                            .contracts
                            .get(hostkey)
                            .unwrap()
                            .first()
                            .unwrap()
                            .clone(),
                    }; // TODO Maybe fund all?
                    let fund_req_str = serde_json::to_string(&fund_req)?;

                    let fund_res = self
                        .http_post(&self.bus_accounts_fund_api_url, Body::from(fund_req_str))
                        .await;

                    if let Err(err) = fund_res {
                        log::warn!("funding {hostkey} failed {}", err);
                        continue;
                    }

                    hosts.insert(
                        host_id,
                        SiaFileHost {
                            hostkey: hostkey.clone(),
                            v2_siamux_addresses: vec![get_address_for_hostkey(hostkey).unwrap()],
                            ephemeral_account_private_key,
                        },
                    );

                    indexed_hostkeys.insert(hostkey.clone(), host_id);
                }
                let mut shard_root = [0u8; 32];
                hex::decode_to_slice(&shard.root, &mut shard_root)?;
                s.shard_roots
                    .insert(*indexed_hostkeys.get(hostkey).unwrap(), shard_root.into());
            }
            slabs.push(s);
        }

        let loc = SiaFile {
            size: o.size,
            slab_size: first_slab.length,
            min_shards: first_slab.slab.min_shards,
            hosts,
            file_encryption_key: po.encryption_key,
            slabs,
        };

        Ok(loc)
    }
}

impl BlobStore for SiaBlobStore {
    type Error = crate::Error;
    async fn contains_hash(&self, _hash: s5_core::Hash) -> bool {
        todo!()
    }

    async fn import_file(&self, path: std::path::PathBuf) -> Result<(Hash, u64), Error> {
        if !path.is_absolute() {
            return Err(
                io::Error::new(io::ErrorKind::InvalidInput, "path must be absolute").into(),
            );
        }
        if !path.is_file() && !path.is_symlink() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "path is not a file or symlink",
            )
            .into());
        }
        let src = ImportSource::File(path);
        self.finalize_import(src).await
    }

    async fn import_bytes(&self, bytes: bytes::Bytes) -> Result<s5_core::Hash, Error> {
        let src = ImportSource::Memory(bytes);

        let res = self.finalize_import(src).await?;
        Ok(res.0)
    }
}

enum ImportSource {
    File(PathBuf),
    Memory(Bytes),
}

impl ImportSource {
    fn len(&self) -> io::Result<u64> {
        match self {
            Self::File(path) => std::fs::metadata(path).map(|m| m.len()),
            Self::Memory(data) => Ok(data.len() as u64),
        }
    }
}

#[derive(Serialize)]
struct SiaRenterdBusApiFundRequest {
    #[serde(rename = "accountId")]
    account_id: String,
    #[serde(rename = "contractID")]
    contract_id: String,
    amount: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct SiaRenterdBusApiHost {
    // public_key: String,
    // v2_siamux_addresses: Vec<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct SiaPinnedObjectRes {
    encryption_key: [u8; 32],
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct SiaObjectRes {
    size: u64,
    // encryption_key: String,
    slabs: Vec<SlabElement>,
}

#[derive(Deserialize)]
struct SlabElement {
    slab: SlabSlab,
    length: u64,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct SlabSlab {
    encryption_key: String,
    min_shards: u8,
    shards: Vec<Shard>,
}

#[derive(Deserialize)]
struct Shard {
    contracts: HashMap<String, Vec<String>>,
    root: String,
}
