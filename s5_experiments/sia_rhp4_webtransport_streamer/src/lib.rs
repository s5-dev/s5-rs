use blake2::digest::consts::U32;
use blake2::{Blake2b, Digest};
use bytes::Bytes;
use chacha20::XChaCha20;
use chacha20::cipher::KeyIvInit;
use chacha20::cipher::StreamCipher;
use chacha20::cipher::StreamCipherSeek;
use dashmap::DashMap;
use ed25519::signature::SignerMut;
use fs5::BlobId;
use fs5::FileRef;
use futures::future::{Either, select_all};
use log::Level;
use log::info;
use log::warn;
use s5_core::blob::location::BlobLocation;
use s5_core::blob::location::SiaFile;
use s5_core::blob::location::SiaFileHost;
use sia::encoding::{SiaDecodable, SiaEncodable};
use sia::rhp::AccountToken;
use sia::rhp::HostPrices;
use sia::rhp::RPCReadSectorRequest;
use sia::rhp::RPCReadSectorResponse;
use sia::rhp::RPCSettingsResponse;
use sia::rhp::RPCWriteSectorResponse;
use sia::signing::PublicKey;
use sia::types::Hash256;
use std::collections::{BTreeMap, HashMap};
use std::io::Cursor;
use std::io::Seek;
use std::sync::Arc;
use time::OffsetDateTime;
use url::Url;
use wasm_bindgen::prelude::*;
use web_transport::{ClientBuilder, Session};

pub type Blake2b256 = Blake2b<U32>;

const SIA_LEAF_SIZE: u64 = 64;

#[wasm_bindgen(start)]
pub fn main() -> Result<(), JsValue> {
    let _ = console_log::init_with_level(Level::Debug);
    //#[cfg(debug_assertions)]
    // TODO console_error_panic_hook::set_once();
    Ok(())
}

#[wasm_bindgen]
pub struct SiaTransporter {
    sessions: DashMap<String, Session>,
    host_prices: DashMap<String, Arc<HostPrices>>,
}

#[wasm_bindgen]
pub struct JSFileRef(FileRef);

#[wasm_bindgen]

impl JSFileRef {
    pub fn size(&self) -> u64 {
        self.0.size
    }
    pub fn media_type(&self) -> String {
        self.0
            .media_type
            .clone()
            .unwrap_or("application/octet-stream".to_string())
    }
    pub fn blob_id(&self) -> String {
        BlobId::new(self.0.hash.into(), self.0.size).to_base32()
    }
}

#[wasm_bindgen]
impl SiaTransporter {
    pub fn new() -> Self {
        Self {
            sessions: DashMap::new(),
            host_prices: DashMap::new(),
        }
    }
    async fn session_for(&self, siamux_address: &str) -> anyhow::Result<Session> {
        if self.sessions.contains_key(siamux_address) {
            let sess: Session = self.sessions.get(siamux_address).unwrap().value().clone();
            return Ok(sess);
        }
        let client = ClientBuilder::new()
            // .with_unreliable(true)
            // .with_congestion_control(web_transport::CongestionControl::Default)
            .with_system_roots()
            .unwrap();

        let session = client
            .connect(Url::parse(&format!(
                "https://{}/sia/rhp/v4",
                siamux_address
            ))?)
            .await
            .unwrap();

        self.sessions
            .insert(siamux_address.to_string(), session.clone());

        Ok(session)
    }

    // ! main entrypoint
    pub async fn download_file_slice(
        &self,
        path: String,
        offset: u64,
        length: u64,
    ) -> Result<Vec<u8>, SiaDownloadError> {
        let bytes: Bytes = hex::decode(BLOB_TMP_HEX).unwrap().try_into().unwrap();

        match BlobLocation::deserialize(&bytes) {
            BlobLocation::SiaFile(file) => {
                info!("sia file {:?}", file);
                let res = self.download_slice(file, offset, length).await;
                res.map(|bytes| bytes.into())
            }
            _ => Err(SiaDownloadError::NoBlobLocation),
        }
    }

    pub async fn get_file_meta(&self, path: String) -> JSFileRef {
        // TODO add fs5 bindings to make this dynamic
        let blob_id =
            BlobId::parse("blobb5wkz67thkc6lvh6biam6it2k26n3s4etjxpkughm3d3zxohfs4mi77ewgki");
        let mut file_ref: FileRef = blob_id.into();
        file_ref.media_type = Some("video/mp4".to_string());
        JSFileRef(file_ref)
    }

    async fn get_prices_for_host(
        &self,
        host: &SiaFileHost,
    ) -> Result<Arc<HostPrices>, SiaDownloadError> {
        if self.host_prices.contains_key(&host.hostkey) {
            return Ok(self.host_prices.get(&host.hostkey).unwrap().value().clone());
        }
        info!("host addresses {:?}", host.v2_siamux_addresses);
        let address = host.v2_siamux_addresses.get(0).unwrap();
        let res: Result<RPCSettingsResponse, SiaDownloadError> = self
            .get_rpc_settings_for_host(&address)
            .await
            .map_err(From::from);

        let prices = Arc::new(res?.settings.prices);
        info!("SUCCESS {} got host prices {:?}", address, prices);

        self.host_prices
            .insert(host.hostkey.clone(), prices.clone());

        Ok(prices)
    }

    async fn download_slice(
        &self,
        file: SiaFile,
        mut offset: u64,
        mut length: u64,
    ) -> Result<Bytes, SiaDownloadError> {
        if offset.checked_add(length).unwrap_or(u64::MAX) > file.size {
            return Err(SiaDownloadError::Custom(format!(
                "requested byte range [{offset}, {length}) exceeds file length ({})",
                file.size,
            )));
        }
        if length == 0 {
            return Ok(Bytes::new());
        }

        let expected_size = length as usize;

        let slab_size = file.slab_size;
        let start_slab_id = offset / slab_size;
        let end_slab_id = (offset + length - 1) / slab_size;

        let mut out: Vec<u8> = Vec::with_capacity(length as usize);

        let valid_until_unix = (js_sys::Date::now() / 1000f64).round() as u64 + 10 * 60;

        let mut tokens: BTreeMap<u8, AccountToken> = BTreeMap::new();

        info!("download_slice {} {}", offset, length);
        // let mut remaining = length;

        'slab_loop: for slab_index in start_slab_id..=end_slab_id {
            let slab = file.slabs.get(slab_index as usize).unwrap();
            info!("dl slab {}", slab_index);
            // let mut downloaded =

            // TODO implement reed solomon support

            let mut futures: Vec<_> = vec![];

            for (host_id, root) in &slab.shard_roots {
                if let Some(host) = file.hosts.get(&host_id) {
                    if !tokens.contains_key(&host_id) {
                        //info!("make account token for {:?}", host.v2_siamux_addresses);
                        let host_key = PublicKey::new(
                            hex::decode(&host.hostkey[8..]).unwrap().try_into().unwrap(),
                        );
                        let mut signing_key = ed25519_dalek::SigningKey::from_bytes(
                            &host.ephemeral_account_private_key,
                        );
                        let public_key = PublicKey::new(*signing_key.verifying_key().as_bytes());

                        let valid_until =
                            OffsetDateTime::from_unix_timestamp(valid_until_unix as i64).unwrap();

                        //info!("make account token step 4");

                        // OffsetDateTime::now_utc()        .replace_offset(UtcOffset::from_whole_seconds(10 * 60).unwrap());

                        let mut hasher = Blake2b256::new();
                        let mut sig_hash = [0u8; 32];
                        hasher.update(host_key);
                        hasher.update(public_key);
                        hasher.update(valid_until_unix.to_le_bytes());
                        hasher.finalize_into((&mut sig_hash).into());

                        let signature = signing_key.sign(&sig_hash);

                        let token = AccountToken {
                            account: public_key,
                            host_key,
                            signature: sia::signing::Signature::new(signature.to_bytes()),
                            valid_until,
                        };
                        tokens.insert(*host_id, token);
                    };
                    let token = tokens.get(&host_id).unwrap();
                    let mut tmp_data_1 = Vec::new();
                    token.encode(&mut tmp_data_1).unwrap();
                    let mut tmp_cursor_1 = Cursor::new(tmp_data_1);
                    futures.push(Box::pin(self.try_host_dl(
                        slab_size,
                        (*root).into(),
                        *host_id,
                        &host,
                        AccountToken::decode(&mut tmp_cursor_1).unwrap(),
                        offset,
                        length,
                        slab.slab_encryption_key,
                        file.file_encryption_key,
                    )));
                };
            }
            while !futures.is_empty() {
                match select_all(futures).await {
                    (Ok((hostkey, data)), _index, _remaining) => {
                        info!("fastest host: {}", hostkey,);

                        out.extend_from_slice(&data);
                        offset = offset + (data.len() as u64);
                        length = length - (data.len() as u64);

                        continue 'slab_loop;
                    }
                    (Err(e), index, remaining) => {
                        warn!("could not download sector from this host: {}", e);
                        futures = remaining;
                    }
                }
            }

            return Err(SiaDownloadError::NoHostAvailableForSlab(slab_index));
        }

        if out.len() > expected_size {
            out.drain(expected_size..);
        }

        return Ok(out.into());
    }

    async fn try_host_dl(
        &self,
        slab_size: u64,
        root: [u8; 32],
        host_id: u8,
        host: &SiaFileHost,
        token: AccountToken,
        offset: u64,
        length: u64,
        slab_encryption_key: [u8; 32],
        file_encryption_key: [u8; 32],
    ) -> Result<(String, Vec<u8>), SiaDownloadError> {
        info!("try shard by {:?}", host.v2_siamux_addresses);

        let mut tmp_data_2 = Vec::new();
        let prices = self.get_prices_for_host(host).await;

        if prices.is_err() {
            info!("failed to get prices, trying next host...");
            return Err(SiaDownloadError::HostNotAvailable(host.hostkey.clone()));
        }

        let _ = &prices.unwrap().encode(&mut tmp_data_2).unwrap();
        let mut tmp_cursor_2 = Cursor::new(tmp_data_2);

        let mut read_sector_len = length.min(slab_size - (offset % slab_size));

        while (offset + read_sector_len) % SIA_LEAF_SIZE != 0 {
            read_sector_len += 1;
        }

        let read_req = RPCReadSectorRequest {
            prices: HostPrices::decode(&mut tmp_cursor_2).unwrap(),
            length: read_sector_len,
            offset: offset % slab_size,
            root: Hash256::new(root),
            token,
        };
        // info!("sending read sector request {:?}", read_req);
        let read_req = encode_read_sector_request(&read_req);

        // let mut data: Vec<u8> = Vec::new();

        let mut session = self
            .session_for(host.v2_siamux_addresses.get(0).unwrap())
            .await?;
        let (mut send, mut recv) = session.open_bi().await.unwrap();
        send.write(&read_req).await.unwrap();

        // send.finish().unwrap();

        let mut buf: Vec<u8> = vec![];
        loop {
            let res = recv.read_buf(&mut buf).await.unwrap();
            if res.is_none() {
                break;
            };
        }

        let buf_copy = if buf.len() < 1000 {
            buf.clone()
        } else {
            vec![]
        };

        let mut c = Cursor::new(buf);
        c.seek(std::io::SeekFrom::Start(1)).unwrap();
        let read_res = RPCReadSectorResponse::decode(&mut c).map_err(|_| {
            SiaDownloadError::RPCReadSectorError(String::from_utf8_lossy(&buf_copy).to_string())
        });

        if read_res.is_err() {
            warn!(
                "could not download shard from {:?}: {}",
                host.v2_siamux_addresses,
                read_res.unwrap_err()
            );
            return Err(SiaDownloadError::HostNotAvailable(host.hostkey.clone()));
        }

        let res = read_res.unwrap();

        // TODO verify proof or via blake3/bao tree

        let mut decrypted_shard_bytes = res.data.to_vec();

        if (decrypted_shard_bytes.len() as u64) != read_sector_len {
            warn!(
                "host {} slab wrong length {} != {read_sector_len}",
                host.hostkey,
                decrypted_shard_bytes.len()
            );
            return Err(SiaDownloadError::HostNotAvailable(host.hostkey.clone()));
        }

        {
            let mut shard_nonce = [0u8; 24];
            shard_nonce[1] = host_id;
            let key = chacha20::Key::from_slice(&slab_encryption_key);
            let iv = chacha20::XNonce::from_slice(&shard_nonce);
            let mut cipher = XChaCha20::new(key, iv);
            cipher.seek(offset % slab_size);
            cipher.apply_keystream(&mut decrypted_shard_bytes);
        }

        {
            let mut slab_nonce = [0u8; 24];
            let overflow_limit = 64 * (u32::MAX as u64);

            let offset = if offset >= overflow_limit {
                let nonce64: u64 = offset / overflow_limit;
                slab_nonce[16..].copy_from_slice(&nonce64.to_le_bytes());
                offset % overflow_limit
            } else {
                offset
            };
            let key = chacha20::Key::from_slice(&file_encryption_key);
            let iv = chacha20::XNonce::from_slice(&slab_nonce);
            let mut cipher = XChaCha20::new(key, iv);
            cipher.seek(offset);
            cipher.apply_keystream(&mut decrypted_shard_bytes);
        }

        Ok((host.hostkey.clone(), decrypted_shard_bytes))
    }

    async fn get_rpc_settings_for_host(
        &self,
        siamux_address: &str,
    ) -> anyhow::Result<RPCSettingsResponse> {
        let mut session = self.session_for(siamux_address).await?;
        let req = encode_rpc_settings_request();
        let (mut send, mut recv) = session.open_bi().await.unwrap();
        send.write(&req).await.unwrap();
        send.finish().unwrap();
        let mut buf: Vec<u8> = vec![];
        let _ = recv.read_buf(&mut buf).await.unwrap();
        let mut c = Cursor::new(buf);
        c.seek(std::io::SeekFrom::Start(1)).unwrap();
        Ok(RPCSettingsResponse::decode(&mut c).unwrap())
    }
}

pub fn encode_rpc_settings_request() -> Vec<u8> {
    let mut specifier = [0u8; 16];
    specifier[..8].copy_from_slice("Settings".as_bytes());
    println!("{:?}", specifier);
    specifier.to_vec()
}

pub fn encode_read_sector_request(req: &RPCReadSectorRequest) -> Vec<u8> {
    let mut data = Vec::new();

    let mut specifier = [0u8; 16];
    specifier[..10].copy_from_slice("ReadSector".as_bytes());

    data.extend_from_slice(&specifier);

    req.encode(&mut data).unwrap();
    data
}

pub fn decode_write_sectors_response(mut data: &[u8]) -> RPCWriteSectorResponse {
    RPCWriteSectorResponse::decode(&mut data).unwrap()
}

#[derive(thiserror::Error, Debug)]
pub enum SiaDownloadError {
    #[error("no host available for slab {0}")]
    NoHostAvailableForSlab(u64),
    #[error("{0}")]
    Custom(String),

    #[error("failed to read sector: {0}")]
    RPCReadSectorError(String),

    #[error("no supported location available to download blob")]
    NoBlobLocation,

    #[error("host {0} not available")]
    HostNotAvailable(String),

    #[error(transparent)]
    Other(anyhow::Error),
}

impl Into<JsValue> for SiaDownloadError {
    fn into(self) -> JsValue {
        self.to_string().into()
    }
}

impl From<anyhow::Error> for SiaDownloadError {
    fn from(err: anyhow::Error) -> Self {
        SiaDownloadError::Other(err)
    }
}

const BLOB_TMP_HEX: &str = "821841861a00bfffff1a0040000001a200837848656432353531393a3730376431383964666430383436396566656266316135313333386233643733656564346163316235363231336130656436353231313739663630613762373181781d686f7374642d746573742e686f77697474732e636f2e756b3a393838345820d29dc9df5c8e4289f2feb85016db2e520c66e2641deaa2434f53e2108dae3e7a01837848656432353531393a363965393532386631383034366266393637623634393633663732646166336161373265373033323463663130386434333932636265643962623664663231648177686f7374642e7a656e2e736961352e6e65743a3939383458204a74f0e563ece5251e6bf849800179162a8ce2ce176f2501033ef848fa44c4915820675fbccd16c83d8dc65b45b9a1f0b0e53af1e21553a74fb97ec84bcb6a2f3bf38382582077b3870c975c87df87f0e7d10204fc9e4d4f3229028dda5156855eb35211b053a2005820379411eff934c9c9fe3325cab900ca87d6eacff2cce778f705148d8bdeaf33e5015820bff25e17157e3970a997fe1fbac939d23f5ce04c71a762f15e7a2ba6e153689982582031e6f6861ed0430bf72ad782962005fe26c4f01cece719339136e558a78c6381a2005820b19c2d1d74002dbc9dd9676861047fb8588747e74914c27bc529a647b750a355015820ffdfae0893ef5eb13a139fd37700c8b58aed6a475483db862f3057d16193ac188258201a0888270817125460dfd69b044702b4022c444b84a59a87cd8c3b15301e4f8ca200582015d8eb632b24b84f7be336f9817880b38590e3b27b29f543b8d8910cec8dcdb701582081ddca7e4ed27733fe18c1ae5480318dc5e3e506dc822cc7ceff43eb9452d493";
