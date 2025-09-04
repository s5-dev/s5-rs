mod config;
mod store;

pub use config::SiaStoreConfig;
pub use store::SiaStore;

use hex::FromHexError;
use std::string::FromUtf8Error;
use thiserror::Error;

pub type SiaStoreResult<T, E = Error> = std::result::Result<T, E>;

#[derive(Error, Debug)]
#[non_exhaustive]
pub enum Error {
    #[error(
        "renterd config error: disable upload packing in your renterd settings, this is not supported yet"
    )]
    RenterdPackingEnabled,

    #[error(
        "renterd config error: set min shards to 1 in your renterd redundancy settings, erasure coding is not supported by s5 yet"
    )]
    RenterdErasureCodingEnabled,

    #[error("host not found on siascan")]
    HostNotFoundOnSiascan,

    #[error("Got HTTP {0} with content '{1}'")]
    HttpFailWithBody(u16, String),
    #[error("Got unexpected HTTP {0}")]
    HttpFail(u16),

    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Http(#[from] http::Error),
    #[error(transparent)]
    Hyper(#[from] hyper::Error),
    #[error(transparent)]
    FromUtf8Error(#[from] FromUtf8Error),
    #[error(transparent)]
    SerdeJson(#[from] serde_json::Error),
    #[error(transparent)]
    Hex(#[from] FromHexError),
    #[error(transparent)]
    HttpInvalidHeaderValue(#[from] http::header::InvalidHeaderValue),
}
