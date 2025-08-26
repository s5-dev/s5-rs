use crate::Hash;

pub trait Pinning: Sync + Send {
    fn hash_pin(&self, hash: Hash, ctx: PinningContext) -> impl std::future::Future<Output = ()> + Send;
    fn hash_unpin(&self, hash: Hash, ctx: PinningContext) -> impl std::future::Future<Output = ()> + Send;
    fn hash_unpin_all(&self, hash: Hash) -> impl std::future::Future<Output = ()> + Send;
}

pub struct PinningContext {
    pub id: [u8; 16],
}
