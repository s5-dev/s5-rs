use crate::{Entry, PublicKeyEd25519};

pub trait RegistryRead: Sync + Send {
    fn registry_get(
        &self,
        pk: PublicKeyEd25519,
    ) -> impl std::future::Future<Output = Option<Entry>> + Send;

    fn registry_listen(
        &self,
        pk: PublicKeyEd25519,
    ) -> impl std::future::Future<Output = impl futures_core::Stream<Item = Entry>> + Send;
}

pub trait RegistryWrite: Sync + Send {
    fn registry_set(&self, entry: Entry) -> impl std::future::Future<Output = ()> + Send;
}
