use crate::{Message, PublicKeyEd25519};

pub trait StreamsRead: Sync + Send {
    fn stream_subscribe(
        &self,
        pk: PublicKeyEd25519,
        after_revision: Option<u64>,
    ) -> impl std::future::Future<Output = impl futures_core::Stream<Item = Message>> + Send;
}

pub trait StreamsWrite: Sync + Send {
    fn stream_publish(&self, msg: Message) -> impl std::future::Future<Output = ()> + Send;
}
