use crate::Hash;

// TODO implement registry entry type
pub struct Entry {
    pub key_type: u8,
    pub key: [u8; 32],
    // pub tweak: u64,
    pub revision: u64,
    // pub hash_type: u8,
    pub hash: Hash,
    pub signature: Box<[u8]>,
}
