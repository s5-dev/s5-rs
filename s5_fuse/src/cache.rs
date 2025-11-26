use dashmap::DashMap;
use fuser::{FileAttr, FileType};
use fuser_async::DirEntry;
use s5_fs::dir::DirV1;
use std::collections::BTreeMap;
use std::sync::Arc;

#[derive(Clone)]
pub struct CachedDir {
    pub dir: DirV1,
    pub keys: BTreeMap<u8, [u8; 32]>,
}

pub struct FsCache {
    pub attrs: DashMap<u64, FileAttr>,
    pub parents: DashMap<u64, u64>,
    pub dirs: DashMap<u64, CachedDir>,
    pub dir_entries: DashMap<u64, Arc<Vec<DirEntryMeta>>>,
}

impl Default for FsCache {
    fn default() -> Self {
        Self::new()
    }
}

impl FsCache {
    pub fn new() -> Self {
        Self {
            attrs: DashMap::new(),
            parents: DashMap::new(),
            dirs: DashMap::new(),
            dir_entries: DashMap::new(),
        }
    }
}

pub struct OpenFile {
    pub path: String,
    pub buf: Vec<u8>,
}

#[derive(Clone)]
pub struct DirEntryMeta {
    pub inode: u64,
    pub file_type: FileType,
    pub name: String,
}

pub struct DirEntryIter {
    pub entries: Arc<Vec<DirEntryMeta>>,
    pub index: usize,
}

impl Iterator for DirEntryIter {
    type Item = DirEntry;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index >= self.entries.len() {
            None
        } else {
            let meta = &self.entries[self.index];
            self.index += 1;
            Some(DirEntry {
                inode: meta.inode,
                file_type: meta.file_type,
                name: meta.name.clone(),
            })
        }
    }
}
