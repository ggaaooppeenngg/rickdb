use crate::key::InternalKeyKind;

use super::key::InternalKey;
use log::debug;
use skiplist::skipmap::SkipMap;
use std::ops::Bound::{Included};
// MemTable struct
// The MemTable struct is a simple wrapper around a SkipMap. The SkipMap is a lock-free, concurrent, ordered map. It is a probabilistic data structure that uses a hierarchy of linked lists with random levels. It is similar to a skip list, but with a few differences. The SkipMap is used to store the key-value pairs in the MemTable. The MemTable is a write-only data structure that is used to store the key-value pairs that are written to the database. The MemTable is used to store the key-value pairs in memory before they are written to the disk. The MemTable is a sorted map that is used to store the key-value pairs in memory. The MemTable is a write-only data structure that is used to store the key-value pairs that are written to the database. The MemTable is a sorted map that is used to store the key-value pairs in memory. The MemTable is a write-only data structure that is used to store the key-value pairs that are written to the database. The MemTable is a sorted map that is used to store the key-value pairs in memory. The MemTable is a write-only data structure that is used to store the key-value pairs that are written to the database. The MemTable is a sorted map that is used to store the key-value pairs in memory. The MemTable is a write-only data structure that is used to store the key-value pairs that are written to the database. The MemTable is a sorted map that is used to store the key-value pairs in memory. The MemTable is a write-only data structure that is used to store the key-value pairs that are written to the database. The MemTable is a sorted map that is used to store the key-value pairs in memory. The MemTable is a write-only data structure that is used to store the key-value pairs that are written to the database. The MemTable is a sorted map that is used to store the key-value pairs in memory. The MemTable is a write-only data structure that is used to store the key-value pairs that are written to the database. The MemTable is a sorted map that is used to store the key-value pairs in memory. The MemTable is a write-only data structure that is used to store the key-value pairs that are written to the database. The MemTable is a sorted map that is used to store the key-value pairs in memory. The MemTable is a write-only
// Path: src/memtable.rs
#[derive(Default)]
pub(crate) struct MemTable {
    size: usize,
    skl: SkipMap<InternalKey, Vec<u8>>,
}
impl MemTable {
    pub fn new() -> Self {
        Self {
            size: 0,
            skl: SkipMap::new(),
        }
    }
    pub fn smallest(&self) -> Option<InternalKey> {
        self.skl.front().map(|(k, _)| k.clone())
    }
    pub fn largest(&self) -> Option<InternalKey> {
        self.skl.back().map(|(k, _)| k.clone())
    }
    pub fn is_empty(&self) -> bool {
        self.skl.is_empty()
    }
    pub fn approximate_memory_usage(&self) -> usize {
        self.size
    }
    pub fn range_all(&self) -> Vec<(InternalKey, Vec<u8>)> {
        self.skl
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }
    pub fn insert(&mut self, key: InternalKey, value: Vec<u8>) {
        self.size += key.size() + value.len();
        self.skl.insert(key, value);
    }
    pub fn get_ge(&self, lookup: &InternalKey) -> Option<Vec<u8>> {
        if let Some((key, value)) = self.skl.lower_bound(Included(lookup)) {
            if key.trailer() & 0xff != InternalKeyKind::Deletion as u64{
                debug!("lookup key {lookup}, found key {key}");
                return Some(value.clone());
            }
        }
        None
    }
    pub fn get(&self, key: &InternalKey) -> Option<Vec<u8>> {
        self.skl.get(key).cloned()
    }
    pub fn iter(&self) -> impl Iterator<Item = (&InternalKey, &Vec<u8>)> {
        self.skl.iter()
    }
    pub fn size(&self) -> usize {
        self.size
    }
    pub fn clear(&mut self) {
        self.size = 0;
        self.skl.clear();
    }
}
