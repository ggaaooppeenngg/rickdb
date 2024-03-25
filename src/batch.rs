use std::io::Write;

use super::key::InternalKeyKind;
use integer_encoding::{VarInt,VarIntWriter};

const SEQNUM_OFFSET: usize = 0;
const COUNT_OFFSET: usize = 8;
const HEADER_SIZE: usize = 12;
// # Internal representation
//
// The internal batch representation is a contiguous byte buffer with a fixed
// 12-byte header, followed by a series of records.
//
//      +-------------+------------+--- ... ---+
//      | SeqNum (8B) | Count (4B) |  Entries  |
//      +-------------+------------+--- ... ---+
//
// Each record has a 1-byte kind tag prefix, followed by 1 or 2 length prefixed
// strings (varstring):
//
//      +-----------+-----------------+-------------------+
//      | Kind (1B) | Key (varstring) | Value (varstring) |
//      +-----------+-----------------+-------------------+
//
// A varstring is a varint32 followed by N bytes of data.
#[derive(Default)]
pub struct Batch {
    entries: Vec<u8>,
}
impl Batch {
    pub fn from(data: Vec<u8>) -> Self {
        Self { entries: data }
    }
    pub fn new() -> Self {
        let mut entries = Vec::with_capacity(128);
        entries.resize(HEADER_SIZE, 0);
        Self { entries }
    }

    pub fn count(&self) -> u32 {
        u32::from_le_bytes(self.entries[COUNT_OFFSET..HEADER_SIZE].try_into().unwrap())
    }
    fn set_count(&mut self, count: u32) {
        self.entries[COUNT_OFFSET..HEADER_SIZE].copy_from_slice(&count.to_le_bytes());
    }
    #[allow(unused)]
    pub fn seqnum(&self) -> u64 {
        u64::from_le_bytes(
            self.entries[SEQNUM_OFFSET..COUNT_OFFSET]
                .try_into()
                .unwrap(),
        )
    }
    pub fn set_seqnum(&mut self, seqnum: u64) {
        self.entries[SEQNUM_OFFSET..COUNT_OFFSET].copy_from_slice(&seqnum.to_le_bytes());
    }
    pub fn get_seqnum(&self) -> u64 {
        u64::from_le_bytes(
            self.entries[SEQNUM_OFFSET..COUNT_OFFSET]
                .try_into()
                .unwrap(),
        )
    }

    pub fn delete(&mut self, key: &[u8]) {
        self.entries.write_all(&[InternalKeyKind::Deletion as u8]).unwrap();
        self.entries.write_varint(key.len() as u32).unwrap();
        self.entries.write_all(key).unwrap();
        self.set_count(self.count() + 1);
    }
    
    pub fn put(&mut self, key: &[u8], value: &[u8]) {
        self.entries.write_all(&[InternalKeyKind::Value as u8]).unwrap();
        self.entries.write_varint(key.len() as u32).unwrap();
        self.entries.write_all(key).unwrap();
        self.entries.write_varint(value.len() as u32).unwrap();
        self.entries.write_all(value).unwrap();
        self.set_count(self.count() + 1);
    }

    #[allow(unused)]
    pub fn clear(&mut self) {
        self.entries.clear();
        self.entries.resize(HEADER_SIZE, 0);
    }
    pub fn iter(&self) -> BatchIter {
        BatchIter {
            data: &self.entries,
            offset: HEADER_SIZE,
        }
    }
    pub fn encode(mut self, seqnum: u64) -> Vec<u8> {
        self.set_seqnum(seqnum);
        self.entries
    }
}

// BatchIter
//
// A BatchIter iterates over the records in a batch.
pub struct BatchIter<'a> {
    data: &'a [u8],
    offset: usize,
}

impl<'a> Iterator for BatchIter<'a> {
    type Item = (&'a [u8], Option<&'a [u8]>, u8);

    fn next(&mut self) -> Option<Self::Item> {
        if self.offset >= self.data.len() {
            return None;
        }
        let kind = self.data[self.offset];
        self.offset += 1;
        let (key_len, sz) = u32::decode_var(&self.data[self.offset..]).unwrap();
        self.offset += sz;
        let key = &self.data[self.offset..self.offset + key_len as usize];
        self.offset += key_len as usize;
        if kind == InternalKeyKind::Deletion as u8 {
            return Some((key, None, kind));
        }
        let (value_len, sz) = u32::decode_var(&self.data[self.offset..]).unwrap();
        self.offset += sz;
        let value = &self.data[self.offset..self.offset + value_len as usize];
        self.offset += value_len as usize;
        Some((key, Some(value), kind))
    }
}
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_write_batch() {
        let mut b = Batch::new();
        let entries = [
            ("abc".as_bytes(), "def".as_bytes()),
            ("123".as_bytes(), "456".as_bytes()),
            ("xxx".as_bytes(), "yyy".as_bytes()),
            ("zzz".as_bytes(), "".as_bytes()),
            ("010".as_bytes(), "".as_bytes()),
        ];

        for &(k, v) in entries.iter() {
            if !v.is_empty() {
                b.put(k, v);
            } else {
                b.delete(k)
            }
        }

        eprintln!("{:?}", b.entries);
        assert_eq!(b.iter().count(), 5);

        let mut i = 0;

        for (k, v, _) in b.iter() {
            assert_eq!(k, entries[i].0);

            match v {
                None => assert!(entries[i].1.is_empty()),
                Some(v_) => assert_eq!(v_, entries[i].1),
            }

            i += 1;
        }

        assert_eq!(i, 5);
        assert_eq!(b.encode(1).len(), 49);
    }
}
