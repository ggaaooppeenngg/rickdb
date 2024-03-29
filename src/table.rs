use super::block::{Block, BlockBuilder, BlockCache, BlockIterator};
use super::bloom_filter::BloomFilter;
use super::cache::LRUCache;
use super::options::Options;

use anyhow::Result;
use integer_encoding::{VarInt, VarIntWriter};
use std::cell::RefCell;
use std::cmp::Ordering;
use std::fs::File;
use std::io::{Read, Seek, Write};
use std::iter::Peekable;
use std::rc::Rc;
use thiserror::Error;

#[allow(unused)]
pub const RESTART_INTERVAL: usize = 32;
#[allow(unused)]
pub const BLOCK_SIZE: usize = 4096;
#[allow(unused)]
pub const FILTER_BITS_PER_KEY: usize = 10;

// offset, size
#[allow(dead_code)]
#[derive(Debug, Clone, Copy)]
struct BlockHandle(u64, u64);
impl BlockHandle {
    #[allow(unused)]
    fn encode(&self) -> Vec<u8> {
        let mut buf = vec![];
        buf.write_varint(self.0);
        buf.write_varint(self.1);
        buf
    }
    #[allow(unused)]
    fn decode(data: &[u8]) -> Self {
        let (offset, sz) = u64::decode_var(data).unwrap();
        let (size, _) = u64::decode_var(&data[sz..]).unwrap();
        Self(offset, size)
    }
}
struct FullFilterBlockBuilder {
    buffer: Vec<u8>,
    // current keys
    keys: Vec<Vec<u8>>,
}
impl FullFilterBlockBuilder {
    #[allow(unused)]
    pub(super) fn new() -> Self {
        Self {
            buffer: vec![],
            keys: vec![],
        }
    }
    #[allow(unused)]
    pub(super) fn add_key(&mut self, key: &[u8]) {
        self.keys.push(key.to_vec());
    }
    #[allow(unused)]
    pub(super) fn finish(&mut self) -> Result<Vec<u8>> {
        let mut filter = BloomFilter::new(FILTER_BITS_PER_KEY, self.keys.len());
        for key in &self.keys {
            filter.add(key);
        }
        let filter = filter.encode();
        self.buffer.write_all(&filter)?;
        Ok(self.buffer.clone())
    }
}
#[allow(unused)]
pub struct TableBuilder<T: Write> {
    dst: T,
    opt: Options,
    offset: usize,
    data_block_builder: BlockBuilder,
    last_key: Vec<u8>,
    index_builder: BlockBuilder,
    filter_block_builder: FullFilterBlockBuilder,
    meta_index_block_builder: BlockBuilder,
}

impl<T: Write> TableBuilder<T> {
    pub fn new(dst: T, opt: Options) -> Self {
        let mut restart_1_opt = opt;
        restart_1_opt.block_restart_interval = 1;
        Self {
            dst,
            opt,
            offset: 0,
            data_block_builder: BlockBuilder::new(opt.block_restart_interval, opt),
            last_key: vec![],
            filter_block_builder: FullFilterBlockBuilder::new(),
            index_builder: BlockBuilder::new(1, restart_1_opt),
            meta_index_block_builder: BlockBuilder::new(1, restart_1_opt),
        }
    }
    #[allow(unused)]
    fn default(dst: T) -> Self {
        Self::new(dst, Options::default())
    }
    pub fn add(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        if self.data_block_builder.size_estimate() >= self.opt.block_size {
            let block = self.data_block_builder.finish()?;
            self.dst.write_all(&block)?;
            // TODO: compress data block and add checksum
            // offset += 1 + 4
            // 1 代表压缩类型 4 代表checksum
            // update index
            // println!(
            //     "prev block last key {} and key {}",
            //     String::from_utf8_lossy(&self.last_key),
            //     String::from_utf8_lossy((&key))
            // );
            (self.opt.cmp.separator)(&mut self.last_key, key);
            // println!("seperator {}", String::from_utf8_lossy(&self.last_key));
            self.index_builder.add(
                &self.last_key,
                &BlockHandle(self.offset as u64, block.len() as u64).encode(),
            )?;
            self.offset += block.len();
            self.data_block_builder.reset();
        }
        self.last_key = key.to_vec();
        self.data_block_builder.add(key, value)?;
        self.filter_block_builder.add_key(key);
        // println!(
        //     "add key {} and value {}",
        //     String::from_utf8_lossy(key),
        //     String::from_utf8_lossy(value)
        // );
        // println!("estimate size {}", self.data_block_builder.size_estimate());
        Ok(())
    }
    #[allow(unused)]
    pub fn finish(mut self) -> Result<()> {
        let block = self.data_block_builder.finish()?;
        let offset = self.offset;
        self.dst.write_all(&block)?;
        self.offset += block.len();

        // order: filter < index
        // filter
        let filter = self.filter_block_builder.finish()?;
        // println!("filter offset {}", self.offset);
        self.dst.write_all(&filter)?; // filter block
        let filter_handle = BlockHandle(self.offset as u64, filter.len() as u64);
        self.offset += filter.len();
        self.meta_index_block_builder
            .add(b"filter", &filter_handle.encode())?;

        // index
        (self.opt.cmp.successor)(&mut self.last_key);
        // println!("add last seperator {}", String::from_utf8_lossy(&self.last_key));
        // println!("offset {}", offset);
        // println!("block len {}", block.len());
        self.index_builder.add(
            &self.last_key,
            &BlockHandle(offset as u64, block.len() as u64).encode(),
        )?;
        let index_block = self.index_builder.finish()?;
        self.dst.write_all(&index_block)?;
        self.meta_index_block_builder.add(
            b"index",
            &BlockHandle(self.offset as u64, index_block.len() as u64).encode(),
        )?;
        self.offset += index_block.len();

        // meta index
        let meta_index_block = self.meta_index_block_builder.finish()?;
        self.dst.write_all(&meta_index_block)?; // meta index block
        let meta_index_handle = BlockHandle(self.offset as u64, meta_index_block.len() as u64);
        // footer
        let mut footer = [0u8; 40];
        let ih = meta_index_handle.encode();
        footer[..ih.len()].copy_from_slice(&ih);
        footer[36..40].copy_from_slice(b"kvdb");
        // write footer
        self.dst.write_all(&footer[..])?;

        // BlockHandler varint64 varint64
        // 40 bytes

        Ok(())
    }
}

#[allow(unused)]
pub struct TableIterator<T> {
    reader: Rc<RefCell<T>>,
    index_block_iter: BlockIterator,
    data_block_iter: Option<BlockIterator>,
    opt: Options,
}

impl<T: Read + Seek> Iterator for TableIterator<T> {
    // we will be counting with usize
    type Item = (Vec<u8>, Vec<u8>);

    fn next(&mut self) -> Option<Self::Item> {
        if self.data_block_iter.is_none() {
            let (_key, bh) = self.index_block_iter.next()?;
            // println!("sep {:?}", String::from_utf8_lossy(&key));
            let bh = BlockHandle::decode(&bh[..]);
            // println!("bh value from index block {:?}", bh);
            let mut value = vec![0u8; bh.1 as usize];
            self.reader
                .borrow_mut()
                .seek(std::io::SeekFrom::Start(bh.0))
                .expect("seek error");
            self.reader
                .borrow_mut()
                .read_exact(&mut value[..])
                .expect("read exact");
            self.data_block_iter = Block::new(&value[..], self.opt)
                .ok()
                .map(|block| block.iter());
        }
        if let Some(kv) = self.data_block_iter.as_mut().unwrap().next() {
            Some(kv)
        } else {
            let (_key, bh) = self.index_block_iter.next()?;
            // println!("sep {:?}", String::from_utf8_lossy(&key));
            let bh = BlockHandle::decode(&bh[..]);
            // println!("bh value from index block {:?}", bh);
            let mut value = vec![0u8; bh.1 as usize];
            self.reader
                .borrow_mut()
                .seek(std::io::SeekFrom::Start(bh.0))
                .expect("seek error");
            self.reader
                .borrow_mut()
                .read_exact(&mut value[..])
                .expect("read exact");
            self.data_block_iter = Block::new(&value[..], self.opt)
                .ok()
                .map(|block| block.iter());
            return self.data_block_iter.as_mut().unwrap().next();
        }
    }
}

#[allow(unused)]
pub struct Table<T: Read + Seek> {
    reader: Rc<RefCell<T>>,
    total_size: usize,
    index_block: Block,
    filter_block: BloomFilter,
    block_cache: BlockCache,
    opt: Options,
}

/// MyErrors enumerates all possible errors returned by this library.
#[derive(Error, Debug)]
pub enum Errors {
    /// Represents invalid file error
    #[error("invalid store file")]
    InvalidStoreFile,
    #[error("block not found: {0}")]
    BlockNotFound(String),
    // #[error("invalid checksum")]
    // ChecksumError,
    /// Represents all other cases of `std::io::Error`.
    #[error(transparent)]
    IOError(#[from] std::io::Error),
}

// 这里的 static 在 Trait bound有点不一样
// 这里表示 T 是有所有权的，所有有所有权的变量都可以过在trait bound中的static
// 他代表你不drop他，他会一直有效。
// 直接用  static 也可以。
// https://doc.rust-lang.org/rust-by-example/scope/lifetime/static_lifetime.html#trait-bound
// https://github.com/pretzelhammer/rust-blog/blob/master/posts/common-rust-lifetime-misconceptions.md
impl<'a, T: Read + Seek + 'a> Table<T> {
    // pub fn iter_no_dyn(&self) -> impl Iterator<Item = (Vec<u8>, Vec<u8>)>{
    //     TableIterator {
    //         reader: self.reader.clone(),
    //         data_block_iter: None,
    //         index_block_iter: self.index_block.clone().iter(),
    //         opt: self.opt,
    //     }
    // }
    #[allow(unused)]
    pub fn iter(&self) -> Box<dyn Iterator<Item = (Vec<u8>, Vec<u8>)> + 'a> {
        Box::new(TableIterator {
            reader: self.reader.clone(),
            data_block_iter: None,
            index_block_iter: self.index_block.clone().iter(),
            opt: self.opt,
        })
    }
    #[allow(unused)]
    pub fn size(&self) -> usize {
        self.total_size
    }
    #[allow(unused)]
    pub fn smallest(&mut self) -> Option<Vec<u8>> {
        self.iter().next().map(|(k, _v)| k)
    }
    #[allow(unused)]
    pub fn biggest(&mut self) -> Option<Vec<u8>> {
        self.index_block
            .clone()
            .iter()
            .last()
            .map(|(_k, bh)| bh)
            .map(|bh| {
                let bh = BlockHandle::decode(&bh[..]);
                let mut buf = vec![0u8; bh.1 as usize];
                self.reader
                    .borrow_mut()
                    .seek(std::io::SeekFrom::Start(bh.0))
                    .unwrap();
                self.reader.borrow_mut().read_exact(&mut buf[..]).unwrap();
                let data_block = Block::new(&buf[..], self.opt).unwrap();
                data_block.iter().last().map(|(k, _v)| k).unwrap()
            })
    }

    #[allow(unused)]
    pub fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        if self.filter_block.may_contain(key) {
            // println!("index block get le {}", String::from_utf8_lossy(key));
            if let Some(value) = self.index_block.get_ge(key) {
                // println!("value {:?}", value);
                let bh = BlockHandle::decode(&value[..]);
                // println!("get block {}", bh.0);
                let data_block = self.block_cache.get_block_or(&bh.0, || {
                    // println!("not cached {}", bh.0);
                    let mut buf = vec![0u8; bh.1 as usize];
                    self.reader
                        .borrow_mut()
                        .seek(std::io::SeekFrom::Start(bh.0));
                    self.reader.borrow_mut().read_exact(&mut buf[..]).unwrap();
                    Block::new(&buf[..], self.opt).unwrap()
                });
                // println!("get data block in get");
                return Ok(data_block.get(key));
            } else {
                // println!("OK");
                return Ok(None);
            }
        }
        Ok(None)
    }
    #[allow(unused)]
    pub fn from_reader(mut reader: T, opt: Options) -> Result<Self, Errors> {
        reader.seek(std::io::SeekFrom::End(-40))?; // 40 bytes
        let mut footer = [0u8; 40];
        reader.read_exact(&mut footer)?;
        let meta_index_block_handle = BlockHandle::decode(&footer);
        if b"kvdb" != &footer[36..40] {
            return Err(Errors::InvalidStoreFile);
        }
        // offset + len + footer length
        let total_size = meta_index_block_handle.0 + meta_index_block_handle.1 + 40;
        let mut buf = vec![0u8; meta_index_block_handle.1 as usize];
        reader.seek(std::io::SeekFrom::Start(meta_index_block_handle.0))?;
        reader.read_exact(&mut buf[..])?;

        // println!("meta");
        let meta_index_block = Block::new(&buf[..], opt)?;
        for (k, v) in meta_index_block.clone().iter() {
            // println!("k {} v {}", String::from_utf8_lossy(&k), String::from_utf8_lossy(&v));
        }
        let index_block_handle = BlockHandle::decode(
            &meta_index_block
                .get(b"index")
                .ok_or(Errors::BlockNotFound("index".to_string()))?,
        );
        // println!("filter");
        let filter_block_handle = BlockHandle::decode(
            &meta_index_block
                .get(b"filter")
                .ok_or(Errors::BlockNotFound("filter".to_string()))?,
        );
        let mut buf = vec![0u8; index_block_handle.1 as usize];
        reader.seek(std::io::SeekFrom::Start(index_block_handle.0))?;
        reader.read_exact(&mut buf[..])?;
        // println!("index block block handle {:?}", index_block_handle);
        let index_block = Block::new(&buf[..], opt)?;
        let mut buf = vec![0u8; filter_block_handle.1 as usize];
        // println!("filter block block handle {:?}", filter_block_handle);
        reader.seek(std::io::SeekFrom::Start(filter_block_handle.0))?;
        reader.read_exact(&mut buf[..])?;
        let filter_block = BloomFilter::decode(&buf[..]);
        Ok(Self {
            reader: Rc::new(RefCell::new(reader)),
            total_size: total_size as usize,
            index_block,
            filter_block,
            block_cache: BlockCache::new(),
            opt,
        })
    }
}

pub struct TableCache {
    cache: LRUCache<u64, Table<File>>,
    opt: Options,
}

impl TableCache {
    pub(crate) fn new(opt: Options) -> Self {
        Self {
            cache: LRUCache::new(100),
            opt,
        }
    }
    pub(crate) fn get_table(&mut self, file_num: u64) -> &Table<File> {
        // TODO: join the db directory.
        self.cache.get_or_insert_with(&file_num, || {
            let file = std::fs::OpenOptions::new()
                .read(true)
                .open(format!("{}.sst", file_num))
                .unwrap();
            Table::from_reader(file, self.opt).unwrap()
        })
    }
}

pub struct ChainedIterator<T: Iterator<Item = (Vec<u8>, Vec<u8>)>> {
    current: Option<T>,
    tables: Vec<T>,
}

impl<T: Iterator<Item = (Vec<u8>, Vec<u8>)>> ChainedIterator<T> {
    pub fn new(tables: Vec<T>) -> Self {
        Self {
            current: None,
            tables,
        }
    }
    pub fn push(&mut self, table: T) {
        self.tables.push(table);
    }
}

impl<T: Iterator<Item = (Vec<u8>, Vec<u8>)>> Iterator for ChainedIterator<T> {
    type Item = (Vec<u8>, Vec<u8>);
    fn next(&mut self) -> Option<Self::Item> {
        if self.current.is_none() {
            self.current = self.tables.pop();
        }
        if let Some(ref mut current) = self.current {
            if let Some(kv) = current.next() {
                return Some(kv);
            }
        }
        self.current = self.tables.pop();
        if let Some(ref mut current) = self.current {
            current.next()
        } else {
            None
        }
    }
}

pub type PeekableIter = Peekable<Box<dyn Iterator<Item = (Vec<u8>, Vec<u8>)>>>;

pub struct MergingIterator {
    tables: Vec<PeekableIter>,
    opt: Options,
}

impl MergingIterator {
    pub fn new(tables: Vec<PeekableIter>, opt: Options) -> Self {
        Self { tables, opt }
    }
}

impl Iterator for MergingIterator {
    type Item = (Vec<u8>, Vec<u8>);
    fn next(&mut self) -> Option<Self::Item> {
        let mut smallest = None;
        let mut smallest_idx = 0;
        for (i, table) in self.tables.iter_mut().enumerate() {
            if let Some((k, _v)) = table.peek() {
                if smallest.is_none() || (self.opt.cmp.cmp)(k, smallest.unwrap()) == Ordering::Less
                {
                    smallest = Some(k);
                    smallest_idx = i;
                }
            }
        }
        self.tables[smallest_idx].next()
    }
}

#[cfg(test)]
mod tests {
    use super::super::options::Options;
    use super::{MergingIterator, Table, TableBuilder};
    use std::fs;
    use std::io::Cursor;
    fn build_data() -> Vec<(&'static str, &'static str)> {
        vec![
            // block 1
            ("abc", "def"),
            ("abd", "dee"),
            ("bcd", "asa"),
            // block 2
            ("bsr", "a00"),
            ("xyz", "xxx"),
            ("xzz", "yyy"),
            // block 3
            ("zzz", "111"),
        ]
    }
    fn build_table_file(data: Vec<(&str, &str)>, filename: &str) {
        fs::create_dir_all("test_dir/table_write").unwrap();
        let mut file = fs::OpenOptions::new()
            .write(true)
            .create(true)
            .open(format!("test_dir/table_write/{}", filename))
            .unwrap();

        let opt = Options {
            cmp: super::super::cmp::UserKeyComparator,
            block_restart_interval: 2,
            block_size: 32,
            ..Options::default()
        };
        {
            // Uses the standard comparator in opt.
            let mut b = TableBuilder::new(&mut file, opt);
            for &(k, v) in data.iter() {
                b.add(k.as_bytes(), v.as_bytes()).unwrap();
            }

            b.finish().unwrap();
        }
    }
    fn build_table(data: Vec<(&'static str, &'static str)>) -> (Vec<u8>, usize, super::Options) {
        let mut d = Vec::with_capacity(512);
        let opt = Options {
            cmp: super::super::cmp::UserKeyComparator,
            block_restart_interval: 2,
            block_size: 32,
            ..Options::default()
        };
        {
            // Uses the standard comparator in opt.
            let mut b = TableBuilder::new(&mut d, opt);
            for &(k, v) in data.iter() {
                b.add(k.as_bytes(), v.as_bytes()).unwrap();
            }

            b.finish().unwrap();
        }
        let size = d.len();
        (d, size, opt)
    }
    #[test]
    fn test_table_iterator_filter() {
        let (src, size, opt) = build_table(build_data());
        let table = Table::from_reader(Cursor::new(src), opt).unwrap();
        assert_eq!(size, table.size());
        for (k, _v) in table.iter() {
            // println!("k {} v {}", String::from_utf8_lossy(&k), String::from_utf8_lossy(&v));
            assert!(table.filter_block.may_contain(&k));
        }
    }
    #[test]
    fn test_table_iterator_fwd() {
        let (src, size, opt) = build_table(build_data());
        let data = build_data();
        let table = Table::from_reader(Cursor::new(src), opt).unwrap();
        assert_eq!(size, table.size());
        let iter = table.iter();
        let mut i = 0;
        // println!("iter");
        for (k, v) in iter {
            // println!("i = {}", i);
            assert_eq!(
                (data[i].0.as_bytes(), data[i].1.as_bytes()),
                (k.as_ref(), v.as_ref())
            );
            i += 1;
        }
        assert_eq!(i, data.len());
    }
    #[test]
    fn test_table_single_set_and_get() {
        let mut buf = vec![];
        let mut table = TableBuilder::new(&mut buf, Options::default());
        table.add(b"key", b"value").unwrap();
        table.finish().unwrap();
        let mut table = Table::from_reader(Cursor::new(buf), Options::default()).unwrap();
        let v = table.get(b"key");
        assert_eq!(v.unwrap().unwrap(), b"value".to_vec());
        let v = table.get(b"key");
        assert_eq!(v.unwrap().unwrap(), b"value".to_vec());
    }
    #[test]
    fn test_table_get() {
        let (src, size, opt) = build_table(build_data());

        let mut table = Table::from_reader(Cursor::new(src), opt).unwrap();
        assert_eq!(size, table.size());
        // Test that all of the table's entries are reachable via get()
        for (k, v) in table.iter() {
            let r = table.get(&k);
            // println!("get key {} and value {}", String::from_utf8_lossy(&k), String::from_utf8_lossy(&v));
            assert_eq!(v, r.unwrap().unwrap());
        }

        // assert_eq!(table.opt.block_cache.borrow().count(), 3);

        // test that filters work and don't return anything at all.
        assert!(table.get(b"aaa").unwrap().is_none());
        assert!(table.get(b"aaaa").unwrap().is_none());
        assert!(table.get(b"aa").unwrap().is_none());
        assert!(table.get(b"abcd").unwrap().is_none());
        assert!(table.get(b"abb").unwrap().is_none());
        assert!(table.get(b"zzy").unwrap().is_none());
        assert!(table.get(b"zz1").unwrap().is_none());
        assert!(table.get("zz{".as_bytes()).unwrap().is_none());
    }

    #[test]
    fn test_merging_iter() {
        let (src, _size, opt) = build_table(build_data());
        let table = Table::from_reader(Cursor::new(src), opt).unwrap();
        // let (src, size, opt) = build_table(build_data());
        build_table_file(build_data(), "table_2.sst");
        let table_2 = Table::from_reader(
            fs::OpenOptions::new()
                .read(true)
                .open("test_dir/table_write/table_2.sst")
                .unwrap(),
            opt,
        )
        .unwrap();

        let mut iter = MergingIterator::new(
            vec![table.iter().peekable(), table_2.iter().peekable()],
            Options {
                cmp: super::super::cmp::UserKeyComparator,
                ..Options::default()
            },
        );
        for (k, v) in table.iter() {
            assert_eq!((k.clone(), v.clone()), iter.next().unwrap());
            // println!("get key {} and value {}", String::from_utf8_lossy(&k), String::from_utf8_lossy(&v));
            assert_eq!((k, v), iter.next().unwrap());
        }
    }
}
