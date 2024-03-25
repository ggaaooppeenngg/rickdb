use integer_encoding::{VarInt,VarIntWriter};
use super::options::Options;
use std::{
    cmp::Ordering,
    io::{Read, Result, Write},
};

use super::cache::LRUCache;

// clone is for index block
#[allow(unused)]
#[derive(Clone)]
pub(super) struct Block {
    content: Vec<u8>,
    restarts: Vec<u32>,
    opt: Options,
}

impl Block {
    #![allow(unused)]
    pub(super) fn new(mut b: impl Read, opt: Options) -> Result<Self> {
        let mut content = vec![];
        b.read_to_end(&mut content)?;
        // n restarts
        // println!("content len {}", content.len());
        let mut read_pos = content.len() - 4;
        let restarts_length = u32::from_le_bytes(content[read_pos..].try_into().unwrap()) as usize;
        // restarts
        read_pos -= restarts_length * 4;
        let mut restarts = vec![0; restarts_length];
        for i in 0..restarts_length {
            restarts[i] = u32::from_le_bytes(
                content[read_pos + i * 4..read_pos + i * 4 + 4]
                    .try_into()
                    .unwrap(),
            );
        }
        content.truncate(read_pos);
        Ok(Self {
            content,
            restarts,
            opt,
        })
    }
    pub(super) fn restart_iter(&self, i: usize) -> RestartIterator {
        let off = self.restarts[i] as usize;
        let end = if i == self.restarts.len() - 1 {
            self.content.len()
        } else {
            self.restarts[i + 1] as usize
        };
        RestartIterator {
            data: &self.content[off..end],
            last_key: Vec::new(),
            pos: 0,
        }
    }
    pub(super) fn iter(self) -> BlockIterator {
        BlockIterator {
            block_content: self.content,
            restarts: self.restarts,
            pos: 0,
            restart_index: 0,
            last_key: Vec::new(),
        }
    }
    fn smallest_key_in_restart(&self, i: usize) -> &[u8] {
        let off = self.restarts[i] as usize;
        let (h, s) = Header::decode(&self.content[off..]);
        &self.content[off + s..off + s + h.klen as usize]
    }
    // 给 index block 用的，只找边界不找准确值
    pub(super) fn get_ge(&self, key: &[u8]) -> Option<Vec<u8>> {
        let mut left = 0usize;
        let mut right = if self.restarts.is_empty() {
            0
        } else {
            self.restarts.len() - 1
        };
        // println!("left {} and right {}", left, right);
        while left < right {
            let mid = (left + right + 1) / 2;
            if (self.opt.cmp.cmp)(key, self.smallest_key_in_restart(mid)) == Ordering::Less {
                right = mid - 1;
            } else {
                left = mid;
            }
        }
        let iter = self.restart_iter(left);
        // println!("left is {}", left);
        for (k, v) in iter {
            // println!(
            //     "key and k is {:?} {:?}",
            //     String::from_utf8_lossy(key),
            //     String::from_utf8_lossy(&k[..])
            // );
            match (self.opt.cmp.cmp)(key, &k[..]) {
                Ordering::Equal | Ordering::Less => {
                    return Some(v.to_vec());
                }
                Ordering::Greater => {}
            }
        }
        // 当前block可能都小于，需要向后看
        if left < self.restarts.len() - 1 {
            let iter = self.restart_iter(left + 1);
            for (k, v) in iter {
                // println!(
                //     "key and k is {:?} {:?}",
                //     String::from_utf8_lossy(key),
                //     String::from_utf8_lossy(&k[..])
                // );
                match (self.opt.cmp.cmp)(key, &k[..]) {
                    Ordering::Equal | Ordering::Less => {
                        return Some(v.to_vec());
                    }
                    Ordering::Greater => {}
                }
            }
        }
        None
    }
    pub(super) fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        let mut left = 0usize;
        let mut right = if self.restarts.is_empty() {
            0
        } else {
            self.restarts.len() - 1
        };
        while left < right {
            let mid = (left + right + 1) / 2;
            // println!("get mid is {}", mid);
            // println!(
            //     "key is {}, begin key {}",
            //     String::from_utf8_lossy(key),
            //     String::from_utf8_lossy(self.begin_key_in_restart(mid))
            // );
            if (self.opt.cmp.cmp)(key, self.smallest_key_in_restart(mid)) == Ordering::Less {
                right = mid - 1;
            } else {
                left = mid;
            }
        }
        // println!("get left and right {} {}", left, right);
        let iter = self.restart_iter(left);
        for (k, v) in iter {
            // println!("loop key is {:?}", String::from_utf8_lossy(&k[..]));
            // println!("key is {:?}", String::from_utf8_lossy(key));
            if &k[..] == key {
                // println!("some v is {:?}", v.clone());
                return Some(v.to_vec());
            }
        }
        None
    }
}

pub struct BlockCache {
    cache: LRUCache<u64, Block>,
}

impl Default for BlockCache {
    fn default() -> Self {
        Self {
            cache: LRUCache::new(100),
        }
    }
}

impl BlockCache {
    pub fn new() -> Self {
        Self::default()
    }

    pub(super) fn get_block_or<F: FnOnce() -> Block>(&mut self, key: &u64, f: F) -> &Block {
        self.cache.get_or_insert_with(key, f)
    }
}

pub(super) struct RestartIterator<'a> {
    data: &'a [u8],
    last_key: Vec<u8>,
    pos: usize,
}

impl<'a> Iterator for RestartIterator<'a> {
    // we will be counting with usize
    type Item = (Vec<u8>, &'a [u8]);

    // next() is the only required method
    fn next(&mut self) -> Option<Self::Item> {
        if self.pos >= self.data.len() {
            return None;
        }
        let (h, s) = Header::decode(&self.data[self.pos..]);
        self.pos += s;
        if self.last_key.is_empty() {
            self.last_key = self.data[self.pos..self.pos + h.klen as usize].to_vec();
        };
        // println!("h klen {0} vlen {1} plen {2}", h.klen, h.vlen, h.plen);
        let mut key = self.last_key.clone()[0..h.plen as usize].to_vec();
        key.extend_from_slice(&self.data[self.pos..self.pos + h.klen as usize]);
        //println!("key {:?}", self.key);
        self.pos += h.klen as usize;
        //println!("k pos {0}", self.pos);
        let value = &self.data[self.pos..self.pos + h.vlen as usize];
        self.pos += h.vlen as usize;
        // println!("value {:?}", self.value);
        // println!("v pos {0}", self.pos);
        self.last_key = key.clone();

        Some((key, value))
    }
}
pub struct BlockIterator {
    block_content: Vec<u8>,
    restarts: Vec<u32>,
    pos: usize,
    restart_index: usize,
    last_key: Vec<u8>,
}

impl Iterator for BlockIterator {
    // we will be counting with usize
    type Item = (Vec<u8>, Vec<u8>);

    // next() is the only required method
    fn next(&mut self) -> Option<Self::Item> {
        // Increment our count. This is why we started at zero.
        if self.pos >= self.block_content.len() {
            assert!(self.pos == self.block_content.len());
            return None;
        }
        if self.restart_index < self.restarts.len() - 1
            && self.pos == self.restarts[self.restart_index + 1] as usize
        {
            self.last_key.clear();
            self.restart_index += 1;
        }
        let (h, s) = Header::decode(&self.block_content[self.pos..]);
        self.pos += s;
        if self.last_key.is_empty() {
            self.last_key = self.block_content[self.pos..self.pos + h.klen as usize].to_vec();
        };
        // println!("h klen {0} vlen {1} plen {2}", h.klen, h.vlen, h.plen);
        let mut key = self.last_key.clone()[0..h.plen as usize].to_vec();
        key.extend_from_slice(&self.block_content[self.pos..self.pos + h.klen as usize]);
        //println!("key {:?}", self.key);
        self.pos += h.klen as usize;
        //println!("k pos {0}", self.pos);
        let value = &self.block_content[self.pos..self.pos + h.vlen as usize];
        self.pos += h.vlen as usize;
        // println!("value {:?}", self.value);
        // println!("v pos {0}", self.pos);
        self.last_key = key.clone();

        Some((key, value.to_vec()))
    }
}
// BlockBuilder 生成的块，其中键是前缀压缩的：

// 当存储键时，我们舍弃与前一个字符串共享的前缀。这有助于显著减少空间需求。
// 此外，每隔 K 个键，我们不应用前缀压缩，并存储整个键。我们将其称为 "重启点"。
// 块的尾部存储所有重启点的偏移量，并可在查找特定键时进行二进制搜索。
// 值按原样存储（无压缩），紧随相应的键之后。

// 特定键值对的条目具有以下形式：
//     shared_bytes: uvarint32
//     unshared_bytes: uvarint32
//     value_length: uvarint32
//     key_delta: char[unshared_bytes]
//     value: char[value_length]
// 对于重启点，shared_bytes == 0。

// 块的尾部具有以下形式：
//     restarts: uint32[num_restarts]
//     num_restarts: uint32
// restarts[i] 包含块中第 i 个重启点的偏移量。
#[allow(unused)]
pub(super) struct BlockBuilder {
    // 一个restart的entry间隔数量，
    // 超过这个值以后要重新开始restart。
    restart_interval: usize,
    // 当前restart中的entry数量
    restart_counter: usize,
    // 写缓存，block在finish之后拷贝给调用者
    // 这个 buffer 的容量不需要大于 BLOCK_SIZE
    // 默认设置成1024
    buffer: Vec<u8>, // 这个buffer不需要 > BLOCK_SIZE
    // restart 的 offset
    restarts: Vec<u32>,
    // 最后一个key，用于计算shared bytes
    last_key: Vec<u8>,

    opt: Options,
}

// 头部但varint
struct Header {
    plen: u32,
    klen: u32,
    vlen: u32,
}

impl Header {
    #[allow(unused)]
    fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.write_varint(self.plen);
        buf.write_varint(self.klen);
        buf.write_varint(self.vlen);
        buf
    }
    fn decode(data: &[u8]) -> (Self, usize) {
        let mut off = 0;
        let (plen, s) = u32::decode_var(data).unwrap();
        off += s;
        let (klen, s) = u32::decode_var(&data[off..]).unwrap();
        off += s;
        let (vlen, s) = u32::decode_var(&data[off..]).unwrap();
        let size = off + s;
        (Self { plen, klen, vlen }, size)
    }
}

impl BlockBuilder {
    #[allow(unused)]
    pub fn new(restart_interval: usize, opt: Options) -> Self {
        Self {
            restart_interval,
            restart_counter: 0,
            buffer: Vec::with_capacity(1024), // < BLOCK_SIZE
            restarts: vec![0],                // 默认添加一个0 offset
            last_key: vec![],
            //keys: Vec::new(),
            opt,
        }
    }
    #[allow(unused)]
    pub(super) fn last_key(&self) -> &[u8] {
        &self.last_key
    }
    #[allow(unused)]
    pub(super) fn reset(&mut self) {
        self.restart_counter = 0;
        self.buffer.clear();
        self.restarts = vec![0];
        self.last_key = vec![];
    }
    #[allow(unused)]
    pub(super) fn finish(&mut self) -> Result<Vec<u8>> {
        for restart in &self.restarts {
            self.buffer.write_all(&restart.to_le_bytes())?;
        }
        self.buffer
            .write_all(&(self.restarts.len() as u32).to_le_bytes())?;
        Ok(self.buffer.clone())
    }
    #[allow(unused)]
    pub(super) fn size_estimate(&mut self) -> usize {
        self.buffer.len() + self.restarts.len() * 4 + 4
    }
    #[allow(unused)]
    pub(super) fn add(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        assert!(
            self.last_key.is_empty()
                || (self.opt.cmp.cmp)(self.last_key.as_slice(), key) == Ordering::Less
        );
        let mut shared: usize = 0;
        // 没有 restarts 过就都是0，最后四个字节是空的。
        if self.restart_counter < self.restart_interval {
            while shared < key.len() && shared < self.last_key.len() {
                if key[shared] != self.last_key[shared] {
                    break;
                }
                shared += 1;
            }
        } else {
            self.restarts.push(self.buffer.len() as u32);
            self.last_key.clear();
            self.restart_counter = 0;
        }
        let header = Header {
            plen: shared as u32,
            klen: (key.len() - shared) as u32,
            vlen: value.len() as u32,
        }
        .encode();
        // self.size += header.len();
        self.buffer.write_all(&header)?;
        // self.size += key.len() - shared + value.len();
        self.buffer.write_all(&key[shared..])?;
        self.buffer.write_all(value)?;
        self.last_key = key.to_vec();
        self.restart_counter += 1;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn read_iter_works() -> Result<()> {
        let opt = Options::default();
        let mut builder = BlockBuilder::new(10, Options::default());
        for i in 0..10 * 10 {
            // 保证k的字节序是有序的
            let k = format!("{:010}", i);
            let v = format!("{:010}", 2 * i);
            builder.add(k.as_bytes(), v.as_bytes())?;
        }
        let result = builder.finish()?;
        let reader = Block::new(Cursor::new(&result[..]), opt)?;
        for (i, kv) in reader.iter().enumerate() {
            let k = format!("{:010}", i);
            //.to_le_bytes();
            let v = format!("{:010}", 2 * i);
            //println!("test k: {}, v: {}", k, v);
            assert_eq!(k.as_bytes(), kv.0.as_slice());
            assert_eq!(v.as_bytes(), kv.1);
        }
        let reader = Block::new(Cursor::new(&result[..]), opt)?;
        for i in 0..10 * 10 {
            // 保证k的字节序是有序的
            let k = format!("{:010}", i);
            assert!(reader.get(k.as_bytes()).is_some())
        }
        Ok(())
    }
}
