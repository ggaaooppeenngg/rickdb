#[allow(dead_code)]
pub(super) struct BloomFilter {
    bits: Vec<u8>,
    k: usize,
}

impl BloomFilter {
    // create encoded bloom filter.
    // the last bytes contains the number of hash functions.
    #[allow(dead_code)]
    pub(super) fn encode(&self) -> Vec<u8> {
        let mut r = self.bits.clone();
        r.push(self.k as u8);
        r
    }
    #[allow(dead_code)]
    pub(super) fn decode(filter: &[u8]) -> Self {
        let bits = filter[0..filter.len() - 1].to_vec();
        let k = filter[filter.len() - 1] as usize;
        Self { bits, k }
    }
    #[allow(dead_code)]
    pub(super) fn add(&mut self, key: &[u8]) {
        let mut hash = murmur1(key, 0xbc9f1d34);
        let delta = (hash >> 17) | (hash << 15);
        for _ in 0..self.k {
            // 设置对应hash的bit位
            let bit_pos = hash as usize % (self.bits.len() * 8);
            self.bits[bit_pos / 8] |= 1 << (bit_pos % 8);
            hash = hash.wrapping_add(delta);
        }
    }
    #[allow(dead_code)]
    pub(super) fn new(bits_per_key: usize, keys_count: usize) -> Self {
        let mut bits = keys_count * bits_per_key;
        if bits < 64 {
            bits = 64;
        }
        let bytes = (bits + 7) / 8;
        let mut k = (bits_per_key as f64 * 0.69).round() as usize;
        if k < 1 {
            k = 1;
        } else if k > 30 {
            k = 30;
        }
        let bits = vec![0u8; bytes];
        Self { bits, k }
    }
    #[allow(dead_code)]
    pub(super) fn may_contain(&self, key: &[u8]) -> bool {
        let mut hash = murmur1(key, 0xbc9f1d34);
        let delta = (hash >> 17) | (hash << 15);
        for _ in 0..self.k {
            // 设置对应hash的bit位
            let bit_pos = hash as usize % (self.bits.len() * 8);
            if self.bits[bit_pos / 8] & (1 << (bit_pos % 8)) == 0 {
                return false;
            }
            hash = hash.wrapping_add(delta);
        }
        true
    }
}
#[allow(dead_code)]
struct Murmur1Hasher(u32);

impl Murmur1Hasher {
    #[allow(dead_code)]
    fn new(seed: u32) -> Murmur1Hasher {
        if seed == 0 {
            Murmur1Hasher(0xbc9f1d34)
        } else {
            Murmur1Hasher(seed)
        }
    }
}

fn murmur1(bytes: &[u8], seed: u32) -> u32 {
    let m: u32 = 0xc6a4a793;
    let r = 24;
    // seed ^ ( n * m )
    // may overflow
    let mut h: u32 = seed ^ (bytes.len() as u32).wrapping_mul(m);
    for chunk in bytes.chunks(4) {
        if chunk.len() == 4 {
            let w = u32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]);
            // may overflow
            h = h.wrapping_add(w);
            h = h.wrapping_mul(m);
            h ^= h >> 16;
        } else {
            for (i, &b) in chunk.iter().enumerate() {
                h = h.wrapping_add((b as u32) << (i * 8));
            }
            // may overflow
            h = h.wrapping_mul(m);
            h ^= h >> r;
        }
    }
    h
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_murmur1() {
        let data1: [u8; 1] = [0x62];
        let data2: [u8; 2] = [0xc3, 0x97];
        let data3: [u8; 3] = [0xe2, 0x99, 0xa5];
        let data4: [u8; 4] = [0xe1, 0x80, 0xb9, 0x32];
        let data5: [u8; 48] = [
            0x01, 0xc0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x14, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x00, 0x14,
            0x00, 0x00, 0x00, 0x18, 0x28, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        ];
        //assert_eq!(hash(0, 0, 0xbc9f1d34), 0xbc9f1d34);
        assert_eq!(murmur1(&data1, 0xbc9f1d34), 0xef1345c4);
        assert_eq!(murmur1(&data2, 0xbc9f1d34), 0x5b663814);
        assert_eq!(murmur1(&data3, 0xbc9f1d34), 0x323c078f);
        assert_eq!(murmur1(&data4, 0xbc9f1d34), 0xed21633a);
        assert_eq!(murmur1(&data5, 0x12345678), 0xf333dabb);
    }
    #[test]
    fn test_bloom_filter() {
        let mut bloom_filter = BloomFilter::new(10, 3);
        bloom_filter.add(&[1]);
        bloom_filter.add(&[2]);
        bloom_filter.add(&[3]);
        assert!(bloom_filter.may_contain(&[1]));
        assert!(bloom_filter.may_contain(&[2]));
        assert!(bloom_filter.may_contain(&[3]));
    }

    #[test]
    fn test_empty_filter() {
        let test = BloomFilter::new(10, 10);
        assert!(!test.may_contain(b"hello"));
        assert!(!test.may_contain(b"world"));
    }
    #[test]
    fn test_large() {
        let mut test = BloomFilter::new(10, 256 * 10);
        for i in 0..256 * 10 {
            // 保证k的字节序是有序的
            let k = format!("{}", i);
            test.add(k.as_bytes());
        }
        for i in 0..256 * 10 {
            // 保证k的字节序是有序的
            let k = format!("{}", i);
            //println!("{} ", k);
            assert!(test.may_contain(k.as_bytes()));
        }
    }
}
