use std::{
    cmp::Ordering,
    fmt::{Display, Formatter},
    ops::Deref,
};

#[derive(Clone, Debug, Hash, Eq, PartialEq, Default)]
pub struct InternalKey(Vec<u8>);
impl InternalKey{
    pub fn size(&self) -> usize{
        self.0.len()
    }
}
#[allow(unused)]
const INTERNAL_KEY_SEQ_NUM_MAX: u64 = (1 << 56) - 1;

pub enum InternalKeyKind {
    Deletion = 0,
    Value = 1,
    Max = 23,
}

pub fn user_key(encoded_key: &[u8]) -> &[u8] {
    assert!(encoded_key.len() >= 8);
    &encoded_key[..encoded_key.len() - 8]
}

#[allow(unused)]
pub fn trailer(encoded_key: &[u8]) -> u64 {
    u64::from_le_bytes(encoded_key[encoded_key.len() - 8..].try_into().unwrap())
}

// Implementing Deref for InternalKey
impl Deref for InternalKey {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl Display for InternalKey {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if self.0.len() >= 8 {
            write!(
                f,
                "{:?},seq:[{}],type:[{}]",
                &self.0[..self.0.len() - 8],
                u64::from_le_bytes(self.0[self.0.len() - 8..].try_into().unwrap()) >> 8,
                u64::from_le_bytes(self.0[self.0.len() - 8..].try_into().unwrap()) & 0xff
            )
        } else {
            write!(f, "{:?}", self.0)
        }
    }
}

impl InternalKey {
    pub fn new(user_key: &[u8], seq: u64, kind: InternalKeyKind) -> Self {
        let mut content = user_key.to_vec();
        content.extend_from_slice(&((seq << 8) | (kind as u64)).to_le_bytes());
        InternalKey(content)
    }
    pub fn user_key(&self) -> &[u8] {
        &self.0[..self.0.len() - 8]
    }
    pub fn trailer(&self) -> u64 {
        u64::from_le_bytes(self.0[self.0.len() - 8..].try_into().unwrap())
    }
    pub fn decode(encoded_key: &[u8]) -> Option<InternalKey> {
        if encoded_key.len() >= 8 {
            return Some(InternalKey(encoded_key.to_vec()));
        }
        None
    }
    pub fn encode(self) -> Vec<u8> {
        self.0
    }
}

impl PartialOrd for InternalKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        assert!(self.len() >= 8);
        assert!(other.len() >= 8);
        match &self[0..self.len() - 8].cmp(&other[0..other.len() - 8]) {
            Ordering::Less => Some(Ordering::Less),
            Ordering::Greater => Some(Ordering::Greater),
            Ordering::Equal => {
                let a_trailer = u64::from_le_bytes(self[self.len() - 8..].try_into().unwrap());
                let b_trailer = u64::from_le_bytes(other[other.len() - 8..].try_into().unwrap());
                // revsersed order
                Some(b_trailer.cmp(&a_trailer))
            }
        }
    }
}

impl Ord for InternalKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.partial_cmp(other).unwrap()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_internal_key() {
        let key = InternalKey::new(b"123", 100, InternalKeyKind::Value);
        let encoded_key = key.clone().encode();
        let decoded_key = InternalKey::decode(&encoded_key).unwrap();
        assert_eq!(key, decoded_key);
        assert_eq!(key.user_key(), decoded_key.user_key());
    }
}
