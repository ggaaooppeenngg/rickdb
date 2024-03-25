use std::{
    cmp::{self, min, Ordering},
    mem,
};

#[allow(unused)]
enum ValueType {
    TypeDeletion = 0,
    TypeValue = 1,
}

impl ValueType {
    #[allow(unused, non_upper_case_globals)]
    pub const TypeForSeek: ValueType = Self::TypeValue;
}
#[allow(unused)]
pub const MAX_SEQUENCE_NUMBER: u64 = (1 << 56) - 1;

#[derive(Clone, Copy)]
pub struct Comparator {
    pub cmp: fn(&[u8], &[u8]) -> Ordering,
    pub separator: fn(&mut Vec<u8>, &[u8]),
    pub successor: fn(&mut Vec<u8>),
    pub name: &'static str,
}
#[allow(unused, non_upper_case_globals)]
pub const InternalComparator: Comparator = Comparator {
    cmp: internal_cmp,
    separator: internal_shortest_separator,
    successor: internal_shortest_successor,
    name: "InternalKeyComparator",
};

pub(super) fn internal_shortest_separator(start: &mut Vec<u8>, limit: &[u8]) {
    assert!(start.len() >= 8);
    assert!(limit.len() >= 8);
    let l = &start[0..start.len() - 8];
    let j = &limit[0..limit.len() - 8];
    let mut tmp = l.to_vec();
    (UserKeyComparator.separator)(&mut tmp, j);
    if tmp.len() < l.len() {
        let pack = MAX_SEQUENCE_NUMBER << 8 | ValueType::TypeForSeek as u64;
        tmp.extend_from_slice(pack.to_le_bytes().as_ref());
        assert!(internal_cmp(start, &tmp) == Ordering::Less);
        assert!(internal_cmp(&tmp, limit) == Ordering::Less);
        mem::swap(start, &mut tmp);
    }
}

pub(super) fn internal_shortest_successor(key: &mut Vec<u8>) {
    assert!(key.len() >= 8);
    let l = &key[0..key.len() - 8];
    let mut tmp = l.to_vec();
    (UserKeyComparator.successor)(&mut tmp);
    if tmp.len() < key.len() {
        let pack = MAX_SEQUENCE_NUMBER << 8 | ValueType::TypeForSeek as u64;
        tmp.extend_from_slice(pack.to_le_bytes().as_ref());
        assert!(internal_cmp(key, &tmp) == Ordering::Less);
        mem::swap(key, &mut tmp);
    }
}

// user key + seno + meta
pub(super) fn internal_cmp(a: &[u8], b: &[u8]) -> Ordering {
    assert!(a.len() >= 8);
    assert!(b.len() >= 8);
    match &a[0..a.len() - 8].cmp(&b[0..b.len() - 8]) {
        Ordering::Less => Ordering::Less,
        Ordering::Greater => Ordering::Greater,
        Ordering::Equal => {
            let a_trailer = u64::from_le_bytes(a[a.len() - 8..].try_into().unwrap());
            let b_trailer = u64::from_le_bytes(b[b.len() - 8..].try_into().unwrap());
            b_trailer.cmp(&a_trailer)
        }
    }
}
#[allow(non_upper_case_globals)]
pub const UserKeyComparator: Comparator = Comparator {
    cmp: cmp::Ord::cmp,
    separator: shortest_separator,
    successor: shortest_successor,
    name: "BytewiseComparator",
};

#[allow(unused)]
pub(super) fn shortest_successor(last_key: &mut Vec<u8>) {
    for i in 0..last_key.len() {
        if last_key[i] != 0xff {
            last_key[i] += 1;
            last_key.resize(i + 1, 0);
            return;
        }
    }
}

#[allow(unused)]
pub(super) fn shortest_separator(start: &mut Vec<u8>, limit: &[u8]) {
    // Iterate over common prefix of start and limit
    let min_length = min(start.len(), limit.len());
    let mut diff_index = 0;

    while diff_index < min_length && start[diff_index] == limit[diff_index] {
        diff_index += 1;
    }

    // Find the first differing byte
    if diff_index < min_length {
        let diff_byte = start[diff_index];
        if diff_byte < 255 && diff_byte + 1 < limit[diff_index] {
            // Increment the differing byte
            start[diff_index] += 1;
            // Remove the rest of the vector to make it shorter
            start.resize(diff_index + 1, 0);
        }
    } // Do not shorten if one string is a prefix of the other
}
