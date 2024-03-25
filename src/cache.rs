use std::collections::{HashMap, VecDeque};
use std::hash::Hash;
pub struct LRUCache<K, V> {
    map: HashMap<K, V>,
    order: VecDeque<K>,
    capacity: usize,
}

impl<K: Clone + Eq + Hash, V> LRUCache<K, V> {
    pub fn new(capacity: usize) -> Self {
        LRUCache {
            map: HashMap::with_capacity(capacity),
            order: VecDeque::with_capacity(capacity),
            capacity,
        }
    }

    pub fn get_or_insert_with<F: FnOnce() -> V>(&mut self, key: &K, f: F) -> &V {
        if self.map.contains_key(key) {
            self.refresh(key);
            self.map.get(key).unwrap()
        } else {
            self.put(key.clone(), f());
            self.map.get(key).unwrap()
        }
    }

    pub fn put(&mut self, key: K, value: V) {
        if self.map.contains_key(&key) {
            self.refresh(&key);
        } else {
            if self.map.len() == self.capacity {
                if let Some(oldest) = self.order.pop_back() {
                    self.map.remove(&oldest);
                }
            }
            self.order.push_front(key.clone());
        }
        self.map.insert(key, value);
    }

    fn refresh(&mut self, key: &K) {
        if let Some(position) = self.order.iter().position(|k| k == key) {
            let key = self.order.remove(position).unwrap();
            self.order.push_front(key);
        } 
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_lru_cache() {
        let mut cache = LRUCache::new(2);
        cache.put(1, 1);
        cache.put(2, 2);
        assert_eq!(cache.get_or_insert_with(&1, || 0), &1);
        cache.put(3, 3);
        assert_eq!(cache.get_or_insert_with(&2, || 0), &0);
        cache.put(4, 4);
        assert_eq!(cache.get_or_insert_with(&1, || 0), &0);
        assert_eq!(cache.get_or_insert_with(&4, || 0), &4);
    }
}
