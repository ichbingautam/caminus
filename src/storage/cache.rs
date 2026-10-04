use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct BlockId {
    pub sstable_id: String,
    pub block_offset: u64,
}

impl BlockId {
    pub fn new(sstable_id: impl Into<String>, block_offset: u64) -> Self {
        Self {
            sstable_id: sstable_id.into(),
            block_offset,
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct CacheMetrics {
    pub hits: u64,
    pub misses: u64,
    pub cached_blocks: usize,
    pub cached_bytes: usize,
    pub hit_ratio: f64,
}

struct CacheEntry {
    data: Vec<u8>,
}

struct InnerCache {
    capacity_bytes: usize,
    current_bytes: usize,
    map: HashMap<BlockId, CacheEntry>,
    order: VecDeque<BlockId>,
}

pub struct LruBlockCache {
    inner: Mutex<InnerCache>,
    hits: AtomicU64,
    misses: AtomicU64,
}

impl LruBlockCache {
    pub fn new(capacity_bytes: usize) -> Self {
        Self {
            inner: Mutex::new(InnerCache {
                capacity_bytes,
                current_bytes: 0,
                map: HashMap::new(),
                order: VecDeque::new(),
            }),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
        }
    }

    pub fn get(&self, id: &BlockId) -> Option<Vec<u8>> {
        let mut guard = self.inner.lock().unwrap();
        if let Some(entry) = guard.map.get(id) {
            let data = entry.data.clone();
            // Move to back (most recently used)
            if let Some(pos) = guard.order.iter().position(|x| x == id) {
                guard.order.remove(pos);
            }
            guard.order.push_back(id.clone());
            self.hits.fetch_add(1, Ordering::Relaxed);
            Some(data)
        } else {
            self.misses.fetch_add(1, Ordering::Relaxed);
            None
        }
    }

    pub fn insert(&self, id: BlockId, data: Vec<u8>) {
        let entry_size = data.len();
        let mut guard = self.inner.lock().unwrap();

        // If block already exists, remove it first
        if let Some(old_entry) = guard.map.remove(&id) {
            guard.current_bytes = guard.current_bytes.saturating_sub(old_entry.data.len());
            if let Some(pos) = guard.order.iter().position(|x| x == &id) {
                guard.order.remove(pos);
            }
        }

        // Evict LRU entries while current_bytes + entry_size > capacity_bytes
        while !guard.order.is_empty() && guard.current_bytes + entry_size > guard.capacity_bytes {
            if let Some(lru_id) = guard.order.pop_front() {
                if let Some(evicted) = guard.map.remove(&lru_id) {
                    guard.current_bytes = guard.current_bytes.saturating_sub(evicted.data.len());
                }
            }
        }

        // Insert new entry if it fits in total capacity
        if entry_size <= guard.capacity_bytes {
            guard.current_bytes += entry_size;
            guard.map.insert(id.clone(), CacheEntry { data });
            guard.order.push_back(id);
        }
    }

    pub fn remove(&self, id: &BlockId) -> bool {
        let mut guard = self.inner.lock().unwrap();
        if let Some(entry) = guard.map.remove(id) {
            guard.current_bytes = guard.current_bytes.saturating_sub(entry.data.len());
            if let Some(pos) = guard.order.iter().position(|x| x == id) {
                guard.order.remove(pos);
            }
            true
        } else {
            false
        }
    }

    pub fn clear(&self) {
        let mut guard = self.inner.lock().unwrap();
        guard.map.clear();
        guard.order.clear();
        guard.current_bytes = 0;
    }

    pub fn metrics(&self) -> CacheMetrics {
        let guard = self.inner.lock().unwrap();
        let hits = self.hits.load(Ordering::Relaxed);
        let misses = self.misses.load(Ordering::Relaxed);
        let total = hits + misses;
        let hit_ratio = if total > 0 {
            hits as f64 / total as f64
        } else {
            0.0
        };

        CacheMetrics {
            hits,
            misses,
            cached_blocks: guard.map.len(),
            cached_bytes: guard.current_bytes,
            hit_ratio,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn test_lru_cache_basic_get_insert() {
        let cache = LruBlockCache::new(100);
        let id1 = BlockId::new("sst_1", 0);
        let data1 = vec![1, 2, 3, 4, 5];

        cache.insert(id1.clone(), data1.clone());
        assert_eq!(cache.get(&id1), Some(data1));

        let id_missing = BlockId::new("sst_1", 100);
        assert_eq!(cache.get(&id_missing), None);

        let m = cache.metrics();
        assert_eq!(m.hits, 1);
        assert_eq!(m.misses, 1);
        assert_eq!(m.hit_ratio, 0.5);
    }

    #[test]
    fn test_lru_eviction() {
        // Capacity: 30 bytes
        let cache = LruBlockCache::new(30);

        let id1 = BlockId::new("sst_1", 0);
        let id2 = BlockId::new("sst_1", 10);
        let id3 = BlockId::new("sst_1", 20);

        cache.insert(id1.clone(), vec![0; 10]); // 10 bytes
        cache.insert(id2.clone(), vec![0; 10]); // 20 bytes
        cache.insert(id3.clone(), vec![0; 10]); // 30 bytes (full)

        assert_eq!(cache.metrics().cached_blocks, 3);
        assert_eq!(cache.metrics().cached_bytes, 30);

        // Access id1 to make it MRU (order becomes: id2, id3, id1)
        assert_eq!(cache.get(&id1).unwrap().len(), 10);

        // Insert id4 (10 bytes), should evict LRU (id2)
        let id4 = BlockId::new("sst_1", 30);
        cache.insert(id4.clone(), vec![0; 10]);

        assert_eq!(cache.get(&id2), None); // Evicted!
        assert!(cache.get(&id1).is_some());
        assert!(cache.get(&id3).is_some());
        assert!(cache.get(&id4).is_some());
    }

    #[test]
    fn test_lru_cache_concurrent_access() {
        let cache = Arc::new(LruBlockCache::new(1000));
        let mut handles = vec![];

        for _ in 0..10 {
            let cache_clone = Arc::clone(&cache);
            handles.push(thread::spawn(move || {
                for j in 0..100 {
                    let id = BlockId::new("sst_shared", j);
                    cache_clone.insert(id.clone(), vec![j as u8; 5]);
                    let _ = cache_clone.get(&id);
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        let m = cache.metrics();
        assert!(m.hits > 0);
        assert!(m.cached_blocks <= 200);
    }
}
