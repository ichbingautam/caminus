use caminus::storage::cache::{BlockId, LruBlockCache};
use std::sync::Arc;
use std::thread;

#[test]
fn test_lru_block_cache_integration_and_metrics() {
    let capacity_bytes = 200;
    let cache = Arc::new(LruBlockCache::new(capacity_bytes));

    // Fill cache up to capacity with 4 blocks of 50 bytes each
    for i in 0..4 {
        let block_id = BlockId::new("sst_integration_1", i);
        cache.insert(block_id, vec![i as u8; 50]);
    }

    let m1 = cache.metrics();
    assert_eq!(m1.cached_blocks, 4);
    assert_eq!(m1.cached_bytes, 200);

    // Access block 0 and block 1 to make them MRU (LRU is now block 2)
    assert!(cache.get(&BlockId::new("sst_integration_1", 0)).is_some());
    assert!(cache.get(&BlockId::new("sst_integration_1", 1)).is_some());

    // Insert block 4 (50 bytes), causing eviction of block 2
    cache.insert(BlockId::new("sst_integration_1", 4), vec![4; 50]);

    assert!(cache.get(&BlockId::new("sst_integration_1", 2)).is_none());
    assert!(cache.get(&BlockId::new("sst_integration_1", 0)).is_some());
    assert!(cache.get(&BlockId::new("sst_integration_1", 1)).is_some());

    // Concurrent read/write stress test
    let mut handles = vec![];
    for t in 0..4 {
        let cache_clone = Arc::clone(&cache);
        handles.push(thread::spawn(move || {
            for i in 0..50 {
                let id = BlockId::new(format!("sst_concurrent_{}", t), i);
                cache_clone.insert(id.clone(), vec![i as u8; 20]);
                let _ = cache_clone.get(&id);
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    let final_metrics = cache.metrics();
    assert!(final_metrics.hits > 0);
    assert!(final_metrics.cached_bytes <= capacity_bytes);
}
