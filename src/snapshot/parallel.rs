use std::sync::Arc;
use tokio::sync::Mutex;
use crate::storage::StateStore;
use crate::source::ChangeEvent;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ChunkStatus {
    Pending,
    InProgress,
    Completed,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChunkRange {
    pub chunk_id: u32,
    pub min_key: u64,
    pub max_key: u64,
    pub status: ChunkStatus,
}

pub struct ParallelSnapshotter {
    pub table_name: String,
    pub chunks: Arc<Mutex<Vec<ChunkRange>>>,
}

impl ParallelSnapshotter {
    pub fn new(table_name: impl Into<String>, total_rows: u64, chunk_size: u64) -> Self {
        let mut chunks = Vec::new();
        let mut min_key = 1;
        let mut chunk_id = 0;

        while min_key <= total_rows {
            let max_key = (min_key + chunk_size - 1).min(total_rows);
            chunks.push(ChunkRange {
                chunk_id,
                min_key,
                max_key,
                status: ChunkStatus::Pending,
            });
            chunk_id += 1;
            min_key = max_key + 1;
        }

        Self {
            table_name: table_name.into(),
            chunks: Arc::new(Mutex::new(chunks)),
        }
    }

    pub async fn process_chunks<F, Fut>(&self, store: &StateStore, worker_count: usize, process_fn: F)
    where
        F: Fn(ChunkRange) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = Vec<ChangeEvent>> + Send,
    {
        let process_fn = Arc::new(process_fn);
        let mut handles = Vec::new();

        for worker_id in 0..worker_count {
            let chunks_arc = Arc::clone(&self.chunks);
            let fn_clone = Arc::clone(&process_fn);
            let table = self.table_name.clone();

            let handle = tokio::spawn(async move {
                loop {
                    let mut chunk_to_process = None;
                    {
                        let mut chunks = chunks_arc.lock().await;
                        for chunk in chunks.iter_mut() {
                            if chunk.status == ChunkStatus::Pending {
                                chunk.status = ChunkStatus::InProgress;
                                chunk_to_process = Some(chunk.clone());
                                break;
                            }
                        }
                    }

                    let chunk = match chunk_to_process {
                        Some(c) => c,
                        None => break,
                    };

                    println!(
                        "[PARALLEL SNAPSHOTTER] Worker {} processing table '{}' chunk {} [{}..{}]",
                        worker_id, table, chunk.chunk_id, chunk.min_key, chunk.max_key
                    );

                    let _events = fn_clone(chunk.clone()).await;

                    {
                        let mut chunks = chunks_arc.lock().await;
                        if let Some(c) = chunks.iter_mut().find(|c| c.chunk_id == chunk.chunk_id) {
                            c.status = ChunkStatus::Completed;
                        }
                    }
                }
            });

            handles.push(handle);
        }

        for handle in handles {
            let _ = handle.await;
        }

        // Save progress to state store
        let chunks = self.chunks.lock().await;
        let completed_count = chunks.iter().filter(|c| c.status == ChunkStatus::Completed).count();
        let _ = store.save_offset(
            &format!("{}_snapshot_progress", self.table_name),
            &format!("{}/{}", completed_count, chunks.len()),
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[tokio::test]
    async fn test_parallel_snapshotter_chunking() {
        let snapshotter = ParallelSnapshotter::new("users", 100, 25);
        let chunks = snapshotter.chunks.lock().await;
        assert_eq!(chunks.len(), 4);
        assert_eq!(chunks[0].min_key, 1);
        assert_eq!(chunks[0].max_key, 25);
        assert_eq!(chunks[3].min_key, 76);
        assert_eq!(chunks[3].max_key, 100);
    }

    #[tokio::test]
    async fn test_parallel_worker_execution() {
        let test_path = "./data/test_parallel_snap_db";
        let _ = fs::remove_dir_all(test_path);
        let store = StateStore::new(test_path).unwrap();

        let snapshotter = ParallelSnapshotter::new("users", 200, 50);
        snapshotter.process_chunks(&store, 2, |chunk| async move {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            Vec::new()
        }).await;

        let progress = store.get_offset("users_snapshot_progress").unwrap();
        assert_eq!(progress, Some("4/4".to_string()));

        let _ = fs::remove_dir_all(test_path);
    }
}
