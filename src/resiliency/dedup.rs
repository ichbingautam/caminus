use std::collections::HashSet;
use std::collections::VecDeque;

pub struct DeduplicationFilter {
    processed_ids: HashSet<String>,
    order: VecDeque<String>,
    max_capacity: usize,
}

impl DeduplicationFilter {
    pub fn new(max_capacity: usize) -> Self {
        Self {
            processed_ids: HashSet::new(),
            order: VecDeque::new(),
            max_capacity,
        }
    }

    /// Returns true if the event_id is a duplicate (already processed), 
    /// otherwise inserts it and returns false.
    pub fn check_and_track(&mut self, event_id: &str) -> bool {
        if self.processed_ids.contains(event_id) {
            true
        } else {
            self.processed_ids.insert(event_id.to_string());
            self.order.push_back(event_id.to_string());
            
            // Limit cache memory growth via FIFO eviction
            if self.order.len() > self.max_capacity {
                if let Some(oldest) = self.order.pop_front() {
                    self.processed_ids.remove(&oldest);
                }
            }
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_dedup_filtering() {
        let mut filter = DeduplicationFilter::new(3);

        assert!(!filter.check_and_track("evt-1"));
        assert!(filter.check_and_track("evt-1"));

        assert!(!filter.check_and_track("evt-2"));
        assert!(!filter.check_and_track("evt-3"));

        // Add 4th item, evicting the oldest (evt-1)
        assert!(!filter.check_and_track("evt-4"));

        // evt-1 is now evicted and should be accepted again
        assert!(!filter.check_and_track("evt-1"));
    }
}
