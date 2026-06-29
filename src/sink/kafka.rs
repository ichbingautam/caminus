use super::CdcSink;
use crate::source::ChangeEvent;
use std::time::Duration;

pub struct KafkaSink {
    pub brokers: String,
    pub topic: String,
    pub client_id: String,
}

impl KafkaSink {
    pub fn new(brokers: String, topic: String, client_id: String) -> Self {
        Self {
            brokers,
            topic,
            client_id,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum KafkaSinkError {
    #[error("Broker connection error: {0}")]
    Connection(String),
    #[error("Message delivery failed: {0}")]
    Delivery(String),
}

impl CdcSink for KafkaSink {
    type Error = KafkaSinkError;

    async fn send(&self, event: &ChangeEvent) -> Result<(), Self::Error> {
        // Simulating network latency to Kafka/Redpanda brokers
        tokio::time::sleep(Duration::from_millis(15)).await;
        
        let payload = serde_json::to_string(event).map_err(|e| KafkaSinkError::Delivery(e.to_string()))?;
        println!(
            "[KAFKA SINK] Sent to topic '{}' on brokers '{}' - Payload size: {} bytes",
            self.topic, self.brokers, payload.len()
        );
        Ok(())
    }
}
