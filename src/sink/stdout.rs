use super::CdcSink;
use crate::source::ChangeEvent;
use std::convert::Infallible;

pub struct StdoutSink;

impl CdcSink for StdoutSink {
    type Error = Infallible;

    async fn send(&self, event: &ChangeEvent) -> Result<(), Self::Error> {
        let serialized = serde_json::to_string(event).unwrap_or_else(|_| "Serialization error".to_string());
        println!("[CONSOLE SINK] {}", serialized);
        Ok(())
    }
}
