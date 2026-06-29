use crate::source::ChangeEvent;
use std::error::Error;

pub mod stdout;
pub mod kafka;

pub trait CdcSink: Send + Sync {
    type Error: Error + Send + Sync + 'static;

    fn send(
        &self,
        event: &ChangeEvent,
    ) -> impl std::future::Future<Output = Result<(), Self::Error>> + Send;
}
