//! The consumer message behind a delivered Python `Message`.
//!
//! The Python `Message` is a frozen dataclass of plain values, so on its own it
//! cannot serve a message-collection write: that write stores the message's
//! Kafka coordinates, which live on core's [`ConsumerMessage`]. [`MessageCore`]
//! carries that message along with the dataclass so the write uses the one the
//! handler received.
//!
//! Rebuilding a [`ConsumerMessage`] from the dataclass fields is not an option.
//! Its constructor takes an `OwnedSemaphorePermit`, and that permit is how the
//! loader bounds how many resolved messages are in memory at once. Minting a
//! fresh semaphore to satisfy the signature hands back a permit drawn on
//! nothing and defeats the backpressure it exists to provide. It would also let
//! any object with the right attributes forge a reference to an arbitrary
//! topic, partition, and offset.

use prosody::consumer::message::ConsumerMessage;
use pyo3::pyclass;
use serde_json::Value;

/// Opaque handle to the consumer message a delivered `Message` came from.
///
/// Rust-only: it is deliberately not registered on the `prosody` module, so
/// Python can hold one and hand it back but can never construct one.
#[pyclass(frozen)]
pub(crate) struct MessageCore(ConsumerMessage<Value>);

impl MessageCore {
    /// Wraps the message a handler is being given.
    pub(crate) fn new(message: ConsumerMessage<Value>) -> Self {
        Self(message)
    }

    /// The wrapped consumer message.
    pub(crate) fn message(&self) -> ConsumerMessage<Value> {
        self.0.clone()
    }
}
