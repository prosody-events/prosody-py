//! The consumer message behind a delivered Python `Message`.
//!
//! The Python `Message` is a frozen dataclass of plain values, so on its own it
//! cannot serve a message-collection write: that write stores the message's
//! Kafka coordinates, which live on core's [`ConsumerMessage`]. [`MessageCore`]
//! carries that message along with the dataclass, so every `Message` prosody
//! hands to a handler holds the message it came from and can be written back.
//!
//! Holding it also keeps the message's consumer permit held. That permit is how
//! the loader bounds how many messages are in memory at once, so it must stay
//! held for as long as Python can still reach the message.
//!
//! Rebuilding a [`ConsumerMessage`] from the dataclass fields is not an option.
//! Its constructor takes an `OwnedSemaphorePermit`, and that permit is how the
//! loader bounds how many resolved messages are in memory at once. Minting a
//! fresh semaphore to satisfy the signature hands back a permit drawn on
//! nothing and defeats the backpressure it exists to provide. It would also let
//! any object with the right attributes forge a reference to an arbitrary
//! topic, partition, and offset.

use prosody::consumer::Keyed;
use prosody::consumer::message::ConsumerMessage;
use pyo3::{Py, PyAny, PyResult, Python, pyclass};
use pythonize::pythonize;
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

/// A consumer record that has a Python dataclass form.
///
/// The positional arguments match the field order of the Python `Message`
/// and `ExciseMessage` dataclasses.
pub(crate) trait PythonRecord {
    /// Builds the Python record by calling `class` with this record's fields.
    fn to_python(&self, py: Python<'_>, class: &Py<PyAny>) -> PyResult<Py<PyAny>>;
}

impl PythonRecord for ConsumerMessage<Value> {
    fn to_python(&self, py: Python<'_>, class: &Py<PyAny>) -> PyResult<Py<PyAny>> {
        let payload = pythonize(py, self.payload())?;
        let core = Py::new(py, MessageCore::new(self.clone()))?;
        class.call1(
            py,
            (
                self.topic().as_ref(),
                self.partition(),
                self.offset(),
                *self.timestamp(),
                self.key().as_ref(),
                payload,
                self.source_system().map(AsRef::<str>::as_ref),
                self.response_requested(),
                core,
            ),
        )
    }
}

impl PythonRecord for ConsumerMessage<()> {
    fn to_python(&self, py: Python<'_>, class: &Py<PyAny>) -> PyResult<Py<PyAny>> {
        class.call1(
            py,
            (
                self.topic().as_ref(),
                self.partition(),
                self.offset(),
                *self.timestamp(),
                self.key().as_ref(),
                self.source_system().map(AsRef::<str>::as_ref),
                self.response_requested(),
            ),
        )
    }
}
