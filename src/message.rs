//! Defines structures and methods for representing Kafka messages in a
//! Python-compatible format.
//!
//! This module provides the `Message` struct to encapsulate Kafka message data
//! and metadata, and the `Context` struct to hold message context information.
//! It also implements methods for accessing message properties and representing
//! messages as strings.

use chrono::{DateTime, Utc};
use prosody::consumer::message::{ConsumerMessage, MessageContext};
use prosody::consumer::Keyed;
use prosody::{Offset, Partition};
use pyo3::types::PyAnyMethods;
use pyo3::{pyclass, pymethods, PyObject, PyResult, PyTraverseError, PyVisit, Python};

/// A Kafka message with associated metadata.
///
/// This struct encapsulates the core components of a Kafka message, including
/// its topic, partition, offset, timestamp, key, and payload.
#[pyclass(frozen)]
pub struct Message {
    pub inner: ConsumerMessage,
    pub payload: PyObject,
}

/// Holds context information for a Kafka message.
#[pyclass]
pub struct Context(pub MessageContext);

#[pymethods]
impl Message {
    /// Returns the topic of the message.
    ///
    /// # Returns
    ///
    /// A string slice containing the topic name.
    pub fn topic(&self) -> &'static str {
        self.inner.topic().as_ref()
    }

    /// Returns the partition of the message.
    ///
    /// # Returns
    ///
    /// The partition number as a `Partition` type.
    pub fn partition(&self) -> Partition {
        self.inner.partition()
    }

    /// Returns the offset of the message.
    ///
    /// # Returns
    ///
    /// The message offset as an `Offset` type.
    pub fn offset(&self) -> Offset {
        self.inner.offset()
    }

    /// Returns the timestamp of the message.
    ///
    /// # Returns
    ///
    /// The message timestamp as a `DateTime<Utc>`.
    pub fn timestamp(&self) -> DateTime<Utc> {
        *self.inner.timestamp()
    }

    /// Returns the key of the message.
    ///
    /// # Returns
    ///
    /// A string slice containing the message key.
    pub fn key(&self) -> &str {
        self.inner.key()
    }

    /// Returns the payload of the message.
    ///
    /// # Returns
    ///
    /// A reference to the message payload as a `PyObject`.
    pub fn payload(&self) -> &PyObject {
        &self.payload
    }

    /// Returns a string representation of the Message.
    ///
    /// This method provides a human-readable summary of the message, including
    /// its topic, partition, offset, key, and a truncated version of the
    /// payload.
    ///
    /// # Arguments
    ///
    /// * `py` - The Python interpreter token.
    ///
    /// # Returns
    ///
    /// A `PyResult` containing a `String` representation of the Message.
    ///
    /// # Errors
    ///
    /// Returns a `PyErr` if the payload cannot be converted to a string.
    pub fn __str__(&self, py: Python) -> PyResult<String> {
        // Convert payload to string and truncate if necessary
        let payload_str = self.payload.bind(py).str()?.extract::<String>()?;
        let truncated_payload = if payload_str.len() > 50 {
            format!("{}...", &payload_str[..47])
        } else {
            payload_str
        };

        // Format and return the string representation
        Ok(format!(
            "Kafka message on topic '{}' (partition {}, offset {}, key '{}'): {}",
            self.topic(),
            self.partition(),
            self.offset(),
            self.key(),
            truncated_payload
        ))
    }

    /// Returns a detailed string representation of the Message.
    ///
    /// This method provides a more detailed representation of the message,
    /// including all fields. It's primarily used for debugging purposes.
    ///
    /// # Arguments
    ///
    /// * `py` - The Python interpreter token.
    ///
    /// # Returns
    ///
    /// A `PyResult` containing a detailed `String` representation of the
    /// Message.
    ///
    /// # Errors
    ///
    /// Returns a `PyErr` if the payload cannot be converted to a string
    /// representation.
    pub fn __repr__(&self, py: Python) -> PyResult<String> {
        // Get the string representation of the payload
        let payload_repr = self.payload.bind(py).repr()?.extract::<String>()?;

        // Format and return the detailed string representation
        Ok(format!(
            "Message(topic='{}', partition={}, offset={}, key='{}', payload={})",
            self.topic(),
            self.partition(),
            self.offset(),
            self.key(),
            payload_repr
        ))
    }

    /// Traverses the Python objects contained in this Message.
    ///
    /// This method is used by Python's garbage collector to traverse
    /// the object graph and detect cycles.
    ///
    /// # Arguments
    ///
    /// * `visit` - A `PyVisit` object used to visit Python objects.
    ///
    /// # Returns
    ///
    /// * `Ok(())` if the traversal was successful.
    ///
    /// # Errors
    ///
    /// Returns `Err(PyTraverseError)` if an error occurs during the traversal.
    #[allow(clippy::needless_pass_by_value)]
    pub fn __traverse__(&self, visit: PyVisit) -> Result<(), PyTraverseError> {
        visit.call(&self.payload)?;
        Ok(())
    }
}
