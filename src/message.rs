//! Defines the `Message` struct and its associated methods for representing
//! Kafka messages.

use prosody::{Key, Offset, Partition, Topic};
use pyo3::types::PyAnyMethods;
use pyo3::{pyclass, pymethods, PyObject, PyResult, PyTraverseError, PyVisit, Python};

/// A Kafka message with associated metadata.
///
/// This struct encapsulates the core components of a Kafka message, including
/// its topic, partition, offset, key, and payload.
#[pyclass(frozen)]
pub struct Message {
    topic: Topic,
    partition: Partition,
    offset: Offset,
    key: Key,
    payload: PyObject,
}

#[pymethods]
impl Message {
    /// Returns the topic of the message.
    ///
    /// Returns:
    ///     str: The topic name.
    pub fn topic(&self) -> &'static str {
        self.topic.as_ref()
    }

    /// Returns the partition of the message.
    ///
    /// Returns:
    ///     int: The partition number.
    pub fn partition(&self) -> Partition {
        self.partition
    }

    /// Returns the offset of the message.
    ///
    /// Returns:
    ///     int: The message offset.
    pub fn offset(&self) -> Offset {
        self.offset
    }

    /// Returns the key of the message.
    ///
    /// Returns:
    ///     str: The message key.
    pub fn key(&self) -> &str {
        &self.key
    }

    /// Returns the payload of the message.
    ///
    /// Returns:
    ///     object: The message payload.
    pub fn payload(&self) -> &PyObject {
        &self.payload
    }

    /// Returns a string representation of the Message.
    ///
    /// This method provides a human-readable summary of the message, including
    /// its topic, partition, offset, and a truncated version of the payload.
    ///
    /// Returns:
    ///     str: A human-readable representation of the Message.
    ///
    /// Raises:
    ///     ValueError: If the payload cannot be converted to a string.
    fn __str__(&self, py: Python) -> PyResult<String> {
        // Convert payload to string and truncate if necessary
        let payload_str = self.payload.bind(py).str()?.extract::<String>()?;
        let truncated_payload = if payload_str.len() > 50 {
            format!("{}...", &payload_str[..47])
        } else {
            payload_str
        };

        // Format the string representation
        Ok(format!(
            "Kafka message on topic '{}' (partition {}, offset {}): {}",
            self.topic(),
            self.partition(),
            self.offset(),
            truncated_payload
        ))
    }

    /// Returns a detailed string representation of the Message.
    ///
    /// This method provides a more detailed representation of the message,
    /// including all fields. It's primarily used for debugging purposes.
    ///
    /// Returns:
    ///     str: A detailed representation of the Message.
    ///
    /// Raises:
    ///     ValueError: If the payload cannot be converted to a string
    ///     representation.
    fn __repr__(&self, py: Python) -> PyResult<String> {
        // Get the string representation of the payload
        let payload_repr = self.payload.bind(py).repr()?.extract::<String>()?;

        // Format the detailed string representation
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
    /// Arguments:
    ///     visit: A `PyVisit` object used to visit Python objects.
    ///
    /// Returns:
    ///     Result<(), PyTraverseError>: Ok if traversal was successful, Err
    ///     otherwise.
    #[allow(clippy::needless_pass_by_value)]
    fn __traverse__(&self, visit: PyVisit) -> Result<(), PyTraverseError> {
        visit.call(&self.payload)?;
        Ok(())
    }
}
