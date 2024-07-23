//! Provides a bridge between Rust's Prosody library and Python for handling
//! Kafka messages.
//!
//! This module defines the `PythonHandler` struct, which implements the
//! `FallibleHandler` trait, allowing Python-defined message handlers to be used
//! with Prosody's Kafka consumer.

use std::sync::Arc;

use prosody::consumer::failure::FallibleHandler;
use prosody::consumer::message::{ConsumerMessage, MessageContext};
use pyo3::exceptions::PyTypeError;
use pyo3::prelude::PyAnyMethods;
use pyo3::{Bound, PyAny, PyErr, PyObject, PyResult, Python};
use pythonize::pythonize;
use tracing::error;

use crate::message::{Context, Message};

/// The name of the abstract Python class that user-defined handlers must
/// subclass.
const HANDLER_CLASS_NAME: &str = "AbstractMessageHandler";

/// A wrapper for Python-defined message handlers.
///
/// This struct holds a reference to the Python `handle` method and implements
/// the `FallibleHandler` trait, allowing it to be used with Prosody's Kafka
/// consumer.
#[derive(Clone)]
pub struct PythonHandler {
    pub handle_method: Arc<PyObject>,
}

impl PythonHandler {
    /// Creates a new `PythonHandler` from a Python object.
    ///
    /// # Arguments
    ///
    /// * `handler` - A Python object that should be a subclass of
    ///   `AbstractMessageHandler`.
    ///
    /// # Returns
    ///
    /// A `PyResult` containing the new `PythonHandler` if successful.
    ///
    /// # Errors
    ///
    /// Returns a `PyTypeError` if the provided handler is not a subclass of
    /// `AbstractMessageHandler`.
    pub fn new(handler: &Bound<PyAny>) -> PyResult<Self> {
        let abstract_handler_class = handler
            .py()
            .import_bound("prosody")?
            .getattr(HANDLER_CLASS_NAME)?;

        // Ensure the provided handler is a subclass of AbstractMessageHandler
        if !handler.is_instance(&abstract_handler_class)? {
            return Err(PyTypeError::new_err(format!(
                "handler must be a subclass of {HANDLER_CLASS_NAME}"
            )));
        };

        // Extract and store the handle method
        let handle_method = Arc::new(handler.getattr("handle")?.unbind());
        Ok(Self { handle_method })
    }
}

impl FallibleHandler for PythonHandler {
    type Error = PyErr;

    /// Handles a Kafka message by calling the Python-defined handle method.
    ///
    /// # Arguments
    ///
    /// * `context` - The message context.
    /// * `message` - The consumer message to be handled.
    ///
    /// # Returns
    ///
    /// A `Result` indicating success or containing a `PyErr` if an error
    /// occurred.
    async fn handle(
        &self,
        context: MessageContext,
        message: ConsumerMessage,
    ) -> Result<(), Self::Error> {
        let topic = message.topic;
        let partition = message.partition;
        let offset = message.offset;
        let timestamp = message.timestamp;
        let key = message.key;
        let payload = message.payload;

        Python::with_gil(|py| {
            // Convert the payload to a Python object
            let payload = match pythonize(py, &payload) {
                Ok(payload) => payload,
                Err(error) => {
                    error!(
                        %topic, %partition, %offset, %key,
                        "unable to decode payload: {error:#}; discarding message"
                    );

                    return Ok(());
                }
            };

            // Create Context and Message objects for the Python handler
            let context = Context(context);
            let message = Message {
                topic,
                partition,
                offset,
                timestamp,
                key,
                payload,
            };

            // Call the Python handle method
            self.handle_method.call1(py, (context, message))?;
            Ok(())
        })
    }
}
