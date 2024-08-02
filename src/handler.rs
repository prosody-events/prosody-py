//! Bridges Rust's Prosody library and Python for Kafka message handling.
//!
//! Defines the `PythonHandler` struct implementing the `FallibleHandler` trait,
//! enabling Python-defined message handlers to work with Prosody's Kafka
//! consumer. Manages OpenTelemetry context propagation between Rust and Python.

use std::collections::HashMap;
use std::sync::Arc;

use opentelemetry::propagation::{TextMapCompositePropagator, TextMapPropagator};
use prosody::consumer::failure::FallibleHandler;
use prosody::consumer::message::{ConsumerMessage, MessageContext};
use prosody::propagator::new_propagator;
use pyo3::exceptions::PyTypeError;
use pyo3::prelude::PyAnyMethods;
use pyo3::types::IntoPyDict;
use pyo3::{Bound, IntoPy, PyAny, PyErr, PyObject, PyResult, Python};
use pyo3_asyncio_0_21::{into_future_with_locals, TaskLocals};
use pythonize::pythonize;
use tracing_opentelemetry::OpenTelemetrySpanExt;

use crate::message::{Context, Message};

/// The name of the abstract Python class that user-defined handlers must
/// subclass.
const HANDLER_CLASS_NAME: &str = "AbstractMessageHandler";

/// The name of the Python class that wraps the user-defined handler for
/// tracing.
const TRACING_HANDLER_CLASS_NAME: &str = "TracingHandler";

/// A wrapper for Python-defined message handlers.
///
/// Holds a reference to the Python `TracingHandler`'s `handle` method and
/// implements the `FallibleHandler` trait for use with Prosody's Kafka
/// consumer.
pub struct PythonHandler {
    pub handle_method: Arc<PyObject>,
    locals: TaskLocals,
    propagator: TextMapCompositePropagator,
}

impl Clone for PythonHandler {
    fn clone(&self) -> Self {
        Self {
            handle_method: Arc::clone(&self.handle_method),
            locals: self.locals.clone(),
            propagator: new_propagator(),
        }
    }
}

impl PythonHandler {
    /// Creates a new `PythonHandler` from a Python object.
    ///
    /// # Arguments
    ///
    /// * `handler` - A Python object subclassing `AbstractMessageHandler`.
    ///
    /// # Returns
    ///
    /// A `PyResult` containing the new `PythonHandler` if successful.
    ///
    /// # Errors
    ///
    /// Returns a `PyTypeError` if `handler` is not a subclass of
    /// `AbstractMessageHandler`.
    pub fn new(handler: &Bound<PyAny>) -> PyResult<Self> {
        let py = handler.py();
        let prosody_module = py.import_bound("prosody")?;
        let abstract_handler_class = prosody_module.getattr(HANDLER_CLASS_NAME)?;
        let tracing_handler_class = prosody_module.getattr(TRACING_HANDLER_CLASS_NAME)?;

        // Ensure the provided handler is a subclass of AbstractMessageHandler
        if !handler.is_instance(&abstract_handler_class)? {
            return Err(PyTypeError::new_err(format!(
                "handler must be a subclass of {HANDLER_CLASS_NAME}"
            )));
        }

        // Create a TracingHandler instance and get its handle method
        let tracing_handler = tracing_handler_class.call1((handler,))?;
        let handle_method = Arc::new(tracing_handler.getattr("handle")?.into_py(py));

        // Capture the running event loop
        let locals = TaskLocals::with_running_loop(py)?.copy_context(py)?;

        Ok(Self {
            handle_method,
            locals,
            propagator: new_propagator(),
        })
    }
}

impl FallibleHandler for PythonHandler {
    type Error = PyErr;

    /// Handles a Kafka message by calling the Python-defined `TracingHandler`'s
    /// handle method.
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
        ConsumerMessage {
            topic,
            partition,
            offset,
            key,
            timestamp,
            payload,
            span,
        }: ConsumerMessage,
    ) -> Result<(), Self::Error> {
        // Serialize the OpenTelemetry context
        let mut serialized_context: HashMap<String, String> = HashMap::with_capacity(2);
        self.propagator
            .inject_context(&span.context(), &mut serialized_context);

        let future = Python::with_gil(|py| {
            // Create Context and Message objects for the Python handler
            let message_context = Context(context);
            let message = Message {
                topic,
                partition,
                offset,
                timestamp,
                key,
                payload: pythonize(py, &payload)?,
            };

            // Convert serialized_context to a Python dict
            let otel_context = serialized_context.into_py_dict_bound(py);

            // Call the TracingHandler's handle method
            into_future_with_locals(
                &self.locals,
                self.handle_method
                    .call1(py, (message_context, message, otel_context))?
                    .into_bound(py),
            )
        })?;

        future.await?;
        Ok(())
    }
}
