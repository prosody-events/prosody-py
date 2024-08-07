//! Bridges Rust's Prosody library and Python for Kafka message handling.
//!
//! Defines the `PythonHandler` struct implementing the `FallibleHandler` trait,
//! enabling Python-defined message handlers to work with Prosody's Kafka
//! consumer. Manages OpenTelemetry context propagation between Rust and Python.

use std::collections::HashMap;
use std::future::Future;

use futures::pin_mut;
use opentelemetry::propagation::{TextMapCompositePropagator, TextMapPropagator};
use prosody::consumer::failure::{ClassifyError, ErrorCategory, FallibleHandler};
use prosody::consumer::message::{ConsumerMessage, MessageContext};
use prosody::propagator::new_propagator;
use pyo3::exceptions::{PyKeyboardInterrupt, PySystemExit, PyTypeError};
use pyo3::prelude::PyAnyMethods;
use pyo3::types::IntoPyDict;
use pyo3::{Bound, PyAny, PyErr, PyObject, PyResult, Python};
use pyo3_asyncio_0_21::{into_future_with_locals, TaskLocals};
use pythonize::pythonize;
use thiserror::Error;
use tokio::select;
use tracing::debug;
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
#[derive(Debug)]
pub struct PythonHandler {
    pub handle_method: PyObject,
    pub event_class: PyObject,
    pub event_set_method: PyObject,
    locals: TaskLocals,
    propagator: TextMapCompositePropagator,
}

impl Clone for PythonHandler {
    fn clone(&self) -> Self {
        Self {
            handle_method: self.handle_method.clone(),
            event_class: self.event_class.clone(),
            event_set_method: self.event_set_method.clone(),
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
        let handle_method = tracing_handler.getattr("handle")?;

        // Get a reference to the event methods
        let tsasync = py.import_bound("tsasync")?;
        let event_class = tsasync.getattr("Event")?;
        let event_set_method = event_class.getattr("set")?;

        // Capture the running event loop
        let locals = TaskLocals::with_running_loop(py)?.copy_context(py)?;

        Ok(Self {
            handle_method: handle_method.unbind(),
            event_class: event_class.unbind(),
            event_set_method: event_set_method.unbind(),
            locals,
            propagator: new_propagator(),
        })
    }
}

impl FallibleHandler for PythonHandler {
    type Error = WrappedPythonError;

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
        message: ConsumerMessage,
    ) -> Result<(), Self::Error> {
        // Serialize the OpenTelemetry context
        let mut serialized_context: HashMap<String, String> = HashMap::with_capacity(2);
        self.propagator
            .inject_context(&message.span.context(), &mut serialized_context);

        let shutdown_future = context.on_shutdown();
        let (shutdown_event, complete_future) = execute(
            context,
            message,
            serialized_context,
            &self.event_class,
            &self.handle_method,
            &self.locals,
        )?;

        pin_mut!(complete_future);
        select! {
            // Future completed
            result = complete_future.as_mut() => {
                result?;
            }

            // Shutdown signal
            () = shutdown_future => {
                debug!("cancelling task");
                cancel_task(&self.event_set_method, shutdown_event)?;

                debug!("waiting for task to complete");
                complete_future.await?;
                debug!("task complete");
            }
        }

        Ok(())
    }
}

fn cancel_task(event_set_method: &PyObject, shutdown_event: PyObject) -> PyResult<()> {
    Python::with_gil(|py| {
        event_set_method.call1(py, (shutdown_event,))?;
        Ok(())
    })
}

fn execute(
    context: MessageContext,
    message: ConsumerMessage,
    serialized_context: HashMap<String, String>,
    event_class: &PyObject,
    handle_method: &PyObject,
    locals: &TaskLocals,
) -> PyResult<(
    PyObject,
    impl Future<Output = PyResult<PyObject>> + Send + Sized,
)> {
    Python::with_gil(move |py| {
        // Create Context and Message objects for the Python handler
        let message_context = Context(context);

        let ConsumerMessage {
            topic,
            partition,
            offset,
            key,
            timestamp,
            payload,
            ..
        } = message;

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

        // Create asyncio.Event for shutdown signaling
        let shutdown_event = event_class.call0(py)?;

        // Call the TracingHandler's handle method to build coroutine
        let coroutine = handle_method
            .call1(
                py,
                (message_context, message, otel_context, &shutdown_event),
            )?
            .into_bound(py);

        // Convert coroutine into a future
        let complete_future = into_future_with_locals(locals, coroutine)?;
        Ok((shutdown_event, complete_future))
    })
}

#[derive(Debug, Error)]
pub enum WrappedPythonError {
    #[error(transparent)]
    Python(#[from] PyErr),
}

impl ClassifyError for WrappedPythonError {
    fn classify_error(&self) -> ErrorCategory {
        match self {
            WrappedPythonError::Python(error) => Python::with_gil(|py| {
                let Ok(asyncio) = py.import_bound("asyncio") else {
                    return ErrorCategory::Terminal;
                };

                let Ok(cancelled_error) = asyncio.getattr("CancelledError") else {
                    return ErrorCategory::Terminal;
                };

                if error.is_instance_bound(py, &cancelled_error)
                    || error.is_instance_of::<PyKeyboardInterrupt>(py)
                    || error.is_instance_of::<PySystemExit>(py)
                {
                    ErrorCategory::Terminal
                } else {
                    ErrorCategory::Transient
                }
            }),
        }
    }
}
