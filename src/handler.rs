//! Bridges Rust's Prosody library and Python for Kafka message handling.
//!
//! This module defines the `PythonHandler` struct, which implements the
//! `FallibleHandler` trait, enabling Python-defined message handlers to work
//! with Prosody's Kafka consumer. It also manages OpenTelemetry context
//! propagation between Rust and Python, handles task cancellation, and provides
//! error classification for Python errors.

use std::collections::HashMap;
use std::future::Future;
use std::time::Duration;

use futures::pin_mut;
use opentelemetry::propagation::{TextMapCompositePropagator, TextMapPropagator};
use prosody::consumer::failure::{ClassifyError, ErrorCategory, FallibleHandler};
use prosody::consumer::message::{ConsumerMessage, MessageContext};
use prosody::consumer::Keyed;
use prosody::propagator::new_propagator;
use pyo3::exceptions::{PyKeyboardInterrupt, PySystemExit, PyTypeError};
use pyo3::prelude::PyAnyMethods;
use pyo3::types::IntoPyDict;
use pyo3::{Bound, PyAny, PyErr, PyObject, PyResult, Python};
use pyo3_asyncio_0_21::{into_future_with_locals, TaskLocals};
use pythonize::pythonize;
use thiserror::Error;
use tokio::select;
use tokio::time::sleep;
use tracing::{debug, instrument, warn};
use tracing_opentelemetry::OpenTelemetrySpanExt;

use crate::context::Context;

/// Name of the abstract Python class that user-defined handlers must subclass.
const HANDLER_CLASS_NAME: &str = "EventHandler";

/// Name of the Python class that wraps the user-defined handler for tracing and
/// cancellation.
const HANDLER_WRAPPER_CLASS_NAME: &str = "ProsodyHandler";

/// Name of the Python class representing a Kafka message.
const MESSAGE_CLASS_NAME: &str = "Message";

/// A wrapper for Python-defined message handlers.
///
/// This struct holds references to Python objects and methods necessary for
/// handling Kafka messages and implements the `FallibleHandler` trait for use
/// with Prosody's Kafka consumer.
#[derive(Debug)]
pub struct PythonHandler {
    pub handle_method: PyObject,
    pub message_class: PyObject,
    pub event_class: PyObject,
    pub event_set_method: PyObject,
    shutdown_grace_period: Duration,
    locals: TaskLocals,
    propagator: TextMapCompositePropagator,
}

impl Clone for PythonHandler {
    fn clone(&self) -> Self {
        Self {
            handle_method: self.handle_method.clone(),
            message_class: self.message_class.clone(),
            event_class: self.event_class.clone(),
            event_set_method: self.event_set_method.clone(),
            shutdown_grace_period: self.shutdown_grace_period,
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
    /// * `handler` - A Python object subclassing `EventHandler`.
    /// * `shutdown_grace_period` - The duration to wait for a task to complete
    ///   during shutdown.
    ///
    /// # Returns
    ///
    /// A `PyResult` containing the new `PythonHandler` if successful.
    ///
    /// # Errors
    ///
    /// Returns a `PyTypeError` if `handler` is not a subclass of
    /// `EventHandler`.
    pub fn new(handler: &Bound<PyAny>, shutdown_grace_period: Duration) -> PyResult<Self> {
        let py = handler.py();
        let prosody_module = py.import_bound("prosody")?;
        let abstract_handler_class = prosody_module.getattr(HANDLER_CLASS_NAME)?;
        let tracing_handler_class = prosody_module.getattr(HANDLER_WRAPPER_CLASS_NAME)?;
        let message_class = prosody_module.getattr(MESSAGE_CLASS_NAME)?;

        // Ensure the provided handler is a subclass of EventHandler
        if !handler.is_instance(&abstract_handler_class)? {
            return Err(PyTypeError::new_err(format!(
                "handler must be a subclass of {HANDLER_CLASS_NAME}"
            )));
        }

        // Create a ProsodyHandler instance and get its handle method
        let tracing_handler = tracing_handler_class.call1((handler,))?;
        let handle_method = tracing_handler.getattr("on_message")?;

        // Get a reference to the event methods
        let tsasync = py.import_bound("tsasync")?;
        let event_class = tsasync.getattr("Event")?;
        let event_set_method = event_class.getattr("set")?;

        // Capture the running event loop
        let locals = TaskLocals::with_running_loop(py)?.copy_context(py)?;

        Ok(Self {
            handle_method: handle_method.unbind(),
            message_class: message_class.unbind(),
            event_class: event_class.unbind(),
            event_set_method: event_set_method.unbind(),
            shutdown_grace_period,
            locals,
            propagator: new_propagator(),
        })
    }
}

impl FallibleHandler for PythonHandler {
    type Error = WrappedPythonError;

    /// Handles a Kafka message by calling the Python-defined `ProsodyHandler`'s
    /// handle method.
    ///
    /// # Arguments
    ///
    /// * `context` - The message context.
    /// * `message` - The consumer message to be handled.
    ///
    /// # Returns
    ///
    /// A `Result` indicating success or containing a `WrappedPythonError` if an
    /// error occurred.
    #[instrument(level = "debug", skip(self), err)]
    async fn on_message(
        &self,
        context: MessageContext,
        message: ConsumerMessage,
    ) -> Result<(), Self::Error> {
        // Serialize the OpenTelemetry context
        let mut serialized_context: HashMap<String, String> = HashMap::with_capacity(2);
        self.propagator
            .inject_context(&message.span().context(), &mut serialized_context);

        let shutdown_future = context.on_shutdown();
        let (shutdown_event, complete_future) = execute(
            context,
            message,
            serialized_context,
            &self.message_class,
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
                debug!("shutdown signal received; waiting for task to complete");
                select! {
                    () = sleep(self.shutdown_grace_period) => {
                        warn!("timeout exceeded; cancelling task");
                        cancel_task(&self.event_set_method, shutdown_event)?;

                        debug!("waiting for task to cleanup");
                        complete_future.await?;
                    }

                    result = complete_future.as_mut() => {
                        result?;
                    }
                }
                debug!("task shutdown");
            }
        }

        Ok(())
    }
}

/// Cancels a Python task by setting its shutdown event.
///
/// # Arguments
///
/// * `event_set_method` - The Python method to set the event.
/// * `shutdown_event` - The Python event object to be set.
///
/// # Returns
///
/// A `PyResult` indicating success or containing a `PyErr` if an error
/// occurred.
fn cancel_task(event_set_method: &PyObject, shutdown_event: PyObject) -> PyResult<()> {
    Python::with_gil(|py| {
        event_set_method.call1(py, (shutdown_event,))?;
        Ok(())
    })
}

/// Prepares and executes a Python handler for a Kafka message.
///
/// # Arguments
///
/// * `context` - The message context.
/// * `message` - The consumer message to be handled.
/// * `serialized_context` - The serialized OpenTelemetry context.
/// * `message_class` - The Python Message class.
/// * `event_class` - The Python Event class.
/// * `handle_method` - The Python handler method.
/// * `locals` - The task locals for the Python event loop.
///
/// # Returns
///
/// A tuple containing the shutdown event and the future representing the
/// handler execution.
///
/// # Errors
///
/// Returns a `PyErr` if there's an error during Python object creation or
/// method calls.
fn execute(
    context: MessageContext,
    message: ConsumerMessage,
    serialized_context: HashMap<String, String>,
    message_class: &PyObject,
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
        let payload = pythonize(py, message.payload())?;

        let message = message_class.call1(
            py,
            (
                message.topic().as_ref(),
                message.partition(),
                message.offset(),
                *message.timestamp(),
                message.key().as_ref(),
                payload,
            ),
        )?;

        // Convert serialized_context to a Python dict
        let otel_context = serialized_context.into_py_dict_bound(py);

        // Create asyncio.Event for shutdown signaling
        let shutdown_event = event_class.call0(py)?;

        // Call the ProsodyHandler's handle method to build coroutine
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

/// Wraps Python errors that may occur during message handling.
#[derive(Debug, Error)]
pub enum WrappedPythonError {
    #[error(transparent)]
    Python(#[from] PyErr),
}

impl ClassifyError for WrappedPythonError {
    /// Classifies the error as either Terminal or Transient.
    ///
    /// # Returns
    ///
    /// * `ErrorCategory::Terminal` for `asyncio.CancelledError`,
    ///   `KeyboardInterrupt`, or `SystemExit`.
    /// * `ErrorCategory::Transient` for all other Python errors.
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
