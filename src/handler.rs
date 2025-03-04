//! Bridges Python and Prosody for Kafka message handling.
//!
//! Enables Python-defined handlers to process Kafka messages through Prosody
//! by:
//! - Implementing `FallibleHandler` for Python message handlers
//! - Propagating OpenTelemetry context between Rust and Python
//! - Managing graceful task cancellation during shutdown
//! - Classifying Python errors for retry/failure handling

use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use futures::pin_mut;
use opentelemetry::propagation::{TextMapCompositePropagator, TextMapPropagator};
use prosody::consumer::Keyed;
use prosody::consumer::failure::{ClassifyError, ErrorCategory, FallibleHandler};
use prosody::consumer::message::{ConsumerMessage, MessageContext};
use prosody::propagator::new_propagator;
use pyo3::exceptions::PyTypeError;
use pyo3::prelude::PyAnyMethods;
use pyo3::types::IntoPyDict;
use pyo3::{Bound, PyAny, PyErr, PyObject, PyResult, Python};
use pyo3_async_runtimes::{TaskLocals, into_future_with_locals};
use pythonize::pythonize;
use thiserror::Error;
use tokio::select;
use tokio::time::sleep;
use tracing::{debug, error, instrument, warn};
use tracing_opentelemetry::OpenTelemetrySpanExt;

use crate::context::Context;

/// Base Python class name for message handlers
const HANDLER_CLASS_NAME: &str = "EventHandler";

/// Python wrapper class name for tracing/cancellation
const HANDLER_WRAPPER_CLASS_NAME: &str = "ProsodyHandler";

/// Python class name for Kafka messages
const MESSAGE_CLASS_NAME: &str = "Message";

/// A wrapper for Python-defined message handlers.
///
/// This struct holds references to Python objects and methods necessary for
/// handling Kafka messages and implements the `FallibleHandler` trait for use
/// with Prosody's Kafka consumer.
#[derive(Clone, Debug)]
pub struct PythonHandler(Arc<PythonHandlerImpl>);

/// Implementation details for Python message handlers
#[derive(Debug)]
pub struct PythonHandlerImpl {
    pub handle_method: PyObject,
    pub message_class: PyObject,
    pub event_class: PyObject,
    pub event_set_method: PyObject,
    shutdown_grace_period: Duration,
    locals: TaskLocals,
    propagator: TextMapCompositePropagator,
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
        let prosody_module = py.import("prosody")?;
        let abstract_handler_class = prosody_module.getattr(HANDLER_CLASS_NAME)?;
        let tracing_handler_class = prosody_module.getattr(HANDLER_WRAPPER_CLASS_NAME)?;
        let message_class = prosody_module.getattr(MESSAGE_CLASS_NAME)?;

        // Verify handler inherits from EventHandler
        if !handler.is_instance(&abstract_handler_class)? {
            return Err(PyTypeError::new_err(format!(
                "handler must be a subclass of {HANDLER_CLASS_NAME}"
            )));
        }

        // Wrap handler with tracing/cancellation support
        let tracing_handler = tracing_handler_class.call1((handler,))?;
        let handle_method = tracing_handler.getattr("on_message")?;

        // Get a reference to the event methods
        let tsasync = py.import("tsasync")?;
        let event_class = tsasync.getattr("Event")?;
        let event_set_method = event_class.getattr("set")?;

        // Capture the running event loop
        let locals = TaskLocals::with_running_loop(py)?.copy_context(py)?;

        Ok(Self(Arc::new(PythonHandlerImpl {
            handle_method: handle_method.unbind(),
            message_class: message_class.unbind(),
            event_class: event_class.unbind(),
            event_set_method: event_set_method.unbind(),
            shutdown_grace_period,
            locals,
            propagator: new_propagator(),
        })))
    }

    /// Gets the Python message handler method
    pub fn handle_method(&self) -> &PyObject {
        &self.0.handle_method
    }

    /// Gets the Python Message class
    pub fn message_class(&self) -> &PyObject {
        &self.0.message_class
    }

    /// Gets the Python Event class
    pub fn event_class(&self) -> &PyObject {
        &self.0.event_class
    }

    /// Gets the Python Event.set method
    pub fn event_set_method(&self) -> &PyObject {
        &self.0.event_set_method
    }
}

impl FallibleHandler for PythonHandler {
    type Error = WrappedPythonError;

    /// Processes a Kafka message by invoking the Python handler.
    ///
    /// # Arguments
    ///
    /// * `context` - Message processing context
    /// * `message` - Kafka message to process
    ///
    /// # Errors
    ///
    /// Returns `WrappedPythonError` on Python exceptions or task cancellation
    /// failures
    #[instrument(level = "debug", skip(self), err)]
    async fn on_message(
        &self,
        context: MessageContext,
        message: ConsumerMessage,
    ) -> Result<(), Self::Error> {
        // Propagate tracing context to Python
        let mut serialized_context: HashMap<String, String> = HashMap::with_capacity(2);
        self.0
            .propagator
            .inject_context(&message.span().context(), &mut serialized_context);

        let shutdown_future = context.on_shutdown();
        let (shutdown_event, complete_future) = execute(
            context,
            message,
            serialized_context,
            &self.0.message_class,
            &self.0.event_class,
            &self.0.handle_method,
            &self.0.locals,
        )?;

        pin_mut!(complete_future);
        select! {
            // Handle normal completion
            result = complete_future.as_mut() => {
                if let Err(error) = log_exception(&result) {
                    error!("message handling failed but error could not be logged: {error:#}");
                }
                result?;
            }

            // Handle shutdown request
            () = shutdown_future => {
                debug!("shutdown signal received; waiting for task to complete");
                select! {
                    () = sleep(self.0.shutdown_grace_period) => {
                        warn!("timeout exceeded; cancelling task");
                        cancel_task(&self.0.event_set_method, shutdown_event)?;

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

/// Logs Python exceptions with full traceback information.
///
/// # Arguments
///
/// * `result` - `PyResult` containing a potential Python error to log
///
/// # Returns
///
/// A `PyResult` indicating whether logging succeeded
///
/// # Errors
///
/// Returns `PyErr` if accessing traceback information fails
fn log_exception(result: &PyResult<PyObject>) -> PyResult<()> {
    let Err(error) = result else {
        return Ok(());
    };

    Python::with_gil(|py| {
        let traceback = py.import("traceback")?;
        let exc_info = (error.get_type(py), error.value(py), error.traceback(py));

        let traceback: Vec<String> = traceback
            .getattr("format_exception")?
            .call1(exc_info)?
            .extract()?;

        let traceback = traceback.join("");

        error!(%traceback, "message handling failed: {error:#}");
        Ok(())
    })
}

/// Cancels a Python task by signaling its shutdown event
///
/// # Arguments
///
/// * `event_set_method` - Python Event.set method
/// * `shutdown_event` - Event to signal
///
/// # Errors
///
/// Returns `PyErr` if setting the event fails
fn cancel_task(event_set_method: &PyObject, shutdown_event: PyObject) -> PyResult<()> {
    Python::with_gil(|py| {
        event_set_method.call1(py, (shutdown_event,))?;
        Ok(())
    })
}

/// Prepares and executes a Python message handler
///
/// # Arguments
///
/// * `context` - Message context
/// * `message` - Kafka message
/// * `serialized_context` - OpenTelemetry context
/// * `message_class` - Python Message class
/// * `event_class` - Python Event class
/// * `handle_method` - Python handler method
/// * `locals` - Python event loop task locals
///
/// # Returns
///
/// Tuple of (shutdown event, handler future)
///
/// # Errors
///
/// Returns `PyErr` on Python object creation/method call failures
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
        // Create Python message objects
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
        let otel_context = serialized_context.into_py_dict(py)?;

        // Create asyncio.Event for shutdown signaling
        let shutdown_event = event_class.call0(py)?;

        // Create and convert handler coroutine to future
        let coroutine = handle_method
            .call1(
                py,
                (message_context, message, otel_context, &shutdown_event),
            )?
            .into_bound(py);

        let complete_future = into_future_with_locals(locals, coroutine)?;
        Ok((shutdown_event, complete_future))
    })
}

/// Python errors from message handling
#[derive(Debug, Error)]
pub enum WrappedPythonError {
    /// Underlying Python exception
    #[error(transparent)]
    Python(#[from] PyErr),
}

impl ClassifyError for WrappedPythonError {
    /// Determines error retry behavior based on Python error attributes
    ///
    /// Returns:
    /// - `ErrorCategory::Permanent` for errors with `is_permanent=True`
    /// - `ErrorCategory::Transient` otherwise
    fn classify_error(&self) -> ErrorCategory {
        match self {
            WrappedPythonError::Python(error) => {
                Python::with_gil(|py| match is_permanent_error(py, error) {
                    Ok(true) => ErrorCategory::Permanent,
                    _ => ErrorCategory::Transient,
                })
            }
        }
    }
}

/// Checks if a Python error is marked as permanent
///
/// # Arguments
///
/// * `py` - Python interpreter token
/// * `error` - Error to check
///
/// # Returns
///
/// Whether error has `is_permanent=True`
fn is_permanent_error(py: Python, error: &PyErr) -> PyResult<bool> {
    error.value(py).getattr("is_permanent")?.extract()
}
