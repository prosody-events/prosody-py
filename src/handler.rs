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

use chrono::{DateTime, Utc};
use futures::pin_mut;
use opentelemetry::propagation::{TextMapCompositePropagator, TextMapPropagator};
use prosody::consumer::event_context::EventContext;
use prosody::consumer::message::ConsumerMessage;
use prosody::consumer::middleware::FallibleHandler;
use prosody::consumer::{DemandType, Keyed};
use prosody::error::{ClassifyError, ErrorCategory};
use prosody::propagator::new_propagator;
use prosody::timers::{TimerType, Trigger};
use pyo3::exceptions::PyTypeError;
use pyo3::prelude::PyAnyMethods;
use pyo3::types::IntoPyDict;
use pyo3::{Bound, Py, PyAny, PyErr, PyResult, Python};
use pyo3_async_runtimes::{TaskLocals, into_future_with_locals};
use pythonize::pythonize;
use thiserror::Error;
use tokio::select;
use tracing::{debug, error, instrument};
use tracing_opentelemetry::OpenTelemetrySpanExt;

use crate::context::Context;

/// Python objects and dependencies needed for message execution
struct MessageExecutionContext<'a> {
    message_class: &'a Py<PyAny>,
    event_class: &'a Py<PyAny>,
    handle_method: &'a Py<PyAny>,
    locals: &'a TaskLocals,
    propagator: Arc<TextMapCompositePropagator>,
    otel_get_current: &'a Py<PyAny>,
    otel_inject: &'a Py<PyAny>,
}

/// Python objects and dependencies needed for timer execution
struct TimerExecutionContext<'a> {
    timer_class: &'a Py<PyAny>,
    event_class: &'a Py<PyAny>,
    timer_method: &'a Py<PyAny>,
    locals: &'a TaskLocals,
    propagator: Arc<TextMapCompositePropagator>,
    otel_get_current: &'a Py<PyAny>,
    otel_inject: &'a Py<PyAny>,
}

/// Base Python class name for message handlers
const HANDLER_CLASS_NAME: &str = "EventHandler";

/// Python wrapper class name for tracing/cancellation
const HANDLER_WRAPPER_CLASS_NAME: &str = "ProsodyHandler";

/// Python class name for Kafka messages
const MESSAGE_CLASS_NAME: &str = "Message";

/// Python class name for timer events
const TIMER_CLASS_NAME: &str = "Timer";

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
    pub handle_method: Py<PyAny>,
    pub timer_method: Py<PyAny>,
    pub message_class: Py<PyAny>,
    pub timer_class: Py<PyAny>,
    pub event_class: Py<PyAny>,
    pub event_set_method: Py<PyAny>,
    locals: TaskLocals,
    propagator: Arc<TextMapCompositePropagator>,
    otel_get_current: Py<PyAny>,
    otel_inject: Py<PyAny>,
}

impl PythonHandler {
    /// Creates a new `PythonHandler` from a Python object.
    ///
    /// # Arguments
    ///
    /// * `handler` - A Python object subclassing `EventHandler`.
    ///
    /// # Returns
    ///
    /// A `PyResult` containing the new `PythonHandler` if successful.
    ///
    /// # Errors
    ///
    /// Returns a `PyTypeError` if `handler` is not a subclass of
    /// `EventHandler`.
    pub fn new(handler: &Bound<PyAny>) -> PyResult<Self> {
        let py = handler.py();
        let prosody_module = py.import("prosody")?;
        let abstract_handler_class = prosody_module.getattr(HANDLER_CLASS_NAME)?;
        let tracing_handler_class = prosody_module.getattr(HANDLER_WRAPPER_CLASS_NAME)?;
        let message_class = prosody_module.getattr(MESSAGE_CLASS_NAME)?;
        let timer_class = prosody_module.getattr(TIMER_CLASS_NAME)?;

        // Verify handler inherits from EventHandler
        if !handler.is_instance(&abstract_handler_class)? {
            return Err(PyTypeError::new_err(format!(
                "handler must be a subclass of {HANDLER_CLASS_NAME}"
            )));
        }

        // Wrap handler with tracing/cancellation support
        let tracing_handler = tracing_handler_class.call1((handler,))?;
        let handle_method = tracing_handler.getattr("on_message")?;
        let timer_method = tracing_handler.getattr("on_timer")?;

        // Get a reference to the event methods
        let tsasync = py.import("tsasync")?;
        let event_class = tsasync.getattr("Event")?;
        let event_set_method = event_class.getattr("set")?;

        // Capture the running event loop
        let locals = TaskLocals::with_running_loop(py)?.copy_context(py)?;

        // Cache OpenTelemetry functions to avoid importing them on every message
        let otel_get_current = py
            .import("opentelemetry.context")?
            .getattr("get_current")?
            .unbind();

        let otel_inject = py
            .import("opentelemetry.propagate")?
            .getattr("inject")?
            .unbind();

        Ok(Self(Arc::new(PythonHandlerImpl {
            handle_method: handle_method.unbind(),
            timer_method: timer_method.unbind(),
            message_class: message_class.unbind(),
            timer_class: timer_class.unbind(),
            event_class: event_class.unbind(),
            event_set_method: event_set_method.unbind(),
            locals,
            propagator: Arc::new(new_propagator()),
            otel_get_current,
            otel_inject,
        })))
    }

    /// Gets the Python message handler method
    pub fn handle_method(&self) -> &Py<PyAny> {
        &self.0.handle_method
    }

    /// Gets the Python Message class
    pub fn message_class(&self) -> &Py<PyAny> {
        &self.0.message_class
    }

    /// Gets the Python Event class
    pub fn event_class(&self) -> &Py<PyAny> {
        &self.0.event_class
    }

    /// Gets the Python Event.set method
    pub fn event_set_method(&self) -> &Py<PyAny> {
        &self.0.event_set_method
    }

    /// Gets the Python timer handler method
    pub fn timer_method(&self) -> &Py<PyAny> {
        &self.0.timer_method
    }

    /// Gets the Python Timer class
    pub fn timer_class(&self) -> &Py<PyAny> {
        &self.0.timer_class
    }
}

impl FallibleHandler for PythonHandler {
    type Error = WrappedPythonError;
    type Output = ();
    type Payload = serde_json::Value;

    /// Processes a Kafka message by invoking the Python handler.
    ///
    /// # Arguments
    ///
    /// * `context` - Message processing context
    /// * `message` - Kafka message to process
    /// * `_demand_type` - Whether this is normal processing or failure retry
    ///
    /// # Errors
    ///
    /// Returns `WrappedPythonError` on Python exceptions or task cancellation
    /// failures
    #[instrument(level = "debug", skip(self, context, demand_type), err)]
    async fn on_message<C>(
        &self,
        context: C,
        message: ConsumerMessage<Self::Payload>,
        demand_type: DemandType,
    ) -> Result<(), Self::Error>
    where
        C: EventContext,
    {
        let _ = demand_type; // Not used in Python handler
        // Propagate tracing context to Python
        let mut serialized_context: HashMap<String, String> = HashMap::with_capacity(2);
        self.0
            .propagator
            .inject_context(&message.span().context(), &mut serialized_context);

        let cancel_future = context.on_cancel();
        let execution_context = MessageExecutionContext {
            message_class: &self.0.message_class,
            event_class: &self.0.event_class,
            handle_method: &self.0.handle_method,
            locals: &self.0.locals,
            propagator: self.0.propagator.clone(),
            otel_get_current: &self.0.otel_get_current,
            otel_inject: &self.0.otel_inject,
        };
        let (shutdown_event, complete_future) =
            execute(context, message, serialized_context, execution_context)?;

        pin_mut!(complete_future);
        select! {
            // Handle normal completion
            result = complete_future.as_mut() => {
                if let Err(error) = log_exception(&result) {
                    error!("message handling failed but error could not be logged: {error:#}");
                }
                result?;
            }

            // Handle cancel request
            () = cancel_future => {
                debug!("cancel signal received; cancelling task");
                cancel_task(&self.0.event_set_method, shutdown_event)?;

                debug!("waiting for task to cleanup");
                complete_future.await?;

                debug!("task cancelled");
            }
        }

        Ok(())
    }

    /// Processes a timer event by invoking the Python handler.
    ///
    /// # Arguments
    ///
    /// * `context` - Timer processing context
    /// * `trigger` - Timer trigger to process
    /// * `_demand_type` - Whether this is normal processing or failure retry
    ///
    /// # Errors
    ///
    /// Returns `WrappedPythonError` on Python exceptions or task cancellation
    /// failures
    #[instrument(level = "debug", skip(self, context, demand_type), err)]
    async fn on_timer<C>(
        &self,
        context: C,
        trigger: Trigger,
        demand_type: DemandType,
    ) -> Result<(), Self::Error>
    where
        C: EventContext,
    {
        let _ = demand_type; // Not used in Python handler

        // Only process application timers; internal timers are handled by middleware
        if trigger.timer_type != TimerType::Application {
            return Ok(());
        }
        // Propagate tracing context to Python
        let mut serialized_context: HashMap<String, String> = HashMap::with_capacity(2);
        self.0
            .propagator
            .inject_context(&trigger.span().context(), &mut serialized_context);

        let cancel_future = context.on_cancel();
        let timer_context = TimerExecutionContext {
            timer_class: &self.0.timer_class,
            event_class: &self.0.event_class,
            timer_method: &self.0.timer_method,
            locals: &self.0.locals,
            propagator: self.0.propagator.clone(),
            otel_get_current: &self.0.otel_get_current,
            otel_inject: &self.0.otel_inject,
        };
        let (shutdown_event, complete_future) =
            execute_timer(context, trigger, serialized_context, timer_context)?;

        pin_mut!(complete_future);
        select! {
            // Handle normal completion
            result = complete_future.as_mut() => {
                if let Err(error) = log_exception(&result) {
                    error!("timer handling failed but error could not be logged: {error:#}");
                }
                result?;
            }

            // Handle cancel request
            () = cancel_future => {
                debug!("cancel signal received; cancelling timer task");
                cancel_task(&self.0.event_set_method, shutdown_event)?;

                debug!("waiting for timer task to cleanup");
                complete_future.await?;

                debug!("timer task cancelled");
            }
        }

        Ok(())
    }

    /// Shuts down the handler.
    ///
    /// This is a no-op for the Python handler since resources are managed
    /// by the Python runtime through garbage collection.
    async fn shutdown(self) {
        // No cleanup required - Python handles resource cleanup via GC
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
fn log_exception(result: &PyResult<Py<PyAny>>) -> PyResult<()> {
    let Err(error) = result else {
        return Ok(());
    };

    Python::attach(|py| {
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
fn cancel_task(event_set_method: &Py<PyAny>, shutdown_event: Py<PyAny>) -> PyResult<()> {
    Python::attach(|py| {
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
fn execute<C>(
    context: C,
    message: ConsumerMessage<serde_json::Value>,
    serialized_context: HashMap<String, String>,
    execution_context: MessageExecutionContext<'_>,
) -> PyResult<(
    Py<PyAny>,
    impl Future<Output = PyResult<Py<PyAny>>> + Send + Sized,
)>
where
    C: EventContext,
{
    Python::attach(move |py| {
        // Create Python message objects using cached OpenTelemetry functions
        let message_context = Context {
            inner: context.boxed(),
            get_current: execution_context.otel_get_current.clone_ref(py),
            inject: execution_context.otel_inject.clone_ref(py),
            propagator: execution_context.propagator,
        };
        let payload = pythonize(py, message.payload())?;

        let message = execution_context.message_class.call1(
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
        let shutdown_event = execution_context.event_class.call0(py)?;

        // Create and convert handler coroutine to future
        let coroutine = execution_context
            .handle_method
            .call1(
                py,
                (message_context, message, otel_context, &shutdown_event),
            )?
            .into_bound(py);

        let complete_future = into_future_with_locals(execution_context.locals, coroutine)?;
        Ok((shutdown_event, complete_future))
    })
}

/// Executes a timer event by calling the Python handler
///
/// # Arguments
///
/// * `context` - Timer processing context
/// * `trigger` - Timer trigger to process
/// * `serialized_context` - OpenTelemetry context serialized as a `HashMap`
/// * `timer_class` - Python Timer class
/// * `event_class` - Python Event class for cancellation
/// * `timer_method` - Python timer handler method
/// * `locals` - Task locals for asyncio integration
///
/// # Returns
///
/// A tuple containing the shutdown event and the completion future
fn execute_timer<C>(
    context: C,
    trigger: Trigger,
    serialized_context: HashMap<String, String>,
    timer_context: TimerExecutionContext<'_>,
) -> PyResult<(
    Py<PyAny>,
    impl Future<Output = PyResult<Py<PyAny>>> + Send + Sized,
)>
where
    C: EventContext,
{
    Python::attach(move |py| {
        // Create Python timer object using cached OpenTelemetry functions
        let context_obj = Context {
            inner: context.boxed(),
            get_current: timer_context.otel_get_current.clone_ref(py),
            inject: timer_context.otel_inject.clone_ref(py),
            propagator: timer_context.propagator,
        };

        let timer = timer_context.timer_class.call1(
            py,
            (trigger.key.as_ref(), {
                let datetime_utc: DateTime<Utc> = trigger.time.into();
                datetime_utc
            }),
        )?;

        // Convert serialized_context to a Python dict
        let otel_context = serialized_context.into_py_dict(py)?;

        // Create asyncio.Event for shutdown signaling
        let shutdown_event = timer_context.event_class.call0(py)?;

        // Create and convert handler coroutine to future
        let coroutine = timer_context
            .timer_method
            .call1(py, (context_obj, timer, otel_context, &shutdown_event))?
            .into_bound(py);

        let complete_future = into_future_with_locals(timer_context.locals, coroutine)?;
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
                Python::attach(|py| match is_permanent_error(py, error) {
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
