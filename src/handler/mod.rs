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
use std::sync::{Arc, Mutex};

use crate::message::MessageCore;
use chrono::{DateTime, Utc};
use futures::pin_mut;
use opentelemetry::propagation::{TextMapCompositePropagator, TextMapPropagator};
use prosody::consumer::event_context::EventContext;
use prosody::consumer::message::ConsumerMessage;
use prosody::consumer::middleware::FallibleHandler;
use prosody::consumer::{DemandType, Keyed};
use prosody::error::{ClassifyError, ErrorCategory};
use prosody::high_level::{ClientHandler, JsonCodecs};
use prosody::propagator::new_propagator;
use prosody::timers::{TimerType, Trigger};
use pyo3::exceptions::PyTypeError;
use pyo3::prelude::PyAnyMethods;
use pyo3::types::IntoPyDict;
use pyo3::{Bound, Py, PyAny, PyErr, PyResult, Python};
use pyo3_async_runtimes::{TaskLocals, into_future_with_locals};
use pythonize::{depythonize, pythonize};
use serde_json::Value;
use thiserror::Error;
use tokio::select;
use tracing::{debug, error, instrument};
use tracing_opentelemetry::OpenTelemetrySpanExt;

mod execution;

pub use execution::WrappedPythonError;
use execution::{cancel_task, execute, execute_timer, log_exception};

use crate::context::Context;

const HANDLER_METHODS: [&str; 3] = ["on_message", "on_excise", "on_timer"];

/// Python objects and dependencies needed for message execution
struct MessageExecutionContext<'a> {
    message_class: &'a Py<PyAny>,
    event_class: &'a Py<PyAny>,
    method: &'a Py<PyAny>,
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
    message_class: &'a Py<PyAny>,
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
    pub excise_method: Py<PyAny>,
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

        for method_name in HANDLER_METHODS {
            if !handler.getattr(method_name)?.is_callable() {
                return Err(PyTypeError::new_err(format!(
                    "handler.{method_name} must be callable"
                )));
            }
        }

        // Wrap handler with tracing/cancellation support
        let tracing_handler = tracing_handler_class.call1((handler,))?;
        let handle_method = tracing_handler.getattr("on_message")?;
        let excise_method = tracing_handler.getattr("on_excise")?;
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
            excise_method: excise_method.unbind(),
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

    async fn handle_record<C>(
        &self,
        context: C,
        message: ConsumerMessage<Value>,
        method: &Py<PyAny>,
        kind: &str,
    ) -> Result<Value, WrappedPythonError>
    where
        C: EventContext<Payload = Value>,
    {
        let mut carrier = HashMap::with_capacity(2);
        self.0
            .propagator
            .inject_context(&message.span().context(), &mut carrier);
        let cancel_future = context.on_cancel();
        let execution_context = MessageExecutionContext {
            message_class: &self.0.message_class,
            event_class: &self.0.event_class,
            method,
            locals: &self.0.locals,
            propagator: self.0.propagator.clone(),
            otel_get_current: &self.0.otel_get_current,
            otel_inject: &self.0.otel_inject,
        };
        let (shutdown_event, complete_future) =
            execute(context, message, carrier, execution_context)?;

        pin_mut!(complete_future);
        let output = select! {
            result = complete_future.as_mut() => {
                if let Err(error) = log_exception(&result) {
                    error!("{kind} handling failed but the error could not be logged: {error:#}");
                }
                result?
            }
            () = cancel_future => {
                debug!("cancel signal received; cancelling task");
                cancel_task(&self.0.event_set_method, shutdown_event)?;
                debug!("waiting for task to finish");
                let output = complete_future.await?;
                debug!("task cancelled");
                output
            }
        };

        Python::attach(|py| depythonize(output.bind(py)))
            .map_err(|error| WrappedPythonError::ResultConversion(error.to_string()))
    }
}

impl FallibleHandler for PythonHandler {
    type Error = WrappedPythonError;
    type Output = Value;
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
    ) -> Result<Self::Output, Self::Error>
    where
        C: EventContext<Payload = Self::Payload>,
    {
        let _ = demand_type;
        self.handle_record(context, message, &self.0.handle_method, "message")
            .await
    }

    #[instrument(level = "debug", skip(self, context, demand_type), err)]
    async fn on_excise<C>(
        &self,
        context: C,
        message: ConsumerMessage<Self::Payload>,
        demand_type: DemandType,
    ) -> Result<Self::Output, Self::Error>
    where
        C: EventContext<Payload = Self::Payload>,
    {
        let _ = demand_type;
        self.handle_record(context, message, &self.0.excise_method, "excise")
            .await
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
    ) -> Result<Self::Output, Self::Error>
    where
        C: EventContext<Payload = Self::Payload>,
    {
        let _ = demand_type; // Not used in Python handler

        // Only process application timers; internal timers are handled by middleware
        if trigger.timer_type != TimerType::Application {
            return Ok(Value::Null);
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
            message_class: &self.0.message_class,
            locals: &self.0.locals,
            propagator: self.0.propagator.clone(),
            otel_get_current: &self.0.otel_get_current,
            otel_inject: &self.0.otel_inject,
        };
        let (shutdown_event, complete_future) =
            execute_timer(context, trigger, serialized_context, timer_context)?;

        pin_mut!(complete_future);
        let output = select! {
            // Handle normal completion
            result = complete_future.as_mut() => {
                if let Err(error) = log_exception(&result) {
                    error!("timer handling failed but error could not be logged: {error:#}");
                }
                result?
            }

            // Handle cancel request
            () = cancel_future => {
                debug!("cancel signal received; cancelling timer task");
                cancel_task(&self.0.event_set_method, shutdown_event)?;

                debug!("waiting for timer task to cleanup");
                let output = complete_future.await?;

                debug!("timer task cancelled");
                output
            }
        };

        Python::attach(|py| depythonize(output.bind(py)))
            .map_err(|error| WrappedPythonError::ResultConversion(error.to_string()))
    }

    /// Shuts down the handler.
    ///
    /// This is a no-op for the Python handler since resources are managed
    /// by the Python runtime through garbage collection.
    async fn shutdown(self) {
        // No cleanup required - Python handles resource cleanup via GC
    }
}

impl ClientHandler for PythonHandler {
    type Codecs = JsonCodecs;
}
