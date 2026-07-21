//! Defines structures for representing Kafka messages in a Python-compatible
//! format.
//!
//! This module provides the `Context` struct to hold message context
//! information for Kafka messages.

use crate::state::{
    DequeStateVariant, MapStateVariant, NativeDequeState, NativeMapState, NativeValueState,
    StateEnv, ValueStateVariant, state_error,
};
use chrono::{DateTime, Utc};
use opentelemetry::propagation::{TextMapCompositePropagator, TextMapPropagator};
use prosody::consumer::event_context::BoxEventContext;
use prosody::timers::TimerType;
use pyo3::exceptions::PyRuntimeError;
use pyo3::gc::{PyTraverseError, PyVisit};
use pyo3::types::{PyAnyMethods, PyDict, PyDictMethods, PyModule, PyTypeMethods};
use pyo3::{Bound, Py, PyAny, PyErr, PyResult, Python, pyclass, pymethods};
use pyo3_async_runtimes::tokio::future_into_py;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tracing::{Instrument, debug, info_span};
use tracing_opentelemetry::OpenTelemetrySpanExt;

/// Encapsulates context information for a Kafka message.
///
/// This struct wraps a `BoxEventContext` from the `prosody` crate,
/// making it accessible in a Python environment.
#[pyclass]
pub struct Context {
    pub inner: BoxEventContext<Value>,
    pub get_current: Py<PyAny>,
    pub inject: Py<PyAny>,
    pub propagator: Arc<TextMapCompositePropagator>,
    pub message_class: Py<PyAny>,
    /// Identity cache of typed wrappers vended this event, keyed
    /// `"{kind}:{payload}:{name}"`, so repeated `state(def)` calls return the
    /// same wrapper object.
    pub state_handles: Mutex<HashMap<String, Py<PyAny>>>,
}

/// Builds a `TransientStateError` for a malformed or hostile state definition —
/// a caller mistake, so transient rather than a message-discarding permanent.
fn transient_state_error(prosody: &Bound<PyModule>, message: &str) -> PyErr {
    match prosody
        .getattr("TransientStateError")
        .and_then(|class| class.call1((message,)))
    {
        Ok(instance) => PyErr::from_value(instance),
        // Constructing the exception itself failed — surface that error.
        Err(error) => error,
    }
}

impl Context {
    /// Builds the shared environment threaded into every vended state handle.
    fn state_env(&self, py: Python) -> PyResult<StateEnv> {
        StateEnv::resolve(
            py,
            &self.get_current,
            &self.inject,
            Arc::clone(&self.propagator),
            &self.message_class,
        )
    }

    /// Helper method to extract tracing context and set it as parent for the
    /// given span
    fn setup_tracing_context(&self, py: Python, span: &tracing::Span) -> PyResult<()> {
        let context = self.get_current.bind(py).call0()?;
        let data = PyDict::new(py);
        self.inject.call1(py, (&data, context))?;

        let headers: HashMap<String, String> = data.extract()?;
        let otel_context = self.propagator.extract(&headers);
        if let Err(err) = span.set_parent(otel_context) {
            debug!("failed to set parent span: {err:#}");
        }

        Ok(())
    }
}

#[allow(
    clippy::multiple_inherent_impl,
    reason = "Python methods are implemented in a separate module"
)]
#[pymethods]
impl Context {
    /// Schedule a new timer at the given execution time for the current message
    /// key.
    ///
    /// # Arguments
    ///
    /// * `time` - A datetime at which the timer should fire
    ///
    /// # Returns
    ///
    /// A coroutine that completes when the timer is scheduled.
    ///
    /// # Errors
    ///
    /// Returns a `PyRuntimeError` if the timer cannot be scheduled.
    fn schedule<'p>(&self, py: Python<'p>, time: DateTime<Utc>) -> PyResult<Bound<'p, PyAny>> {
        let time = time
            .try_into()
            .map_err(|e| PyRuntimeError::new_err(format!("Invalid time: {e}")))?;

        let span = info_span!("schedule", %time);
        self.setup_tracing_context(py, &span)?;

        let context = self.inner.clone();
        future_into_py(py, async move {
            context
                .schedule(time, TimerType::Application)
                .instrument(span)
                .await
                .map_err(|e| PyRuntimeError::new_err(format!("Failed to schedule timer: {e}")))
        })
    }

    /// Unschedule ALL existing timers for the current key, then schedule
    /// exactly one new timer.
    ///
    /// # Arguments
    ///
    /// * `time` - The time for the new, sole scheduled timer
    ///
    /// # Errors
    ///
    /// Returns a `PyRuntimeError` if the operation fails.
    fn clear_and_schedule<'p>(
        &self,
        py: Python<'p>,
        time: DateTime<Utc>,
    ) -> PyResult<Bound<'p, PyAny>> {
        let time = time
            .try_into()
            .map_err(|e| PyRuntimeError::new_err(format!("Invalid time: {e}")))?;

        let span = info_span!("clear_and_schedule", %time);
        self.setup_tracing_context(py, &span)?;

        let context = self.inner.clone();
        future_into_py(py, async move {
            context
                .clear_and_schedule(time, TimerType::Application)
                .instrument(span)
                .await
                .map_err(|e| {
                    PyRuntimeError::new_err(format!("Failed to clear and schedule timer: {e}"))
                })
        })
    }

    /// Unschedule a specific timer for the current key at the specified time.
    ///
    /// # Arguments
    ///
    /// * `time` - The execution time of the timer to remove
    ///
    /// # Errors
    ///
    /// Returns a `PyRuntimeError` if the timer cannot be unscheduled.
    fn unschedule<'p>(&self, py: Python<'p>, time: DateTime<Utc>) -> PyResult<Bound<'p, PyAny>> {
        let time = time
            .try_into()
            .map_err(|e| PyRuntimeError::new_err(format!("Invalid time: {e}")))?;

        let span = info_span!("unschedule", %time);
        self.setup_tracing_context(py, &span)?;

        let context = self.inner.clone();
        future_into_py(py, async move {
            context
                .unschedule(time, TimerType::Application)
                .instrument(span)
                .await
                .map_err(|e| PyRuntimeError::new_err(format!("Failed to unschedule timer: {e}")))
        })
    }

    /// Unschedule ALL timers for the current key.
    ///
    /// # Errors
    ///
    /// Returns a `PyRuntimeError` if the operation fails.
    fn clear_scheduled<'p>(&self, py: Python<'p>) -> PyResult<Bound<'p, PyAny>> {
        let span = info_span!("clear_scheduled");
        self.setup_tracing_context(py, &span)?;

        let context = self.inner.clone();
        future_into_py(py, async move {
            context
                .clear_scheduled(TimerType::Application)
                .instrument(span)
                .await
                .map_err(|e| {
                    PyRuntimeError::new_err(format!("Failed to clear scheduled timers: {e}"))
                })
        })
    }

    /// List all scheduled execution times for timers on the current key.
    ///
    /// # Returns
    ///
    /// A list of scheduled execution times as epoch seconds.
    ///
    /// # Errors
    ///
    /// Returns a `PyRuntimeError` if the operation fails.
    fn scheduled<'p>(&self, py: Python<'p>) -> PyResult<Bound<'p, PyAny>> {
        let span = info_span!("scheduled");
        self.setup_tracing_context(py, &span)?;

        let context = self.inner.clone();
        future_into_py(py, async move {
            context
                .scheduled(TimerType::Application)
                .instrument(span)
                .await
                .map(|times| {
                    times
                        .into_iter()
                        .map(DateTime::<Utc>::from)
                        .collect::<Vec<_>>()
                })
                .map_err(|e| PyRuntimeError::new_err(format!("Failed to get scheduled times: {e}")))
        })
    }

    /// Check if cancellation has been requested.
    ///
    /// Cancellation includes both message-level cancellation (e.g., timeout)
    /// and partition shutdown.
    ///
    /// # Returns
    ///
    /// True if cancellation has been requested, False otherwise
    fn should_cancel(&self) -> bool {
        self.inner.should_cancel()
    }

    /// Waits for a cancellation signal.
    ///
    /// Cancellation includes both message-level cancellation (e.g., timeout)
    /// and partition shutdown.
    ///
    /// # Returns
    ///
    /// A coroutine that completes when cancellation is signaled.
    fn on_cancel<'p>(&self, py: Python<'p>) -> PyResult<Bound<'p, PyAny>> {
        let context = self.inner.clone();
        future_into_py(py, async move {
            context.on_cancel().await;
            Ok(())
        })
    }

    /// Returns a string representation of the `Context`.
    ///
    /// # Returns
    ///
    /// A string representation showing the context state.
    fn __repr__(slf: &Bound<Self>) -> PyResult<String> {
        let class_name = slf.get_type().qualname()?;
        let slf = slf.borrow();
        Ok(format!(
            "{}(cancelled={})",
            class_name,
            slf.inner.should_cancel()
        ))
    }

    /// Returns a human-readable string description of the `Context`.
    ///
    /// # Returns
    ///
    /// A human-readable description of the context.
    fn __str__(slf: &Bound<Self>) -> PyResult<String> {
        let class_name = slf.get_type().qualname()?;
        let slf = slf.borrow();
        let status = if slf.inner.should_cancel() {
            "cancelled"
        } else {
            "active"
        };
        Ok(format!("{class_name}: {status}"))
    }

    /// Vends the low-level handle for the named JSON value collection.
    ///
    /// Vending verifies the collection's registration (core-side); no span is
    /// opened here — vended handles outlive the call, and every operation opens
    /// its own span.
    ///
    /// # Errors
    ///
    /// Returns a permanent error if the name is unregistered or its registered
    /// identity mismatches.
    fn value_state(&self, py: Python, name: &str) -> PyResult<NativeValueState> {
        let env = self.state_env(py)?;
        let handle = self
            .inner
            .value_state(name)
            .map_err(|e| state_error(py, &env, &e))?;
        Ok(NativeValueState {
            state: Arc::new(ValueStateVariant::Json(handle)),
            env,
        })
    }

    /// Vends the low-level handle for the named JSON map collection.
    ///
    /// # Errors
    ///
    /// Returns a permanent error if the name is unregistered or its registered
    /// identity mismatches.
    fn map_state(&self, py: Python, name: &str) -> PyResult<NativeMapState> {
        let env = self.state_env(py)?;
        let handle = self
            .inner
            .map_state(name)
            .map_err(|e| state_error(py, &env, &e))?;
        Ok(NativeMapState {
            state: Arc::new(MapStateVariant::Json(handle)),
            env,
        })
    }

    /// Vends the low-level handle for the named JSON deque collection.
    ///
    /// # Errors
    ///
    /// Returns a permanent error if the name is unregistered or its registered
    /// identity mismatches.
    fn deque_state(&self, py: Python, name: &str) -> PyResult<NativeDequeState> {
        let env = self.state_env(py)?;
        let handle = self
            .inner
            .deque_state(name)
            .map_err(|e| state_error(py, &env, &e))?;
        Ok(NativeDequeState {
            state: Arc::new(DequeStateVariant::Json(handle)),
            env,
        })
    }

    /// Vends the low-level handle for the named Kafka-message value collection.
    ///
    /// Items are the full `Message` the handler received, loader-resolved on
    /// read.
    ///
    /// # Errors
    ///
    /// Returns a permanent error if the name is unregistered or its registered
    /// identity mismatches.
    fn message_value_state(&self, py: Python, name: &str) -> PyResult<NativeValueState> {
        let env = self.state_env(py)?;
        let handle = self
            .inner
            .message_value_state(name)
            .map_err(|e| state_error(py, &env, &e))?;
        Ok(NativeValueState {
            state: Arc::new(ValueStateVariant::Message(handle)),
            env,
        })
    }

    /// Vends the low-level handle for the named Kafka-message map collection.
    ///
    /// Items are the full `Message` the handler received, loader-resolved on
    /// read.
    ///
    /// # Errors
    ///
    /// Returns a permanent error if the name is unregistered or its registered
    /// identity mismatches.
    fn message_map_state(&self, py: Python, name: &str) -> PyResult<NativeMapState> {
        let env = self.state_env(py)?;
        let handle = self
            .inner
            .message_map_state(name)
            .map_err(|e| state_error(py, &env, &e))?;
        Ok(NativeMapState {
            state: Arc::new(MapStateVariant::Message(handle)),
            env,
        })
    }

    /// Vends the low-level handle for the named Kafka-message deque collection.
    ///
    /// Items are the full `Message` the handler received, loader-resolved on
    /// read.
    ///
    /// # Errors
    ///
    /// Returns a permanent error if the name is unregistered or its registered
    /// identity mismatches.
    fn message_deque_state(&self, py: Python, name: &str) -> PyResult<NativeDequeState> {
        let env = self.state_env(py)?;
        let handle = self
            .inner
            .message_deque_state(name)
            .map_err(|e| state_error(py, &env, &e))?;
        Ok(NativeDequeState {
            state: Arc::new(DequeStateVariant::Message(handle)),
            env,
        })
    }

    /// Binds a registered keyed-state collection for this event and returns the
    /// typed Python wrapper (`ValueState`/`MapState`/`DequeState`).
    ///
    /// Reads the definition's `to_config()` for name/kind/payload, dispatches
    /// to the matching internal vend, wraps the native handle in the Python
    /// class, and caches the wrapper by `(kind, payload, name)` so repeated
    /// calls in one event return the same object.
    ///
    /// # Errors
    ///
    /// Returns `TransientStateError` if the definition is malformed or hostile
    /// (a caller mistake). The permanent unregistered/identity-mismatch
    /// error is raised by the internal vend.
    fn state(&self, py: Python, definition: &Bound<PyAny>) -> PyResult<Py<PyAny>> {
        let prosody = py.import("prosody")?;

        let cfg = definition.call_method0("to_config").map_err(|_| {
            transient_state_error(
                &prosody,
                "state: definition.to_config() is missing or raised",
            )
        })?;
        let cfg = cfg.cast::<PyDict>().map_err(|_| {
            transient_state_error(&prosody, "state: to_config() must return a dict")
        })?;

        let read = |field: &str| -> PyResult<String> {
            match cfg.get_item(field)? {
                Some(value) if !value.is_none() => value.extract::<String>().map_err(|_| {
                    transient_state_error(
                        &prosody,
                        &format!("state: definition {field} must be a string"),
                    )
                }),
                _ => Err(transient_state_error(
                    &prosody,
                    &format!("state: definition {field} is missing"),
                )),
            }
        };
        let name = read("name")?;
        if name.is_empty() {
            return Err(transient_state_error(
                &prosody,
                "state: definition name must be non-empty",
            ));
        }
        let kind = read("kind")?;
        let payload = read("payload")?;

        let cache_key = format!("{kind}:{payload}:{name}");
        {
            let cache = self
                .state_handles
                .lock()
                .map_err(|_| PyRuntimeError::new_err("state cache mutex poisoned"))?;
            if let Some(existing) = cache.get(&cache_key) {
                return Ok(existing.clone_ref(py));
            }
        }

        let native: Py<PyAny> = match (kind.as_str(), payload.as_str()) {
            ("value", "json") => Py::new(py, self.value_state(py, &name)?)?.into_any(),
            ("map", "json") => Py::new(py, self.map_state(py, &name)?)?.into_any(),
            ("deque", "json") => Py::new(py, self.deque_state(py, &name)?)?.into_any(),
            ("value", "message") => Py::new(py, self.message_value_state(py, &name)?)?.into_any(),
            ("map", "message") => Py::new(py, self.message_map_state(py, &name)?)?.into_any(),
            ("deque", "message") => Py::new(py, self.message_deque_state(py, &name)?)?.into_any(),
            (other_kind, other_payload) => {
                return Err(transient_state_error(
                    &prosody,
                    &format!(
                        "state: unknown collection kind/payload {other_kind:?}/{other_payload:?}"
                    ),
                ));
            }
        };
        let wrapper_name = match kind.as_str() {
            "value" => "ValueState",
            "map" => "MapState",
            _ => "DequeState",
        };
        let wrapper = prosody.getattr(wrapper_name)?.call1((native,))?.unbind();
        {
            let mut cache = self
                .state_handles
                .lock()
                .map_err(|_| PyRuntimeError::new_err("state cache mutex poisoned"))?;
            cache.insert(cache_key, wrapper.clone_ref(py));
        };
        Ok(wrapper)
    }

    /// Traverses Python objects contained in this Context for garbage
    /// collection.
    ///
    /// # Arguments
    ///
    /// * `visit` - A `PyVisit` object used to visit Python objects.
    ///
    /// # Errors
    ///
    /// Returns `Err(PyTraverseError)` if an error occurs during the traversal.
    #[allow(clippy::needless_pass_by_value)]
    fn __traverse__(&self, visit: PyVisit) -> Result<(), PyTraverseError> {
        visit.call(self.get_current.as_any())?;
        visit.call(self.inject.as_any())?;
        visit.call(self.message_class.as_any())?;
        if let Ok(cache) = self.state_handles.lock() {
            for handle in cache.values() {
                visit.call(handle.as_any())?;
            }
        }
        Ok(())
    }

    /// Drops cached state wrappers so the cyclic GC can reclaim any reference
    /// cycle through this Context.
    fn __clear__(&self) {
        if let Ok(mut cache) = self.state_handles.lock() {
            cache.clear();
        }
    }
}
