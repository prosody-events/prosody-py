//! Defines structures for representing Kafka messages in a Python-compatible
//! format.
//!
//! This module provides the `Context` struct to hold message context
//! information for Kafka messages. The `state` module binds keyed-state
//! collections for the context.

use crate::state::StateEnv;
use chrono::{DateTime, Utc};
use parking_lot::Mutex;
use prosody::consumer::DemandType;
use prosody::consumer::event_context::BoxEventContext;
use prosody::timers::TimerType;
use pyo3::exceptions::PyRuntimeError;
use pyo3::gc::{PyTraverseError, PyVisit};
use pyo3::types::{PyAnyMethods, PyTypeMethods};
use pyo3::{Bound, Py, PyAny, PyResult, Python, pyclass, pymethods};
use pyo3_async_runtimes::tokio::future_into_py;
use serde_json::Value;
use state::StateDefinitionKind;
use std::collections::HashMap;
use tracing::{Instrument, info_span};

mod state;

/// Encapsulates context information for a Kafka message.
///
/// This struct wraps a `BoxEventContext` from the `prosody` crate,
/// making it accessible in a Python environment.
#[pyclass]
pub struct Context {
    pub inner: BoxEventContext<Value>,
    /// The Python environment that traced calls and state handles share.
    pub(crate) env: StateEnv,
    /// Whether this attempt is a normal delivery or a retry after a failure.
    pub(crate) demand: DemandType,
    /// Typed-wrapper cache keyed by descriptor type and collection name. It
    /// drains when Prosody drops the context at the end of the event.
    pub(crate) state_handles: Mutex<HashMap<(StateDefinitionKind, String), Py<PyAny>>>,
}

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
        self.env.set_parent(py, &span)?;

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
        self.env.set_parent(py, &span)?;

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
        self.env.set_parent(py, &span)?;

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
        self.env.set_parent(py, &span)?;

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
        self.env.set_parent(py, &span)?;

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

    /// Reports why this attempt runs: a normal delivery, or a retry after a
    /// failure with its retry count.
    ///
    /// # Errors
    ///
    /// Returns a `PyErr` if the `prosody.demand` import fails.
    #[getter]
    fn demand(&self, py: Python) -> PyResult<Py<PyAny>> {
        let module = py.import("prosody.demand")?;
        let kind = module.getattr("DemandKind")?;
        let kind = match self.demand {
            DemandType::Normal => kind.getattr("NORMAL")?,
            DemandType::Failure { .. } => kind.getattr("FAILURE")?,
        };
        let demand = module
            .getattr("Demand")?
            .call1((kind, self.demand.retry()))?;
        Ok(demand.unbind())
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

    /// Binds a registered keyed-state collection for this event and returns the
    /// typed Python wrapper (`ValueState`/`MapState`/`SetState`/`DequeState`).
    ///
    /// Uses the descriptor's concrete type to select the matching internal
    /// vend and Python wrapper. Repeated calls cache the wrapper by descriptor
    /// type and collection name.
    ///
    /// # Errors
    ///
    /// Returns `TransientStateError` if the definition is malformed or hostile
    /// (a caller mistake). The permanent unregistered/identity-mismatch
    /// error is raised by the internal vend.
    fn state(&self, py: Python, definition: &Bound<PyAny>) -> PyResult<Py<PyAny>> {
        state::bind(self, py, definition)
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
    #[expect(
        clippy::needless_pass_by_value,
        reason = "PyO3 fixes the __traverse__ signature"
    )]
    fn __traverse__(&self, visit: PyVisit) -> Result<(), PyTraverseError> {
        // `StateEnv` documents why the environment is not visited. Skip the
        // cache if another thread holds its lock.
        if let Some(cache) = self.state_handles.try_lock() {
            for handle in cache.values() {
                visit.call(handle.as_any())?;
            }
        }
        Ok(())
    }

    /// Drops cached state wrappers so the cyclic GC can reclaim any reference
    /// cycle through this Context.
    fn __clear__(&self) {
        self.state_handles.lock().clear();
    }
}
