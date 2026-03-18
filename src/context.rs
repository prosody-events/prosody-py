//! Defines structures for representing Kafka messages in a Python-compatible
//! format.
//!
//! This module provides the `Context` struct to hold message context
//! information for Kafka messages.

use chrono::{DateTime, Utc};
use opentelemetry::propagation::{TextMapCompositePropagator, TextMapPropagator};
use prosody::consumer::event_context::BoxEventContext;
use prosody::timers::TimerType;
use pyo3::exceptions::PyRuntimeError;
use pyo3::gc::{PyTraverseError, PyVisit};
use pyo3::types::{PyAnyMethods, PyDict, PyTypeMethods};
use pyo3::{Bound, Py, PyAny, PyResult, Python, pyclass, pymethods};
use pyo3_async_runtimes::tokio::future_into_py;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{Instrument, debug, info_span};
use tracing_opentelemetry::OpenTelemetrySpanExt;

/// Encapsulates context information for a Kafka message.
///
/// This struct wraps a `BoxEventContext` from the `prosody` crate,
/// making it accessible in a Python environment.
#[pyclass]
pub struct Context {
    pub inner: BoxEventContext,
    pub get_current: Py<PyAny>,
    pub inject: Py<PyAny>,
    pub propagator: Arc<TextMapCompositePropagator>,
}

impl Context {
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
        Ok(())
    }
}
