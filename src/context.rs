//! Defines structures for representing Kafka messages in a Python-compatible
//! format.
//!
//! This module provides the `Context` struct to hold message context
//! information for Kafka messages.

use chrono::{DateTime, Utc};
use futures::TryStreamExt;
use prosody::consumer::event_context::BoxEventContext;
use prosody::timers::datetime::CompactDateTime;
use pyo3::exceptions::PyRuntimeError;
use pyo3::{Bound, PyAny, PyResult, Python, pyclass, pymethods};
use pyo3_async_runtimes::tokio::future_into_py;

/// Encapsulates context information for a Kafka message.
///
/// This struct wraps a `BoxEventContext` from the `prosody` crate,
/// making it accessible in a Python environment.
#[pyclass]
pub struct Context(pub BoxEventContext);

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

        let context = self.0.clone();
        future_into_py(py, async move {
            context
                .schedule(time)
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

        let context = self.0.clone();
        future_into_py(py, async move {
            context.clear_and_schedule(time).await.map_err(|e| {
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

        let context = self.0.clone();
        future_into_py(py, async move {
            context
                .unschedule(time)
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
        let context = self.0.clone();
        future_into_py(py, async move {
            context.clear_scheduled().await.map_err(|e| {
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
        let context = self.0.clone();
        future_into_py(py, async move {
            context
                .scheduled()
                .map_ok(<DateTime<Utc> as From<CompactDateTime>>::from)
                .try_collect::<Vec<_>>()
                .await
                .map_err(|e| PyRuntimeError::new_err(format!("Failed to get scheduled times: {e}")))
        })
    }

    /// Check if shutdown has been requested.
    ///
    /// # Returns
    ///
    /// True if shutdown has been requested, False otherwise
    fn should_shutdown(&self) -> bool {
        self.0.should_shutdown()
    }
}
