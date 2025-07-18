//! Defines structures for representing Kafka messages in a Python-compatible
//! format.
//!
//! This module provides the `Context` struct to hold message context
//! information for Kafka messages.

use chrono::{DateTime, Utc};
use futures::TryStreamExt;
use prosody::consumer::event_context::BoxEventContext;
use pyo3::exceptions::PyRuntimeError;
use pyo3::{PyResult, pyclass, pymethods};

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
    async fn schedule(&self, time: DateTime<Utc>) -> PyResult<()> {
        let time = time
            .try_into()
            .map_err(|e| PyRuntimeError::new_err(format!("Invalid time: {e}")))?;

        self.0
            .schedule(time)
            .await
            .map_err(|e| PyRuntimeError::new_err(format!("Failed to schedule timer: {e}")))
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
    async fn clear_and_schedule(&self, time: DateTime<Utc>) -> PyResult<()> {
        let time = time
            .try_into()
            .map_err(|e| PyRuntimeError::new_err(format!("Invalid time: {e}")))?;

        self.0.clear_and_schedule(time).await.map_err(|e| {
            PyRuntimeError::new_err(format!("Failed to clear and schedule timer: {e}"))
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
    async fn unschedule(&self, time: DateTime<Utc>) -> PyResult<()> {
        let time = time
            .try_into()
            .map_err(|e| PyRuntimeError::new_err(format!("Invalid time: {e}")))?;

        self.0
            .unschedule(time)
            .await
            .map_err(|e| PyRuntimeError::new_err(format!("Failed to unschedule timer: {e}")))
    }

    /// Unschedule ALL timers for the current key.
    ///
    /// # Errors
    ///
    /// Returns a `PyRuntimeError` if the operation fails.
    async fn clear_scheduled(&self) -> PyResult<()> {
        self.0
            .clear_scheduled()
            .await
            .map_err(|e| PyRuntimeError::new_err(format!("Failed to clear scheduled timers: {e}")))
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
    async fn scheduled(&self) -> PyResult<Vec<DateTime<Utc>>> {
        self.0
            .scheduled()
            .map_ok(DateTime::<Utc>::from)
            .try_collect()
            .await
            .map_err(|e| PyRuntimeError::new_err(format!("Failed to get scheduled times: {e}")))
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
