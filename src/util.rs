//! Utility functions for working with Python objects in Rust.
//!
//! This module provides helper functions to extract and convert data
//! from Python objects into Rust-compatible types using the `PyO3` library.

use pyo3::exceptions::{PyTypeError, PyValueError};
use pyo3::types::{PyAnyMethods, PyDelta, PyDeltaAccess};
use pyo3::{Bound, PyAny, PyResult};
use std::time::Duration;

/// Extracts a vector of strings from a Python object.
///
/// # Arguments
///
/// * `value` - A Python object that is either a string or a list of strings.
///
/// # Returns
///
/// A `PyResult` containing a vector of strings.
///
/// # Errors
///
/// Returns a `PyErr` if the extraction fails.
pub fn string_or_vec(value: &Bound<PyAny>) -> PyResult<Vec<String>> {
    // Try to extract a single string first
    if let Ok(string) = value.extract::<String>() {
        return Ok(vec![string]);
    }

    // If not a single string, try to extract a list of strings
    value.extract()
}

/// Decodes a Python object into a Rust `Duration`.
///
/// # Arguments
///
/// * `value` - A Python object representing a duration (either a `timedelta` or
///   a float).
///
/// # Returns
///
/// A `PyResult` containing the decoded `Duration`.
///
/// # Errors
///
/// Returns a `PyTypeError` if the input is neither a `timedelta` nor a float.
/// Returns a `PyValueError` if the float conversion fails.
pub fn decode_duration(value: &Bound<PyAny>) -> PyResult<Duration> {
    // Try to decode as a timedelta first
    if let Ok(delta) = value.downcast::<PyDelta>() {
        let days = u64::try_from(delta.get_days())?;
        let seconds = u64::try_from(delta.get_seconds())?;
        let micros = u64::try_from(delta.get_microseconds())?;

        let mut duration = Duration::from_secs(days * 24 * 60 * 60);
        duration += Duration::from_secs(seconds);
        duration += Duration::from_micros(micros);
        return Ok(duration);
    }

    // If not a timedelta, try to decode as a float
    if let Ok(seconds) = value.extract::<f64>() {
        let duration = Duration::try_from_secs_f64(seconds)
            .map_err(|error| PyValueError::new_err(error.to_string()))?;

        return Ok(duration);
    }

    // If neither a timedelta nor a float, return an error
    Err(PyTypeError::new_err(
        "expected a timedelta or non-negative float representing seconds",
    ))
}

/// Decodes an optional Python object into an optional Rust `Duration`.
///
/// # Arguments
///
/// * `value` - An optional Python object representing a duration.
///
/// # Returns
///
/// A `PyResult` containing an `Option<Duration>`.
///
/// # Errors
///
/// Propagates errors from `decode_duration`.
pub fn decode_optional_duration(value: &Bound<PyAny>) -> PyResult<Option<Duration>> {
    Ok(if value.is_none() {
        None
    } else {
        Some(decode_duration(value)?)
    })
}
