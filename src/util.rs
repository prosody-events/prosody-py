//! Utility functions for working with Python objects in Rust.
//!
//! This module provides helper functions to extract and convert data
//! from Python objects into Rust-compatible types using the `PyO3` library.

use prosody::high_level::erased::ErasedReadCache;
use pyo3::exceptions::{PyRuntimeError, PyTypeError, PyValueError};
use pyo3::types::{PyAnyMethods, PyBool, PyDelta, PyDict, PyDictMethods};
use pyo3::{Bound, PyAny, PyResult};
use std::process;
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
    if value.is_instance_of::<PyDelta>() {
        return value.extract();
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

/// Parses a read-cache option. `None` inherits the default, `False` bypasses
/// the cache, and a duration sets the cache window.
///
/// # Errors
///
/// Returns a `PyValueError` that names `field` for `True` or for a value that
/// is not a duration.
pub fn parse_read_cache(field: &str, value: Option<&Bound<PyAny>>) -> PyResult<ErasedReadCache> {
    let Some(value) = value else {
        return Ok(ErasedReadCache::Inherit);
    };
    if value.is_instance_of::<PyBool>() {
        if value.is_truthy()? {
            return Err(PyValueError::new_err(format!(
                "{field}: True is ambiguous; use a duration or False"
            )));
        }
        return Ok(ErasedReadCache::Disabled);
    }
    decode_duration(value)
        .map(ErasedReadCache::Ttl)
        .map_err(|error| PyValueError::new_err(format!("{field}: {}", error.value(value.py()))))
}

/// Reads the option `key` from a keyword-argument dict.
///
/// A missing key and an explicit `None` both read as "not set", so the core
/// default and its environment variable apply.
///
/// # Errors
///
/// Returns a `PyErr` if the dict lookup fails.
pub fn option<'py>(config: &Bound<'py, PyDict>, key: &str) -> PyResult<Option<Bound<'py, PyAny>>> {
    Ok(config.get_item(key)?.filter(|value| !value.is_none()))
}

/// Rejects a call on a client that the current process did not create.
///
/// A forked child inherits the client's memory but not its threads, so the
/// client cannot work there. `pid` is the process that created the client.
///
/// # Errors
///
/// Returns a `PyRuntimeError` that names `client` when the process differs.
pub fn check_fork(pid: u32, client: &str) -> PyResult<()> {
    if process::id() != pid {
        return Err(PyRuntimeError::new_err(format!(
            "{client} cannot be used after fork. Create a new client in the child process."
        )));
    }
    Ok(())
}
