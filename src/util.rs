//! Utility functions for working with Python objects in Rust.
//!
//! This module provides helper functions to extract and convert data
//! from Python objects into Rust-compatible types using the PyO3 library.

use pyo3::types::PyAnyMethods;
use pyo3::{Bound, PyAny, PyResult};

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
