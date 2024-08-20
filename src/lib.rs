//! A Rust library that provides Python bindings for Kafka message handling.
//!
//! This library integrates Rust's Prosody library with Python, offering a
//! high-performance Kafka client that can be used from Python code. It includes
//! modules for client operations, message handling, and Kafka message
//! representation.

#![allow(clippy::multiple_crate_versions)]
#![warn(missing_docs)]

use ::prosody::tracing::{initialize_tracing, Identity};
use once_cell::sync::Lazy;
use pyo3::exceptions::PyRuntimeError;
use pyo3::types::PyModule;
use pyo3::{pymodule, Bound, PyResult};
use tokio::runtime::Runtime;

use crate::client::ProsodyClient;
use crate::context::Context;

mod client;
mod context;
mod handler;

/// A global Tokio runtime for asynchronous operations.
///
/// # Panics
///
/// Panics if the Tokio runtime cannot be created.
#[allow(clippy::expect_used)]
static RUNTIME: Lazy<Runtime> =
    Lazy::new(|| Runtime::new().expect("Failed to create Tokio runtime"));

/// Initializes the Python module and adds the necessary classes.
///
/// This function is called by PyO3 when the module is imported in Python.
/// It initializes logging and adds the `ProsodyClient`, `Context`, and
/// `Message` classes to the module.
///
/// # Arguments
///
/// * `m` - A mutable reference to the Python module being initialized.
///
/// # Returns
///
/// Returns `Ok(())` if the initialization is successful, or a `PyErr` if an
/// error occurs.
#[pymodule]
fn prosody(m: &Bound<PyModule>) -> PyResult<()> {
    let _enter = RUNTIME.enter();
    initialize_tracing::<Identity>(None)
        .map_err(|error| PyRuntimeError::new_err(error.to_string()))?;

    // Initialize logging for the module
    pyo3_log::init();

    // Add classes to the module
    m.add_class::<ProsodyClient>()?;
    m.add_class::<Context>()?;

    Ok(())
}
