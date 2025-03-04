//! A Rust library that provides Python bindings for Kafka message handling.
//!
//! This library integrates Rust's Prosody library with Python, offering a
//! high-performance Kafka client that can be used from Python code. It includes
//! modules for client operations, message handling, and Kafka message
//! representation.

#![allow(clippy::multiple_crate_versions)]
#![warn(missing_docs)]

use crate::client::ProsodyClient;
use crate::context::Context;
use ::prosody::tracing::{Identity, initialize_tracing};
use once_cell::sync::Lazy;
use pyo3::exceptions::PyRuntimeError;
use pyo3::types::{PyAnyMethods, PyModule, PyModuleMethods};
use pyo3::{Bound, PyResult, Python, pymodule};
use tokio::runtime::Runtime;

#[cfg(feature = "admin-client")]
use crate::admin::AdminClient;

#[cfg(feature = "admin-client")]
mod admin;

mod client;
mod context;
mod handler;
mod util;

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
fn prosody(py: Python, prosody_module: &Bound<PyModule>) -> PyResult<()> {
    let _enter = RUNTIME.enter();
    initialize_tracing::<Identity>(None)
        .map_err(|error| PyRuntimeError::new_err(error.to_string()))?;

    // Initialize logging for the module
    pyo3_log::init();

    // Add classes to the module
    prosody_module.add_class::<ProsodyClient>()?;

    let context_module = PyModule::new(py, "context")?;
    prosody_module.add_submodule(&context_module)?;
    context_module.add_class::<Context>()?;

    py.import("sys")?
        .getattr("modules")?
        .set_item("prosody.context", context_module)?;

    #[cfg(feature = "admin-client")]
    prosody_module.add_class::<AdminClient>()?;

    Ok(())
}
