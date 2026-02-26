//! A Rust library that provides Python bindings for Kafka message handling.
//!
//! This library integrates Rust's Prosody library with Python, offering a
//! high-performance Kafka client that can be used from Python code. It includes
//! modules for client operations, message handling, and Kafka message
//! representation.

#![allow(clippy::multiple_crate_versions)]
#![warn(missing_docs)]

use crate::admin::AdminClient;
use crate::client::ProsodyClient;
use crate::context::Context;
use crate::logging::PythonLoggingLayer;
use ::prosody::tracing::initialize_tracing;
use pyo3::exceptions::PyRuntimeError;
use pyo3::types::{PyAnyMethods, PyModule, PyModuleMethods};
use pyo3::{Bound, PyResult, Python, pymodule};
#[cfg(not(target_env = "msvc"))]
use tikv_jemallocator::Jemalloc;

mod admin;
mod client;
mod context;
mod handler;
mod logging;
mod util;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

/// Initializes the Python module and adds the necessary classes.
///
/// This function is called by `PyO3` when the module is imported in Python.
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
    // Initialize tracing with our non-blocking Python logging layer.
    // This layer queues log events and forwards them to Python's logging
    // system on a dedicated background thread, avoiding GIL deadlocks.
    let python_logging = PythonLoggingLayer::new(py)?;
    initialize_tracing(Some(python_logging))
        .map_err(|error| PyRuntimeError::new_err(error.to_string()))?;

    // Add classes to the module
    prosody_module.add_class::<ProsodyClient>()?;

    let context_module = PyModule::new(py, "context")?;
    prosody_module.add_submodule(&context_module)?;
    context_module.add_class::<Context>()?;

    py.import("sys")?
        .getattr("modules")?
        .set_item("prosody.context", context_module)?;

    prosody_module.add_class::<AdminClient>()?;

    Ok(())
}
