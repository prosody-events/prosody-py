//! A Rust library that provides Python bindings for Kafka message handling.
//!
//! This library integrates Rust's Prosody library with Python, offering a
//! high-performance Kafka client that can be used from Python code. It includes
//! modules for client operations, message handling, and Kafka message
//! representation.

#![allow(clippy::multiple_crate_versions)]
#![warn(missing_docs)]
#![recursion_limit = "256"]

use crate::admin::AdminClient;
use crate::client::ProsodyClient;
use crate::context::Context;
use crate::logging::PythonLoggingLayer;
use crate::state::{NativeDequeState, NativeMapState, NativeStateScan, NativeValueState};
use ::prosody::tracing::{
    flush_telemetry as core_flush_telemetry, initialize_tracing,
    shutdown_telemetry as core_shutdown_telemetry,
};
use mimalloc::MiMalloc;
use pyo3::exceptions::PyRuntimeError;
use pyo3::types::{PyAnyMethods, PyModule, PyModuleMethods};
use pyo3::{Bound, PyResult, Python, pyfunction, pymodule, wrap_pyfunction};

mod admin;
mod client;
mod context;
mod handler;
mod logging;
mod state;
mod util;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

/// Initializes the Python module and adds the necessary classes.
///
/// This function is called by `PyO3` when the module is imported in Python.
/// It initializes logging and adds the client, admin, context, and internal
/// keyed-state handle classes to the module.
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

    // Internal erased keyed-state handles (the typed Python surface wraps
    // these); registered on the main module rather than a `prosody.state`
    // submodule to leave that name free for the typed layer.
    prosody_module.add_class::<NativeValueState>()?;
    prosody_module.add_class::<NativeMapState>()?;
    prosody_module.add_class::<NativeDequeState>()?;
    prosody_module.add_class::<NativeStateScan>()?;

    prosody_module.add_function(wrap_pyfunction!(flush_telemetry, prosody_module)?)?;
    let shutdown = wrap_pyfunction!(shutdown_telemetry, prosody_module)?;
    prosody_module.add_function(shutdown.clone())?;

    // Auto-flush and shut down telemetry at interpreter exit so short-lived
    // scripts don't lose the tail of their trace/metric data to the batch
    // exporters' export interval.
    py.import("atexit")?.call_method1("register", (shutdown,))?;

    Ok(())
}

/// Exports all buffered spans and metrics without tearing the telemetry
/// pipeline down.
///
/// Call this for a mid-run flush (e.g. one of several clients shutting
/// down while others keep running); use [`shutdown_telemetry`] once at
/// process exit instead. A safe no-op if tracing was never initialized.
/// Blocks until the export completes.
#[pyfunction]
fn flush_telemetry() -> PyResult<()> {
    core_flush_telemetry()
        .map_err(|error| PyRuntimeError::new_err(format!("Failed to flush telemetry: {error}")))
}

/// Flushes all buffered telemetry and shuts the export pipeline down.
///
/// Automatically registered with `atexit` so it runs once at interpreter
/// shutdown; call it manually only if the process needs to shut telemetry
/// down earlier. A safe no-op if tracing was never initialized. Blocks
/// until the final export completes.
#[pyfunction]
fn shutdown_telemetry() -> PyResult<()> {
    core_shutdown_telemetry()
        .map_err(|error| PyRuntimeError::new_err(format!("Failed to shut down telemetry: {error}")))
}
