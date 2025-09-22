//! Provides a Python interface for Prosody's admin functionality.

use crate::admin::config::{build_topic_config, try_build_admin_config};
use prosody::admin::ProsodyAdminClient;
use pyo3::exceptions::PyRuntimeError;
use pyo3::types::{PyAnyMethods, PyDict, PyTypeMethods};
use pyo3::{Bound, PyAny, PyResult, Python, pyclass, pymethods};
use pyo3_async_runtimes::tokio::future_into_py;
use std::sync::Arc;

mod config;

/// A client for performing administrative operations on Kafka topics.
///
/// This class provides methods for creating and deleting Kafka topics with
/// configurable parameters and settings.
#[pyclass]
pub struct AdminClient {
    client: Arc<ProsodyAdminClient>,
}

#[pymethods]
impl AdminClient {
    /// Creates a new `AdminClient` instance.
    ///
    /// # Arguments
    ///
    /// * `config` - Configuration options as keyword arguments:
    ///   - `bootstrap_servers` - Kafka servers for connection (string or list of strings)
    ///                          Can also be set via `PROSODY_BOOTSTRAP_SERVERS` environment variable
    ///
    /// # Errors
    ///
    /// Returns a `PyRuntimeError` if the client creation fails.
    #[new]
    #[pyo3(signature = (**config))]
    fn new(py: Python, config: Option<&Bound<PyDict>>) -> PyResult<Self> {
        try_build_admin_config(py, config)
    }

    /// Creates a new Kafka topic.
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the topic to create.
    /// * `config` - Configuration options as keyword arguments:
    ///   - `partition_count` - Number of partitions (u16, optional, uses broker default)
    ///                        Can also be set via `PROSODY_TOPIC_PARTITIONS` environment variable
    ///   - `replication_factor` - Replication factor (u16, optional, uses broker default)
    ///                           Can also be set via `PROSODY_TOPIC_REPLICATION_FACTOR` environment variable
    ///   - `cleanup_policy` - Cleanup policy ("delete", "compact", etc., optional)
    ///                       Can also be set via `PROSODY_TOPIC_CLEANUP_POLICY` environment variable
    ///   - `retention` - Message retention time (Duration/timedelta/float seconds, optional)
    ///                  Can also be set via `PROSODY_TOPIC_RETENTION` environment variable
    ///
    /// # Errors
    ///
    /// Returns a `PyRuntimeError` if the topic creation fails.
    #[pyo3(signature = (name, **config))]
    fn create_topic<'p>(
        &self,
        py: Python<'p>,
        name: String,
        config: Option<&Bound<PyDict>>,
    ) -> PyResult<Bound<'p, PyAny>> {
        let client = self.client.clone();
        let topic_config = build_topic_config(config, name)?;

        future_into_py(py, async move {
            client
                .create_topic(&topic_config)
                .await
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))
        })
    }

    /// Deletes an existing Kafka topic.
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the topic to delete.
    ///
    /// # Errors
    ///
    /// Returns a `PyRuntimeError` if the topic deletion fails.
    fn delete_topic<'p>(&self, py: Python<'p>, name: String) -> PyResult<Bound<'p, PyAny>> {
        let client = self.client.clone();
        future_into_py(py, async move {
            client
                .delete_topic(&name)
                .await
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))
        })
    }

    /// Returns a string representation of the `AdminClient`.
    ///
    /// # Returns
    ///
    /// A string representation of the `AdminClient`.
    fn __repr__(slf: &Bound<Self>) -> PyResult<String> {
        let class_name = slf.get_type().qualname()?;
        Ok(format!("{class_name}()"))
    }

    /// Returns a human-readable string description of the `AdminClient`.
    ///
    /// # Returns
    ///
    /// A human-readable description of the `AdminClient`.
    fn __str__(slf: &Bound<Self>) -> PyResult<String> {
        let class_name = slf.get_type().qualname()?;
        Ok(format!("{class_name}: Kafka administration client"))
    }
}
