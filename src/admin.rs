//! Provides a Python interface for Prosody's admin functionality.

use crate::util::string_or_vec;
use prosody::admin::ProsodyAdminClient;
use pyo3::exceptions::PyRuntimeError;
use pyo3::types::{PyAnyMethods, PyTypeMethods};
use pyo3::{Bound, PyAny, PyResult, Python, pyclass, pymethods};
use pyo3_async_runtimes::tokio::future_into_py;
use std::sync::Arc;

/// Wrapper for `ProsodyAdminClient` to provide a Python interface for Prosody's
/// admin functionality.
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
    /// * `bootstrap_servers` - A string or list of strings representing Kafka
    ///   bootstrap servers.
    ///
    /// # Errors
    ///
    /// Returns a `PyRuntimeError` if the client creation fails.
    #[new]
    fn new(bootstrap_servers: &Bound<PyAny>) -> PyResult<Self> {
        let bootstrap_servers = string_or_vec(bootstrap_servers)?;
        let client = ProsodyAdminClient::new(&bootstrap_servers)
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

        Ok(AdminClient {
            client: Arc::new(client),
        })
    }

    /// Creates a new Kafka topic.
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the topic to create.
    /// * `partition_count` - The number of partitions for the new topic.
    /// * `replication_factor` - The replication factor for the new topic.
    ///
    /// # Errors
    ///
    /// Returns a `PyRuntimeError` if the topic creation fails.
    fn create_topic<'p>(
        &self,
        py: Python<'p>,
        name: String,
        partition_count: u16,
        replication_factor: u16,
    ) -> PyResult<Bound<'p, PyAny>> {
        let client = self.client.clone();
        future_into_py(py, async move {
            client
                .create_topic(&name, partition_count, replication_factor)
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
