//! Provides a Python interface for Prosody's admin functionality.

use crate::util::string_or_vec;
use prosody::admin::ProsodyAdminClient;
use pyo3::exceptions::PyRuntimeError;
use pyo3::{pyclass, pymethods, Bound, PyAny, PyResult};

/// Wrapper for `ProsodyAdminClient` to provide a Python interface for Prosody's
/// admin functionality.
#[pyclass]
pub struct AdminClient {
    client: ProsodyAdminClient,
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

        Ok(AdminClient { client })
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
    async fn create_topic(
        &self,
        name: String,
        partition_count: u16,
        replication_factor: u16,
    ) -> PyResult<()> {
        self.client
            .create_topic(&name, partition_count, replication_factor)
            .await
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))
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
    async fn delete_topic(&self, name: String) -> PyResult<()> {
        self.client
            .delete_topic(&name)
            .await
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))
    }
}
