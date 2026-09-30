//! Python client for Kafka production and consumption.

use prosody::high_level::erased::ErasedConsumerState;
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::types::PyDict;
use pyo3::{Bound, Py, PyAny, PyResult, PyTraverseError, PyVisit, Python, pyclass, pymethods};
use pyo3_async_runtimes::tokio::future_into_py;
use pythonize::depythonize;
use serde_json::Value;
use std::fmt::Display;
use std::process;
use std::sync::Arc;
use tracing::{Instrument, info_span};

use crate::client::config::prepare_config;
use crate::handler::PythonHandler;
use crate::published::{PublishedDeque, PublishedMap, PublishedSet, PublishedValue};
use crate::request::{request_outcomes, request_parameters};
use crate::util::{check_fork, parse_read_cache};

mod config;
mod model;

pub use model::ProsodyClient;
use model::{consumer_state_name, shutdown};

/// Raises a failure to open a published reader as a `RuntimeError`.
fn opened<T, E: Display>(reader: Result<T, E>) -> PyResult<T> {
    reader.map_err(|error| PyRuntimeError::new_err(error.to_string()))
}

/// A client for interacting with Kafka using the Prosody library.
///
/// This client provides methods for sending messages to Kafka topics and
/// subscribing to topics for message consumption. It supports different
/// operational modes and configuration options.
#[pymethods]
impl ProsodyClient {
    /// Creates a client without blocking the Python event loop.
    ///
    /// # Arguments
    ///
    /// * `config` - An optional dictionary containing configuration options.
    ///
    /// # Returns
    ///
    /// A `PyResult` containing the new `ProsodyClient` if successful.
    ///
    /// # Errors
    ///
    /// Returns a `PyValueError` if the configuration is invalid.
    /// Returns a `PyRuntimeError` if the client fails to initialize.
    #[staticmethod]
    #[pyo3(signature = (**config))]
    fn create(py: Python, config: Option<&Bound<PyDict>>) -> PyResult<Py<PyAny>> {
        let config = prepare_config(py, config)?;
        future_into_py(py, async move {
            let client = config.connect().await?;
            Python::attach(|py| Py::new(py, client))
        })
        .map(Bound::unbind)
    }

    /// Sends a message to a specified topic.
    ///
    /// # Arguments
    ///
    /// * `topic` - The topic to which the message should be sent.
    /// * `key` - The key associated with the message.
    /// * `payload` - The content of the message (must be JSON-serializable).
    ///
    /// # Errors
    ///
    /// Returns a `PyRuntimeError` if there's an error sending the message.
    fn send<'p>(
        &self,
        py: Python<'p>,
        topic: String,
        key: String,
        payload: &Bound<'p, PyAny>,
    ) -> PyResult<Bound<'p, PyAny>> {
        check_fork(self.pid, "ProsodyClient")?;
        let payload = depythonize::<Value>(payload)?;
        let span = info_span!("python-send", %topic, %key);
        self.env.set_parent(py, &span)?;

        // Send the message using the producer
        let client = self.client.clone();
        future_into_py(py, async move {
            client
                .send(topic.as_str().into(), key, payload)
                .instrument(span)
                .await
                .map_err(|error| PyRuntimeError::new_err(error.to_string()))?;

            Ok(())
        })
    }

    /// Sends an excise record for a key.
    fn excise<'p>(&self, py: Python<'p>, topic: String, key: String) -> PyResult<Bound<'p, PyAny>> {
        check_fork(self.pid, "ProsodyClient")?;
        let span = info_span!("python-excise", %topic, %key);
        self.env.set_parent(py, &span)?;

        let client = self.client.clone();
        future_into_py(py, async move {
            client
                .excise(topic.as_str().into(), key)
                .instrument(span)
                .await
                .map_err(|error| PyRuntimeError::new_err(error.to_string()))?;
            Ok(())
        })
    }

    /// Sends one request and returns one outcome per subsystem.
    #[pyo3(signature = (topic, key, payload, *, subsystems, timeout))]
    fn request(
        &self,
        topic: String,
        key: String,
        payload: &Bound<'_, PyAny>,
        subsystems: Vec<String>,
        timeout: &Bound<'_, PyAny>,
    ) -> PyResult<Py<PyAny>> {
        check_fork(self.pid, "ProsodyClient")?;
        let py = payload.py();
        let span = info_span!("python-request", %topic, %key);
        self.env.set_parent(py, &span)?;
        let payload = depythonize::<Value>(payload)?;
        let (subsystems, timeout) = request_parameters(subsystems, timeout)?;
        let client = self.client.clone();

        future_into_py(py, async move {
            let results = client
                .request(
                    Vec::new(),
                    topic.as_str().into(),
                    key,
                    payload,
                    subsystems,
                    timeout,
                )
                .instrument(span)
                .await
                .map_err(|error| PyRuntimeError::new_err(error.to_string()))?;
            Python::attach(|py| request_outcomes(py, results))
        })
        .map(Bound::unbind)
    }

    /// Sends one excise request and returns one outcome per subsystem.
    #[pyo3(signature = (topic, key, *, subsystems, timeout))]
    fn request_excise(
        &self,
        py: Python<'_>,
        topic: String,
        key: String,
        subsystems: Vec<String>,
        timeout: &Bound<'_, PyAny>,
    ) -> PyResult<Py<PyAny>> {
        check_fork(self.pid, "ProsodyClient")?;
        let span = info_span!("python-request-excise", %topic, %key);
        self.env.set_parent(py, &span)?;
        let (subsystems, timeout) = request_parameters(subsystems, timeout)?;
        let client = self.client.clone();
        future_into_py(py, async move {
            let results = client
                .request_excise(Vec::new(), topic.as_str().into(), key, subsystems, timeout)
                .instrument(span)
                .await
                .map_err(|error| PyRuntimeError::new_err(error.to_string()))?;
            Python::attach(|py| request_outcomes(py, results))
        })
        .map(Bound::unbind)
    }

    /// Gets the current state of the consumer.
    ///
    /// # Returns
    ///
    /// A string that contains the current state: `unconfigured`, `configured`,
    /// `running`, or `shut_down`.
    ///
    /// # Errors
    ///
    /// Raises `RuntimeError` if the consumer configuration failed during
    /// build, with the full error message from the underlying
    /// `ModeConfigurationError`.
    fn consumer_state<'p>(&self, py: Python<'p>) -> PyResult<Bound<'p, PyAny>> {
        check_fork(self.pid, "ProsodyClient")?;
        let client = self.client.clone();
        future_into_py(py, async move {
            let state = client.consumer_state().await;
            if let ErasedConsumerState::ConfigurationFailed(error) = &state {
                return Err(PyRuntimeError::new_err(format!(
                    "consumer configuration failed: {error}"
                )));
            }
            Ok(consumer_state_name(&state))
        })
    }

    /// Opens a read-only published collection of `kind`: `"value"`, `"map"`,
    /// `"set"`, or `"deque"`.
    #[pyo3(signature = (subsystem, kind, name, *, read_cache = None))]
    fn _published<'p>(
        &self,
        py: Python<'p>,
        subsystem: String,
        kind: String,
        name: String,
        read_cache: Option<&Bound<'p, PyAny>>,
    ) -> PyResult<Bound<'p, PyAny>> {
        check_fork(self.pid, "ProsodyClient")?;
        let cache = parse_read_cache("read_cache", read_cache)?;
        let env = self.env.clone();
        let client = self.client.clone();
        future_into_py(py, async move {
            match kind.as_str() {
                "value" => {
                    let inner = opened(client.value_state(subsystem, name, cache).await)?;
                    Python::attach(|py| Ok(Py::new(py, PublishedValue { inner, env })?.into_any()))
                }
                "map" => {
                    let inner = opened(client.map_state(subsystem, name, cache).await)?;
                    Python::attach(|py| Ok(Py::new(py, PublishedMap { inner, env })?.into_any()))
                }
                "set" => {
                    let inner = opened(client.set_state(subsystem, name, cache).await)?;
                    Python::attach(|py| Ok(Py::new(py, PublishedSet { inner, env })?.into_any()))
                }
                "deque" => {
                    let inner = opened(client.deque_state(subsystem, name, cache).await)?;
                    Python::attach(|py| Ok(Py::new(py, PublishedDeque { inner, env })?.into_any()))
                }
                other => Err(PyValueError::new_err(format!(
                    "kind: expected \"value\", \"map\", \"set\", or \"deque\", got {other:?}"
                ))),
            }
        })
    }

    /// Subscribes to messages using the provided handler.
    ///
    /// # Arguments
    ///
    /// * `handler` - An instance implementing the `EventHandler` interface.
    ///
    /// # Errors
    ///
    /// Returns a `PyRuntimeError` if the consumer is not configured or is
    /// already subscribed.
    fn subscribe<'p>(
        &self,
        py: Python<'p>,
        handler: &Bound<'p, PyAny>,
    ) -> PyResult<Bound<'p, PyAny>> {
        check_fork(self.pid, "ProsodyClient")?;
        let handler = PythonHandler::new(handler)?;
        let retained = handler.clone();
        let current = Arc::clone(&self.handler);
        let client = self.client.clone();

        future_into_py(py, async move {
            client
                .subscribe(handler)
                .await
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
            *current.lock() = Some(retained);
            Ok(())
        })
    }

    /// Returns the number of partitions assigned to the consumer.
    ///
    /// Returns 0 if the consumer is not in the Running state.
    fn assigned_partition_count<'p>(&self, py: Python<'p>) -> PyResult<Bound<'p, PyAny>> {
        check_fork(self.pid, "ProsodyClient")?;
        let client = self.client.clone();
        future_into_py(
            py,
            async move { Ok(client.assigned_partition_count().await) },
        )
    }

    /// Checks if the consumer is stalled.
    ///
    /// Returns `false` if the consumer is not in the Running state.
    fn is_stalled<'p>(&self, py: Python<'p>) -> PyResult<Bound<'p, PyAny>> {
        check_fork(self.pid, "ProsodyClient")?;
        let client = self.client.clone();
        future_into_py(py, async move { Ok(client.is_stalled().await) })
    }

    /// Gets the source system identifier configured for the client.
    ///
    /// # Returns
    ///
    /// The source system identifier used to identify the originating service
    /// or component in produced messages, enabling loop detection.
    #[getter]
    fn source_system(&self) -> &str {
        &self.client.producer_config().source_system
    }

    /// Unsubscribes from messages and shuts down the consumer.
    ///
    /// # Errors
    ///
    /// Returns a `PyRuntimeError` if the consumer is not configured or not
    /// subscribed.
    fn unsubscribe<'p>(&self, py: Python<'p>) -> PyResult<Bound<'p, PyAny>> {
        check_fork(self.pid, "ProsodyClient")?;
        let client = self.client.clone();
        let current = Arc::clone(&self.handler);
        future_into_py(py, async move {
            let result = client
                .unsubscribe()
                .await
                .map_err(|error| PyRuntimeError::new_err(error.to_string()));
            *current.lock() = None;
            result
        })
    }

    /// Shuts down the client and all its services.
    /// Concurrent and repeated calls await the same operation.
    ///
    /// # Errors
    ///
    /// Returns a `PyRuntimeError` if shutdown fails.
    fn shutdown<'p>(&self, py: Python<'p>) -> PyResult<Bound<'p, PyAny>> {
        check_fork(self.pid, "ProsodyClient")?;
        let shutdown = self.shutdown.clone();
        let current = Arc::clone(&self.handler);
        future_into_py(py, async move {
            let result = shutdown
                .await
                .map_err(|error| PyRuntimeError::new_err(error.to_string()));
            *current.lock() = None;
            result
        })
    }

    /// Traverses Python objects contained in this Client for garbage
    /// collection.
    ///
    /// # Arguments
    ///
    /// * `visit` - A `PyVisit` object used to visit Python objects.
    ///
    /// # Errors
    ///
    /// Returns `Err(PyTraverseError)` if an error occurs during the traversal,
    /// such as when the `PyVisit::call` method fails.
    #[expect(
        clippy::needless_pass_by_value,
        reason = "PyO3 fixes the __traverse__ signature"
    )]
    fn __traverse__(&self, visit: PyVisit) -> Result<(), PyTraverseError> {
        // Never lock synchronization state inherited from another process.
        if process::id() == self.pid
            && let Some(handler) = self.handler.lock().as_ref()
        {
            visit.call(handler.handle_method().as_any())?;
            visit.call(handler.timer_method().as_any())?;
            visit.call(handler.message_class().as_any())?;
            visit.call(handler.timer_class().as_any())?;
            visit.call(handler.event_class().as_any())?;
            visit.call(handler.event_set_method().as_any())?;
        }

        Ok(())
    }
}
