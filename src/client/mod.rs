//! Provides a Python-compatible Kafka client for message production and
//! consumption.
//!
//! This module implements a `ProsodyClient` that interfaces with Kafka,
//! supporting both message production and consumption. It offers configurable
//! operational modes, retry mechanisms, and failure handling strategies.

use futures::FutureExt;
use futures::future::{BoxFuture, Shared};
use opentelemetry::propagation::TextMapPropagator;
use parking_lot::Mutex;
use prosody::high_level::erased::{ErasedConsumerState, ErasedReadCache, SharedHighLevelClient};
use prosody::propagator::new_propagator;
use prosody::subsystem::SubsystemName;
use pyo3::exceptions::{PyRuntimeError, PyTypeError, PyValueError};
use pyo3::types::{PyAnyMethods, PyBool, PyDict, PyDictMethods, PyTypeMethods};
use pyo3::{Bound, Py, PyAny, PyResult, PyTraverseError, PyVisit, Python, pyclass, pymethods};
use pyo3_async_runtimes::tokio::future_into_py;
use pythonize::depythonize;
use serde_json::Value;
use std::collections::HashMap;
use std::process;
use std::sync::Arc;
use std::time::Duration;
use tracing::{Instrument, debug, info_span};
use tracing_opentelemetry::OpenTelemetrySpanExt;

use crate::client::config::prepare_config;
use crate::handler::PythonHandler;
use crate::published::{PublishedDeque, PublishedMap, PublishedValue};
use crate::request::to_python;
use crate::state::StateEnv;
use crate::util::decode_duration;

mod config;

type Shutdown = Shared<BoxFuture<'static, Result<(), Arc<str>>>>;

/// A client for interacting with Kafka using the Prosody library.
///
/// This client provides methods for sending messages to Kafka topics and
/// subscribing to topics for message consumption. It supports different
/// operational modes and configuration options.
#[pyclass(subclass, name = "_NativeProsodyClient")]
pub struct ProsodyClient {
    client: SharedHighLevelClient<PythonHandler>,
    shutdown: Shutdown,
    get_context: Py<PyAny>,
    inject: Py<PyAny>,
    handler: Arc<Mutex<Option<PythonHandler>>>,
    pid: u32,
}

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
        self.check_fork()?;
        // Extract trace headers and convert payload to JSON-serializable value
        let context = self.get_context.bind(py).call0()?;
        let data = PyDict::new(py);
        self.inject.call1(py, (&data, context))?;

        let headers: HashMap<String, String> = data.extract()?;
        let payload = depythonize::<Value>(payload)?;

        // Create and set the tracing context
        let context = self.client.propagator().extract(&headers);
        let span = info_span!("python-send", %topic, %key);
        if let Err(err) = span.set_parent(context) {
            debug!("failed to set parent span: {err:#}");
        }

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
        self.check_fork()?;
        let context = self.get_context.bind(py).call0()?;
        let data = PyDict::new(py);
        self.inject.call1(py, (&data, context))?;
        let headers: HashMap<String, String> = data.extract()?;
        let context = self.client.propagator().extract(&headers);
        let span = info_span!("python-excise", %topic, %key);
        if let Err(err) = span.set_parent(context) {
            debug!("failed to set parent span: {err:#}");
        }

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
    #[pyo3(signature = (topic, key, payload, *, subsystems, timeout, headers = None))]
    fn request(
        &self,
        topic: String,
        key: String,
        payload: &Bound<'_, PyAny>,
        subsystems: Vec<String>,
        timeout: &Bound<'_, PyAny>,
        headers: Option<HashMap<String, String>>,
    ) -> PyResult<Py<PyAny>> {
        self.check_fork()?;
        let py = payload.py();
        let context = self.get_context.bind(py).call0()?;
        let data = PyDict::new(py);
        self.inject.call1(py, (&data, context))?;
        let trace_headers: HashMap<String, String> = data.extract()?;
        let context = self.client.propagator().extract(&trace_headers);
        let span = info_span!("python-request", %topic, %key);
        if let Err(error) = span.set_parent(context) {
            debug!("failed to set parent span: {error:#}");
        }
        let payload = depythonize::<Value>(payload)?;
        let subsystems = subsystems
            .into_iter()
            .map(|name| {
                SubsystemName::try_new(name)
                    .map_err(|error| PyValueError::new_err(error.to_string()))
            })
            .collect::<PyResult<Vec<_>>>()?;
        let timeout = decode_duration(timeout)?;
        let client = self.client.clone();

        future_into_py(py, async move {
            let results = client
                .request(
                    headers.unwrap_or_default().into_iter().collect(),
                    topic.as_str().into(),
                    key,
                    payload,
                    subsystems,
                    timeout,
                )
                .instrument(span)
                .await
                .map_err(|error| PyRuntimeError::new_err(error.to_string()))?;
            Python::attach(|py| {
                let module = py.import("prosody.request")?;
                let outcomes = PyDict::new(py);
                for (subsystem, result) in results {
                    outcomes.set_item(subsystem.as_str(), to_python(py, &module, result)?)?;
                }
                Ok(outcomes.into_any().unbind())
            })
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
        self.check_fork()?;
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

    /// Opens a read-only published value collection.
    #[pyo3(signature = (subsystem, name, *, read_cache = None))]
    fn _published_value<'p>(
        &self,
        py: Python<'p>,
        subsystem: String,
        name: String,
        read_cache: Option<&Bound<'p, PyAny>>,
    ) -> PyResult<Bound<'p, PyAny>> {
        self.check_fork()?;
        let cache = parse_read_cache(read_cache)?;
        let env = self.published_env(py)?;
        let client = self.client.clone();
        future_into_py(py, async move {
            let inner = client
                .value_state(subsystem, name, cache)
                .await
                .map_err(|error| PyRuntimeError::new_err(error.to_string()))?;
            Python::attach(|py| Ok(Py::new(py, PublishedValue { inner, env })?.into_any()))
        })
    }

    /// Opens a read-only published map collection.
    #[pyo3(signature = (subsystem, name, *, read_cache = None))]
    fn _published_map<'p>(
        &self,
        py: Python<'p>,
        subsystem: String,
        name: String,
        read_cache: Option<&Bound<'p, PyAny>>,
    ) -> PyResult<Bound<'p, PyAny>> {
        self.check_fork()?;
        let cache = parse_read_cache(read_cache)?;
        let env = self.published_env(py)?;
        let client = self.client.clone();
        future_into_py(py, async move {
            let inner = client
                .map_state(subsystem, name, cache)
                .await
                .map_err(|error| PyRuntimeError::new_err(error.to_string()))?;
            Python::attach(|py| Ok(Py::new(py, PublishedMap { inner, env })?.into_any()))
        })
    }

    /// Opens a read-only published deque collection.
    #[pyo3(signature = (subsystem, name, *, read_cache = None))]
    fn _published_deque<'p>(
        &self,
        py: Python<'p>,
        subsystem: String,
        name: String,
        read_cache: Option<&Bound<'p, PyAny>>,
    ) -> PyResult<Bound<'p, PyAny>> {
        self.check_fork()?;
        let cache = parse_read_cache(read_cache)?;
        let env = self.published_env(py)?;
        let client = self.client.clone();
        future_into_py(py, async move {
            let inner = client
                .deque_state(subsystem, name, cache)
                .await
                .map_err(|error| PyRuntimeError::new_err(error.to_string()))?;
            Python::attach(|py| Ok(Py::new(py, PublishedDeque { inner, env })?.into_any()))
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
        self.check_fork()?;
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
        self.check_fork()?;
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
        self.check_fork()?;
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
        self.check_fork()?;
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
        self.check_fork()?;
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

    /// Returns a string representation of the `ProsodyClient`.
    ///
    /// # Returns
    ///
    /// A string representation of the `ProsodyClient`.
    fn __repr__(slf: &Bound<Self>) -> PyResult<String> {
        let class_name = slf.get_type().qualname()?;
        let slf = slf.borrow();
        slf.check_fork()?;
        Ok(format!(
            "{}(producer='running', bootstrap={:?})",
            class_name,
            slf.client.producer_config().bootstrap_servers,
        ))
    }

    /// Returns a human-readable string description of the `ProsodyClient`.
    ///
    /// # Returns
    ///
    /// A human-readable description of the `ProsodyClient`.
    fn __str__(slf: &Bound<Self>) -> PyResult<String> {
        let class_name = slf.get_type().qualname()?;
        let slf = slf.borrow();
        slf.check_fork()?;
        Ok(format!(
            "{}: producer=running, bootstrap={}",
            class_name,
            slf.client.producer_config().bootstrap_servers.join(","),
        ))
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
    #[allow(clippy::needless_pass_by_value)]
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

        visit.call(self.get_context.as_any())?;
        visit.call(self.inject.as_any())?;

        Ok(())
    }
}

fn shutdown(client: &SharedHighLevelClient<PythonHandler>) -> Shutdown {
    let client = client.clone();
    async move {
        client
            .shutdown()
            .await
            .map_err(|error| Arc::from(error.to_string()))
    }
    .boxed()
    .shared()
}

fn parse_read_cache(value: Option<&Bound<'_, PyAny>>) -> PyResult<ErasedReadCache> {
    let Some(value) = value else {
        return Ok(ErasedReadCache::Inherit);
    };
    if value.is_instance_of::<PyBool>() {
        return if value.extract::<bool>()? {
            Err(PyValueError::new_err(
                "read_cache=True is ambiguous; pass a duration, False, or None",
            ))
        } else {
            Ok(ErasedReadCache::Disabled)
        };
    }
    let seconds = if let Ok(seconds) = value.extract::<f64>() {
        seconds
    } else if let Ok(total_seconds) = value.getattr("total_seconds") {
        total_seconds.call0()?.extract::<f64>()?
    } else {
        return Err(PyTypeError::new_err(
            "read_cache must be seconds, timedelta, False, or None",
        ));
    };
    let ttl = Duration::try_from_secs_f64(seconds)
        .map_err(|_| PyValueError::new_err("read_cache must be finite and non-negative"))?;
    Ok(ErasedReadCache::Ttl(ttl))
}

#[allow(clippy::multiple_inherent_impl)]
impl ProsodyClient {
    fn published_env(&self, py: Python) -> PyResult<StateEnv> {
        let message_class = py.import("prosody")?.getattr("Message")?.unbind();
        StateEnv::resolve(
            py,
            &self.get_context,
            &self.inject,
            Arc::new(new_propagator()),
            &message_class,
        )
    }

    fn check_fork(&self) -> PyResult<()> {
        if process::id() != self.pid {
            return Err(PyRuntimeError::new_err(
                "ProsodyClient cannot be used after fork. Create a new client in the child \
                 process.",
            ));
        }
        Ok(())
    }
}

fn consumer_state_name(state: &ErasedConsumerState<PythonHandler>) -> &'static str {
    match state {
        ErasedConsumerState::Shutdown => "shut_down",
        ErasedConsumerState::Unconfigured => "unconfigured",
        ErasedConsumerState::ConfigurationFailed(_) => "configuration_failed",
        ErasedConsumerState::Configured(_) => "configured",
        ErasedConsumerState::Running { .. } => "running",
    }
}
