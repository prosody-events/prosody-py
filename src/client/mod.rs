//! Provides a Python-compatible Kafka client for message production and
//! consumption.
//!
//! This module implements a `ProsodyClient` that interfaces with Kafka,
//! supporting both message production and consumption. It offers configurable
//! operational modes, retry mechanisms, and failure handling strategies.

use opentelemetry::propagation::TextMapPropagator;
use prosody::high_level::state::ConsumerState;
use prosody::high_level::HighLevelClient;
use pyo3::exceptions::PyRuntimeError;
use pyo3::types::{PyAnyMethods, PyDict, PyTypeMethods};
use pyo3::{
    pyclass, pymethods, Bound, PyAny, PyObject, PyResult, PyTraverseError, PyVisit, Python,
};
use pythonize::depythonize;
use serde_json::Value;
use std::collections::HashMap;
use std::time::Duration;
use tracing::{info_span, Instrument};
use tracing_opentelemetry::OpenTelemetrySpanExt;

use crate::client::config::try_build_config;
use crate::client::format::format_list;

use crate::handler::PythonHandler;
use crate::RUNTIME;

mod config;
mod format;

/// A client for interacting with Kafka using the Prosody library.
///
/// This client provides methods for sending messages to Kafka topics and
/// subscribing to topics for message consumption. It supports different
/// operational modes and configuration options.
#[pyclass]
pub struct ProsodyClient {
    client: HighLevelClient<PythonHandler>,
    get_context: PyObject,
    inject: PyObject,
}

#[pymethods]
impl ProsodyClient {
    /// Creates a new ProsodyClient with the given configuration.
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
    #[new]
    #[pyo3(signature = (**config))]
    fn new(py: Python, config: Option<&Bound<PyDict>>) -> PyResult<Self> {
        try_build_config(py, config)
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
    async fn send(&self, topic: String, key: String, payload: PyObject) -> PyResult<()> {
        // Extract trace headers and convert payload to JSON-serializable value
        let (trace_headers, payload) = Python::with_gil(|py| {
            let context = self.get_context.bind(py).call0()?;
            let data = PyDict::new(py);
            self.inject.call1(py, (&data, context))?;

            let headers: HashMap<String, String> = data.extract()?;
            let payload = depythonize::<Value>(&payload.bind(py).clone())?;
            PyResult::Ok((headers, payload))
        })?;

        // Create and set the tracing context
        let context = self.client.propagator().extract(&trace_headers);
        let span = info_span!("python-send", %topic, %key);
        span.set_parent(context);

        // Send the message using the producer
        self.client
            .send(topic.as_str().into(), &key, &payload)
            .instrument(span)
            .await
            .map_err(|error| PyRuntimeError::new_err(error.to_string()))?;

        Ok(())
    }

    /// Gets the current state of the consumer.
    ///
    /// # Returns
    ///
    /// A string representing the current state ('unconfigured', 'configured',
    /// or 'running').
    fn consumer_state(&self) -> String {
        self.client.consumer_state().to_string()
    }

    /// Subscribes to messages using the provided handler.
    ///
    /// # Arguments
    ///
    /// * `handler` - An instance implementing the EventHandler interface.
    ///
    /// # Errors
    ///
    /// Returns a `PyRuntimeError` if the consumer is not configured or is
    /// already subscribed.
    fn subscribe(&self, handler: &Bound<PyAny>) -> PyResult<()> {
        let _enter = RUNTIME.enter();

        let timeout = self
            .client
            .consumer_state()
            .mode_configuration()
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?
            .consumer_config()
            .stall_threshold;

        // Set the task grace period to 80% of the total partition timeout
        let nanos = timeout.as_nanos();
        let result_nanos = (nanos * 4) / 5;
        let task_grace_period = Duration::from_nanos(result_nanos as u64);

        self.client
            .subscribe(PythonHandler::new(handler, task_grace_period)?)
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))
    }

    /// Returns the number of partitions assigned to the consumer.
    ///
    /// Returns 0 if the consumer is not in the Running state.
    fn assigned_partition_count(&self) -> u32 {
        self.client.assigned_partition_count()
    }

    /// Checks if the consumer is stalled.
    ///
    /// Returns `false` if the consumer is not in the Running state.
    fn is_stalled(&self) -> bool {
        self.client.is_stalled()
    }

    /// Unsubscribes from messages and shuts down the consumer.
    ///
    /// # Errors
    ///
    /// Returns a `PyRuntimeError` if the consumer is not configured or not
    /// subscribed.
    async fn unsubscribe(&self) -> PyResult<()> {
        self.client
            .unsubscribe()
            .await
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))
    }

    /// Returns a string representation of the ProsodyClient.
    ///
    /// # Returns
    ///
    /// A string representation of the ProsodyClient.
    fn __repr__(slf: &Bound<Self>) -> PyResult<String> {
        let class_name = slf.get_type().qualname()?;
        let slf = slf.borrow();
        let consumer_state = slf.client.consumer_state();
        let consumer_state_ref: &ConsumerState<_> = &consumer_state;

        let consumer_properties = match consumer_state_ref {
            ConsumerState::Unconfigured => String::new(),
            ConsumerState::Configured(config) | ConsumerState::Running { config, .. } => {
                let consumer_config = config.consumer_config();
                format!(
                    ", mode='{}', topics={}, group_id={}",
                    config.mode(),
                    format_list(&consumer_config.subscribed_topics),
                    consumer_config.group_id
                )
            }
        };

        Ok(format!(
            "{}(producer='running', consumer='{}', bootstrap={}{})",
            class_name,
            consumer_state_ref,
            format_list(&slf.client.producer_config().bootstrap_servers),
            consumer_properties
        ))
    }

    /// Returns a human-readable string description of the ProsodyClient.
    ///
    /// # Returns
    ///
    /// A human-readable description of the ProsodyClient.
    fn __str__(slf: &Bound<Self>) -> PyResult<String> {
        let class_name = slf.get_type().qualname()?;
        let slf = slf.borrow();
        let consumer_state = slf.client.consumer_state();
        let consumer_state_ref: &ConsumerState<_> = &consumer_state;

        let consumer_properties = match consumer_state_ref {
            ConsumerState::Unconfigured => String::new(),
            ConsumerState::Configured(config) | ConsumerState::Running { config, .. } => {
                let consumer_config = config.consumer_config();
                format!(
                    ", mode={}, topics={}, group_id={}",
                    config.mode(),
                    consumer_config.subscribed_topics.join(","),
                    consumer_config.group_id
                )
            }
        };

        Ok(format!(
            "{}: producer=running, consumer={}, bootstrap={}{}",
            class_name,
            consumer_state_ref,
            slf.client.producer_config().bootstrap_servers.join(","),
            consumer_properties
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
        // If the consumer is in the Running state, visit the handler's method
        if let ConsumerState::Running { handler, .. } = &*self.client.consumer_state() {
            visit.call(handler.handle_method().as_any())?;
            visit.call(handler.message_class().as_any())?;
            visit.call(handler.event_class().as_any())?;
            visit.call(handler.event_set_method().as_any())?;
        }

        visit.call(self.get_context.as_any())?;
        visit.call(self.inject.as_any())?;

        Ok(())
    }
}
