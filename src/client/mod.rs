//! Provides a Python-compatible Kafka client for message production and
//! consumption.
//!
//! This module implements a `ProsodyClient` that interfaces with Kafka,
//! supporting both message production and consumption. It offers configurable
//! operational modes, retry mechanisms, and failure handling strategies.

use std::collections::HashMap;
use std::mem::take;

use opentelemetry::propagation::{TextMapCompositePropagator, TextMapPropagator};
use prosody::consumer::failure::retry::RetryConfiguration;
use prosody::consumer::failure::topic::FailureTopicConfiguration;
use prosody::consumer::{ConsumerConfiguration, ProsodyConsumer};
use prosody::producer::{ProducerConfiguration, ProsodyProducer};
use pyo3::exceptions::PyRuntimeError;
use pyo3::types::{PyAnyMethods, PyDict, PyDictMethods, PyTypeMethods};
use pyo3::{
    pyclass, pymethods, Bound, PyAny, PyObject, PyResult, PyTraverseError, PyVisit, Python,
};
use pythonize::depythonize_bound;
use serde_json::Value;
use tracing::{info, info_span, Instrument};
use tracing_opentelemetry::OpenTelemetrySpanExt;

use crate::client::config::{
    decode_duration, decode_optional_duration, string_or_vec, try_build_config,
};
use crate::client::format::format_list;
use crate::client::mode::{Mode, ModeConfiguration};
use crate::client::state::ConsumerState;
use crate::handler::PythonHandler;
use crate::RUNTIME;

mod config;
mod format;
mod mode;
mod state;

/// Pipeline mode for the Prosody client.
const PIPELINE_MODE: &str = "pipeline";

/// Low-latency mode for the Prosody client.
const LOW_LATENCY_MODE: &str = "low-latency";

/// A client for interacting with Kafka using the Prosody library.
///
/// This client provides methods for sending messages to Kafka topics and
/// subscribing to topics for message consumption. It supports different
/// operational modes and configuration options.
#[pyclass]
pub struct ProsodyClient {
    producer: ProsodyProducer,
    producer_config: ProducerConfiguration,
    consumer: ConsumerState,
    propagator: TextMapCompositePropagator,
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
        let mut mode = Mode::default();
        let mut producer_builder = ProducerConfiguration::builder();
        let mut consumer_builder = ConsumerConfiguration::builder();
        let mut retry_builder = RetryConfiguration::builder();
        let mut failure_topic_builder = FailureTopicConfiguration::builder();

        // If no config is provided, build with default settings
        let Some(config) = config else {
            return try_build_config(
                py,
                mode,
                &producer_builder,
                &consumer_builder,
                &retry_builder,
                &failure_topic_builder,
            );
        };

        // Extract and set configuration options
        if let Some(mode_str) = config.get_item("mode")? {
            mode = mode_str.extract::<String>()?.parse()?;
        }

        // Configure bootstrap servers for both producer and consumer
        if let Some(bootstrap) = config.get_item("bootstrap_servers")? {
            let bootstrap = string_or_vec(&bootstrap)?;
            producer_builder.bootstrap_servers(bootstrap.clone());
            consumer_builder.bootstrap_servers(bootstrap);
        }

        // Set mock mode for both producer and consumer if specified
        if let Some(mock) = config.get_item("mock")? {
            let mock = mock.extract::<bool>()?;
            producer_builder.mock(mock);
            consumer_builder.mock(mock);
        }

        // Configure producer-specific options
        if let Some(send_timeout) = config.get_item("send_timeout")? {
            producer_builder.send_timeout(decode_optional_duration(&send_timeout)?);
        };

        // Configure consumer-specific options
        if let Some(group_id) = config.get_item("group_id")? {
            consumer_builder.group_id(group_id.extract::<String>()?);
        }

        if let Some(subscribed_topics) = config.get_item("subscribed_topics")? {
            let subscribed_topics = string_or_vec(&subscribed_topics)?;
            consumer_builder.subscribed_topics(subscribed_topics);
        }

        if let Some(max_uncommitted) = config.get_item("max_uncommitted")? {
            consumer_builder.max_uncommitted(max_uncommitted.extract::<usize>()?);
        }

        if let Some(max_enqueued_per_key) = config.get_item("max_enqueued_per_key")? {
            consumer_builder.max_enqueued_per_key(max_enqueued_per_key.extract::<usize>()?);
        }

        if let Some(value) = config.get_item("partition_shutdown_timeout")? {
            consumer_builder.partition_shutdown_timeout(decode_optional_duration(&value)?);
        }

        if let Some(poll_interval) = config.get_item("poll_interval")? {
            consumer_builder.poll_interval(decode_duration(&poll_interval)?);
        }

        if let Some(commit_interval) = config.get_item("commit_interval")? {
            consumer_builder.commit_interval(decode_duration(&commit_interval)?);
        }

        // Configure retry options
        if let Some(retry_base) = config.get_item("retry_base")? {
            retry_builder.base(retry_base.extract::<u8>()?);
        }

        if let Some(max_retries) = config.get_item("max_retries")? {
            retry_builder.max_retries(max_retries.extract::<u32>()?);
        }

        if let Some(retry_max_delay) = config.get_item("max_retry_delay")? {
            retry_builder.max_delay(decode_duration(&retry_max_delay)?);
        }

        // Configure failure topic
        if let Some(topic) = config.get_item("failure_topic")? {
            failure_topic_builder.failure_topic(topic.extract::<String>()?);
        }

        try_build_config(
            py,
            mode,
            &producer_builder,
            &consumer_builder,
            &retry_builder,
            &failure_topic_builder,
        )
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
            let data = PyDict::new_bound(py);
            self.inject.call1(py, (&data, context))?;
            let headers: HashMap<String, String> = data.extract()?;
            let payload = depythonize_bound::<Value>(payload.bind(py).clone())?;
            PyResult::Ok((headers, payload))
        })?;

        // Create and set the tracing context
        let context = self.propagator.extract(&trace_headers);
        let span = info_span!("send", %topic, %key);
        span.set_parent(context);

        // Send the message using the producer
        self.producer
            .send([], topic.as_str().into(), &key, payload)
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
        self.consumer.to_string()
    }

    /// Subscribes to messages using the provided handler.
    ///
    /// # Arguments
    ///
    /// * `handler` - An instance implementing the AbstractMessageHandler
    ///   interface.
    ///
    /// # Errors
    ///
    /// Returns a `PyRuntimeError` if the consumer is not configured or is
    /// already subscribed.
    fn subscribe(&mut self, handler: &Bound<PyAny>) -> PyResult<()> {
        let _enter = RUNTIME.enter();

        // Extract the configuration or return an error if not in the correct state
        let config = match take(&mut self.consumer) {
            ConsumerState::Unconfigured => {
                return Err(PyRuntimeError::new_err(
                    "consumer has not been configured; create a client with a valid consumer \
                     configuration",
                ));
            }
            ConsumerState::Configured(configuration) => configuration,
            running @ ConsumerState::Running { .. } => {
                self.consumer = running;
                return Err(PyRuntimeError::new_err("consumer is already subscribed"));
            }
        };

        let handler = PythonHandler::new(handler)?;

        // Initialize the consumer based on the configuration mode
        let consumer = match &config {
            ModeConfiguration::Pipeline { consumer, retry } => {
                ProsodyConsumer::pipeline_consumer(consumer, retry.clone(), handler.clone())
            }
            ModeConfiguration::LowLatency {
                consumer,
                retry,
                failure_topic,
            } => ProsodyConsumer::low_latency_consumer(
                consumer,
                retry.clone(),
                failure_topic.clone(),
                self.producer.clone(),
                handler.clone(),
            ),
        }
        .map_err(|error| {
            PyRuntimeError::new_err(format!("failed to initialize consumer: {error:#}"))
        })?;

        self.consumer = ConsumerState::Running {
            consumer,
            config,
            handler,
        };

        Ok(())
    }

    /// Unsubscribes from messages and shuts down the consumer.
    ///
    /// # Errors
    ///
    /// Returns a `PyRuntimeError` if the consumer is not configured or not
    /// subscribed.
    async fn unsubscribe(&mut self) -> PyResult<()> {
        let consumer = match take(&mut self.consumer) {
            ConsumerState::Unconfigured => {
                return Err(PyRuntimeError::new_err("consumer is not configured"));
            }
            configured @ ConsumerState::Configured(_) => {
                self.consumer = configured;
                return Err(PyRuntimeError::new_err("consumer is not subscribed"));
            }
            ConsumerState::Running {
                consumer, config, ..
            } => {
                self.consumer = ConsumerState::Configured(config);
                consumer
            }
        };

        info!("shutting down consumer");
        consumer.shutdown().await;
        Ok(())
    }

    /// Returns a string representation of the ProsodyClient.
    ///
    /// # Returns
    ///
    /// A string representation of the ProsodyClient.
    fn __repr__(slf: &Bound<Self>) -> PyResult<String> {
        let class_name = slf.get_type().qualname()?;
        let slf = slf.borrow();

        let consumer_properties = match &slf.consumer {
            ConsumerState::Unconfigured => String::new(),
            ConsumerState::Configured(config) | ConsumerState::Running { config, .. } => {
                let consumer_config = config.consumer();
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
            slf.consumer,
            format_list(&slf.producer_config.bootstrap_servers),
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

        let consumer_properties = match &slf.consumer {
            ConsumerState::Unconfigured => String::new(),
            ConsumerState::Configured(config) | ConsumerState::Running { config, .. } => {
                let consumer_config = config.consumer();
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
            slf.consumer,
            slf.producer_config.bootstrap_servers.join(","),
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
        if let ConsumerState::Running { handler, .. } = &self.consumer {
            visit.call(handler.handle_method.as_any())?;
        }

        visit.call(self.get_context.as_any())?;
        visit.call(self.inject.as_any())?;

        Ok(())
    }
}
