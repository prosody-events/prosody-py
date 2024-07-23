//! Provides a Python-compatible Kafka client for message production and
//! consumption.
//!
//! This module implements a `ProsodyClient` that interfaces with Kafka,
//! supporting both message production and consumption. It offers configurable
//! operational modes, retry mechanisms, and failure handling strategies.

use std::fmt;
use std::fmt::{Display, Formatter};
use std::mem::take;
use std::str::FromStr;
use std::time::Duration;

use prosody::consumer::failure::retry::{RetryConfiguration, RetryConfigurationBuilder};
use prosody::consumer::failure::topic::{
    FailureTopicConfiguration, FailureTopicConfigurationBuilder,
};
use prosody::consumer::{ConsumerConfiguration, ConsumerConfigurationBuilder, ProsodyConsumer};
use prosody::producer::{ProducerConfiguration, ProducerConfigurationBuilder, ProsodyProducer};
use pyo3::exceptions::{PyRuntimeError, PyTypeError, PyValueError};
use pyo3::types::{PyAnyMethods, PyDelta, PyDeltaAccess, PyDict, PyDictMethods, PyTypeMethods};
use pyo3::{
    pyclass, pymethods, Bound, PyAny, PyErr, PyObject, PyResult, PyTraverseError, PyVisit, Python,
};
use pythonize::depythonize_bound;
use serde_json::Value;
use tracing::info;

use crate::handler::PythonHandler;
use crate::RUNTIME;

/// A Python-compatible Kafka client that supports message production and
/// consumption.
#[pyclass]
pub struct ProsodyClient {
    producer: ProsodyProducer,
    producer_config: ProducerConfiguration,
    consumer: ConsumerState,
}

/// Pipeline mode for the Prosody client.
const PIPELINE_MODE: &str = "pipeline";

/// Low-latency mode for the Prosody client.
const LOW_LATENCY_MODE: &str = "low-latency";

/// Operational modes for the Prosody client.
#[derive(Copy, Clone, Debug, Default)]
enum Mode {
    #[default]
    Pipeline,
    LowLatency,
}

impl Display for Mode {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match self {
            Mode::Pipeline => f.write_str(PIPELINE_MODE),
            Mode::LowLatency => f.write_str(LOW_LATENCY_MODE),
        }
    }
}

impl FromStr for Mode {
    type Err = PyErr;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            PIPELINE_MODE => Ok(Mode::Pipeline),
            LOW_LATENCY_MODE => Ok(Mode::LowLatency),
            unknown => Err(PyValueError::new_err(format!("unknown mode: '{unknown}'"))),
        }
    }
}

/// Configuration for different operational modes of the Prosody client.
#[derive(Debug)]
enum ModeConfiguration {
    Pipeline {
        consumer: ConsumerConfiguration,
        retry: RetryConfiguration,
    },
    LowLatency {
        consumer: ConsumerConfiguration,
        retry: RetryConfiguration,
        failure_topic: FailureTopicConfiguration,
    },
}

impl ModeConfiguration {
    /// Returns the mode of the configuration.
    fn mode(&self) -> Mode {
        match self {
            ModeConfiguration::Pipeline { .. } => Mode::Pipeline,
            ModeConfiguration::LowLatency { .. } => Mode::LowLatency,
        }
    }

    /// Returns a reference to the consumer configuration.
    fn consumer(&self) -> &ConsumerConfiguration {
        match &self {
            ModeConfiguration::Pipeline { consumer, .. }
            | ModeConfiguration::LowLatency { consumer, .. } => consumer,
        }
    }
}

/// Represents the current state of the consumer.
#[derive(Default)]
enum ConsumerState {
    #[default]
    Unconfigured,
    Configured(ModeConfiguration),
    Running {
        consumer: ProsodyConsumer,
        config: ModeConfiguration,
        handler: PythonHandler,
    },
}

impl Display for ConsumerState {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        let state = match self {
            ConsumerState::Unconfigured => "unconfigured",
            ConsumerState::Configured(_) => "configured",
            ConsumerState::Running { .. } => "running",
        };

        f.write_str(state)
    }
}

#[pymethods]
impl ProsodyClient {
    /// Initialize a new ProsodyClient with the given configuration.
    ///
    /// Args:
    ///     **config: Configuration options for the client. Key options include:
    ///         bootstrap_servers: Kafka bootstrap servers
    ///         mock: Use mock Kafka client for testing
    ///         send_timeout: Timeout for message send operations
    ///         group_id: Consumer group ID
    ///         subscribed_topics: Topics to subscribe to
    ///         max_uncommitted: Maximum number of uncommitted messages
    ///         max_enqueued_per_key: Maximum enqueued messages per key
    ///         partition_shutdown_timeout: Timeout for partition shutdown
    ///         poll_interval: Interval between Kafka poll operations
    ///         commit_interval: Interval between offset commit operations
    ///         mode: Operating mode ('pipeline' or 'low-latency')
    ///         retry_base: Base value for exponential backoff in retries
    ///         max_retries: Maximum number of retries for failed operations
    ///         max_retry_delay: Maximum delay between retries
    ///         failure_topic: Topic for failed messages in low-latency mode
    ///
    /// Raises:
    ///     ValueError: If the configuration is invalid.
    ///     RuntimeError: If the client fails to initialize.
    #[new]
    #[pyo3(signature = (**config))]
    fn new(config: Option<&Bound<PyDict>>) -> PyResult<Self> {
        let mut mode = Mode::default();
        let mut producer_builder = ProducerConfiguration::builder();
        let mut consumer_builder = ConsumerConfiguration::builder();
        let mut retry_builder = RetryConfiguration::builder();
        let mut failure_topic_builder = FailureTopicConfiguration::builder();

        let Some(config) = config else {
            return try_build_config(
                mode,
                &producer_builder,
                &consumer_builder,
                &retry_builder,
                &failure_topic_builder,
            );
        };

        if let Some(mode_str) = config.get_item("mode")? {
            mode = mode_str.extract::<String>()?.parse()?;
        }

        if let Some(bootstrap) = config.get_item("bootstrap_servers")? {
            let bootstrap = string_or_vec(&bootstrap)?;
            producer_builder.bootstrap_servers(bootstrap.clone());
            consumer_builder.bootstrap_servers(bootstrap.clone());
        }

        if let Some(mock) = config.get_item("mock")? {
            let mock = mock.extract::<bool>()?;
            producer_builder.mock(mock);
            consumer_builder.mock(mock);
        }

        if let Some(send_timeout) = config.get_item("send_timeout")? {
            producer_builder.send_timeout(decode_optional_duration(&send_timeout)?);
        };

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

        if let Some(retry_base) = config.get_item("retry_base")? {
            retry_builder.base(retry_base.extract::<u8>()?);
        }

        if let Some(max_retries) = config.get_item("max_retries")? {
            retry_builder.max_retries(max_retries.extract::<u32>()?);
        }

        if let Some(retry_max_delay) = config.get_item("max_retry_delay")? {
            retry_builder.max_delay(decode_duration(&retry_max_delay)?);
        }

        if let Some(topic) = config.get_item("failure_topic")? {
            failure_topic_builder.failure_topic(topic.extract::<String>()?);
        }

        try_build_config(
            mode,
            &producer_builder,
            &consumer_builder,
            &retry_builder,
            &failure_topic_builder,
        )
    }

    /// Send a message to a specified topic.
    ///
    /// Args:
    ///     topic (str): The topic to which the message should be sent.
    ///     key (str): The key associated with the message.
    ///     payload: The content of the message (must be JSON-serializable).
    ///
    /// Raises:
    ///     RuntimeError: If there's an error sending the message.
    async fn send(&self, topic: String, key: String, payload: PyObject) -> PyResult<()> {
        let payload = Python::with_gil(|py| depythonize_bound::<Value>(payload.bind(py).clone()))?;

        self.producer
            .send([], topic.as_str().into(), &key, payload)
            .await
            .map_err(|error| PyRuntimeError::new_err(error.to_string()))?;

        Ok(())
    }

    /// Get the current state of the consumer.
    ///
    /// Returns:
    ///     str: The current state ('unconfigured', 'configured', or 'running').
    fn consumer_state(&self) -> String {
        self.consumer.to_string()
    }

    /// Subscribe to messages using the provided handler.
    ///
    /// Args:
    ///     handler: An instance implementing the AbstractMessageHandler
    /// interface.
    ///
    /// Raises:
    ///     RuntimeError: If the consumer is not configured or is already
    ///     subscribed.
    fn subscribe(&mut self, handler: &Bound<PyAny>) -> PyResult<()> {
        let _enter = RUNTIME.enter();
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

    /// Unsubscribe from messages and shut down the consumer.
    ///
    /// Raises:
    ///     RuntimeError: If the consumer is not configured or not subscribed.
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

    /// Return a string representation of the ProsodyClient.
    ///
    /// Returns:
    ///     str: A string representation of the ProsodyClient.
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

    /// Return a human-readable string description of the ProsodyClient.
    ///
    /// Returns:
    ///     str: A human-readable description of the ProsodyClient.
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

    /// Traverse Python objects contained in this Client for garbage collection.
    ///
    /// Args:
    ///     visit: A `PyVisit` object used to visit Python objects.
    ///
    /// Returns:
    ///     Result<(), PyTraverseError>: Ok if traversal was successful, Err
    ///     otherwise.
    #[allow(clippy::needless_pass_by_value)]
    fn __traverse__(&self, visit: PyVisit) -> Result<(), PyTraverseError> {
        if let ConsumerState::Running { handler, .. } = &self.consumer {
            visit.call(handler.handle_method.as_any())?;
        }

        Ok(())
    }
}

/// Attempts to build a `ProsodyClient` configuration based on the provided
/// builders.
///
/// # Arguments
///
/// * `mode` - The operational mode for the client.
/// * `producer_builder` - Builder for the producer configuration.
/// * `consumer_builder` - Builder for the consumer configuration.
/// * `retry_builder` - Builder for the retry configuration.
/// * `failure_topic_builder` - Builder for the failure topic configuration.
///
/// # Returns
///
/// A `PyResult` containing the configured `ProsodyClient` if successful.
///
/// # Errors
///
/// Returns a `PyValueError` if producer configuration, retry configuration,
/// or failure topic configuration fails.
/// Returns a `PyRuntimeError` if producer initialization fails.
fn try_build_config(
    mode: Mode,
    producer_builder: &ProducerConfigurationBuilder,
    consumer_builder: &ConsumerConfigurationBuilder,
    retry_builder: &RetryConfigurationBuilder,
    failure_topic_builder: &FailureTopicConfigurationBuilder,
) -> PyResult<ProsodyClient> {
    let producer_config = producer_builder.build().map_err(|error| {
        PyValueError::new_err(format!("failed to configure producer: {error:#}"))
    })?;

    let producer = ProsodyProducer::new(&producer_config).map_err(|error| {
        PyRuntimeError::new_err(format!("failed to initialize producer: {error:#}"))
    })?;

    let retry = retry_builder.build().map_err(|error| {
        PyValueError::new_err(format!(
            "failed to configure retry failure strategy: {error:#}"
        ))
    })?;

    let consumer = match consumer_builder.build().ok() {
        None => ConsumerState::Unconfigured,
        Some(consumer) => ConsumerState::Configured(match mode {
            Mode::Pipeline => ModeConfiguration::Pipeline { consumer, retry },
            Mode::LowLatency => {
                let failure_topic = failure_topic_builder.build().map_err(|error| {
                    PyValueError::new_err(format!(
                        "failed to configure failure topic strategy: {error:#}"
                    ))
                })?;

                ModeConfiguration::LowLatency {
                    consumer,
                    retry,
                    failure_topic,
                }
            }
        }),
    };

    Ok(ProsodyClient {
        producer,
        producer_config,
        consumer,
    })
}

/// Extracts a vector of strings from a Python object.
///
/// # Arguments
///
/// * `value` - A Python object that is either a string or a list of strings.
///
/// # Returns
///
/// A `PyResult` containing a vector of strings.
///
/// # Errors
///
/// Returns a `PyErr` if the extraction fails.
fn string_or_vec(value: &Bound<PyAny>) -> PyResult<Vec<String>> {
    if let Ok(string) = value.extract::<String>() {
        return Ok(vec![string]);
    }

    value.extract()
}

/// Decodes a Python object into a Rust `Duration`.
///
/// # Arguments
///
/// * `value` - A Python object representing a duration (either a `timedelta` or
///   a float).
///
/// # Returns
///
/// A `PyResult` containing the decoded `Duration`.
///
/// # Errors
///
/// Returns a `PyTypeError` if the input is neither a `timedelta` nor a float.
/// Returns a `PyValueError` if the float conversion fails.
fn decode_duration(value: &Bound<PyAny>) -> PyResult<Duration> {
    if let Ok(delta) = value.downcast::<PyDelta>() {
        let days = u64::try_from(delta.get_days())?;
        let seconds = u64::try_from(delta.get_seconds())?;
        let micros = u64::try_from(delta.get_microseconds())?;

        let mut duration = Duration::from_secs(days * 24 * 60 * 60);
        duration += Duration::from_secs(seconds);
        duration += Duration::from_micros(micros);
        return Ok(duration);
    };

    if let Ok(seconds) = value.extract::<f64>() {
        let duration = Duration::try_from_secs_f64(seconds)
            .map_err(|error| PyValueError::new_err(error.to_string()))?;

        return Ok(duration);
    }

    Err(PyTypeError::new_err(
        "expected a timedelta or non-negative float representing seconds",
    ))
}

/// Decodes an optional Python object into an optional Rust `Duration`.
///
/// # Arguments
///
/// * `value` - An optional Python object representing a duration.
///
/// # Returns
///
/// A `PyResult` containing an `Option<Duration>`.
///
/// # Errors
///
/// Propagates errors from `decode_duration`.
fn decode_optional_duration(value: &Bound<PyAny>) -> PyResult<Option<Duration>> {
    Ok(if value.is_none() {
        None
    } else {
        Some(decode_duration(value)?)
    })
}

/// Formats a slice of string-like values into a Python-style list
/// representation.
///
/// # Arguments
///
/// * `value` - A slice of values that can be referenced as strings.
///
/// # Returns
///
/// A `String` representing the formatted list.
fn format_list(value: &[impl AsRef<str>]) -> String {
    let items = value
        .iter()
        .map(|s| format!("'{}'", s.as_ref()))
        .collect::<Vec<_>>()
        .join(", ");

    format!("[{items}]")
}
