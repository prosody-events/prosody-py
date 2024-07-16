use std::fmt;
use std::fmt::{Display, Formatter};
use std::mem::take;
use std::time::Duration;

use prosody::consumer::{ConsumerConfiguration, ConsumerConfigurationBuilder, ProsodyConsumer};
use prosody::producer::{ProducerConfiguration, ProducerConfigurationBuilder, ProsodyProducer};
use pyo3::exceptions::{PyRuntimeError, PyTypeError, PyValueError};
use pyo3::types::{
    PyAnyMethods, PyDelta, PyDeltaAccess, PyDict, PyDictMethods, PyString, PyTypeMethods,
};
use pyo3::{
    pyclass, pymethods, Bound, PyAny, PyObject, PyResult, PyTraverseError, PyVisit, Python,
};
use pythonize::depythonize_bound;
use serde_json::Value;
use tracing::info;

use crate::handler::PythonHandler;
use crate::RUNTIME;

#[pyclass]
pub struct ProsodyClient {
    producer: ProsodyProducer,
    producer_config: ProducerConfiguration,
    consumer: ConsumerState,
}

#[derive(Default)]
enum ConsumerState {
    #[default]
    Unconfigured,
    Configured(ConsumerConfiguration),
    Running {
        consumer: ProsodyConsumer,
        config: ConsumerConfiguration,
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
    #[new]
    #[pyo3(signature = (**config))]
    fn new(config: Option<&Bound<PyDict>>) -> PyResult<Self> {
        let mut producer_builder = ProducerConfiguration::builder();
        let mut consumer_builder = ConsumerConfiguration::builder();

        let Some(config) = config else {
            return try_build_config(&producer_builder, &consumer_builder);
        };

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

        try_build_config(&producer_builder, &consumer_builder)
    }

    /// Send a message to a specified topic.
    ///
    /// # Parameters
    ///
    /// * `topic`: The topic to which the message should be sent.
    /// * `key`: The key associated with the message.
    /// * `payload`: The content of the message. This can be any
    ///   JSON-serializable object.
    ///
    /// # Raises
    ///
    /// * `PyRuntimeError`: If there's an error sending the message.
    async fn send(&self, topic: String, key: String, payload: PyObject) -> PyResult<()> {
        let payload = Python::with_gil(|py| depythonize_bound::<Value>(payload.bind(py).clone()))?;

        self.producer
            .send(topic.as_str().into(), &key, payload)
            .await
            .map_err(|error| PyRuntimeError::new_err(error.to_string()))?;

        Ok(())
    }

    /// Get the current state of the consumer.
    ///
    /// # Returns
    ///
    /// A string representing the current state of the consumer.
    /// Possible values are 'unconfigured', 'configured', or 'running'.
    fn consumer_state(&self) -> String {
        self.consumer.to_string()
    }

    /// Subscribe to messages using the provided handler.
    ///
    /// # Parameters
    ///
    /// * `handler`: An instance of a class that implements the
    ///   AbstractMessageHandler interface. This handler will be called for each
    ///   received message.
    ///
    /// # Raises
    ///
    /// * `PyRuntimeError`: If the consumer is not configured or is already
    ///   subscribed.
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

        info!("initializing consumer");
        let consumer = ProsodyConsumer::new(&config, handler.clone()).map_err(|error| {
            PyRuntimeError::new_err(format!("failed to initialize consumer: {error:#}"))
        })?;

        self.consumer = ConsumerState::Running {
            consumer,
            config,
            handler,
        };

        Ok(())
    }

    /// Unsubscribe from messages.
    ///
    /// This method shuts down the consumer and stops receiving messages.
    ///
    /// # Raises
    ///
    /// * `PyRuntimeError`: If the consumer is not configured or not currently
    ///   subscribed.
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
    /// # Returns
    ///
    /// A string representation of the ProsodyClient, including its current
    /// state and configuration details.
    fn __repr__(slf: &Bound<Self>) -> PyResult<String> {
        let class_name: Bound<PyString> = slf.get_type().qualname()?;
        let slf = slf.borrow();

        let consumer_properties = match &slf.consumer {
            ConsumerState::Unconfigured => String::new(),
            ConsumerState::Configured(config) | ConsumerState::Running { config, .. } => {
                format!(
                    ", subscribed={}, group_id={}",
                    format_list(&config.subscribed_topics),
                    config.group_id
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
    /// # Returns
    ///
    /// A human-readable string describing the ProsodyClient, its state,
    /// and key configuration details.
    fn __str__(slf: &Bound<Self>) -> PyResult<String> {
        let class_name: Bound<PyString> = slf.get_type().qualname()?;
        let slf = slf.borrow();

        let consumer_properties = match &slf.consumer {
            ConsumerState::Unconfigured => String::new(),
            ConsumerState::Configured(config) | ConsumerState::Running { config, .. } => {
                format!(
                    ", subscribed={}, group_id={}",
                    config.subscribed_topics.join(","),
                    config.group_id
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

    /// Traverses the Python objects contained in this Client.
    ///
    /// This method is used by Python's garbage collector to traverse
    /// the object graph and detect cycles.
    ///
    /// Arguments:
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

fn try_build_config(
    producer_builder: &ProducerConfigurationBuilder,
    consumer_builder: &ConsumerConfigurationBuilder,
) -> PyResult<ProsodyClient> {
    let producer_config = producer_builder
        .build()
        .map_err(|error| PyValueError::new_err(error.to_string()))?;

    let producer = ProsodyProducer::new(&producer_config)
        .map_err(|error| PyRuntimeError::new_err(error.to_string()))?;

    let consumer = match consumer_builder.build().ok() {
        None => ConsumerState::Unconfigured,
        Some(consumer_config) => ConsumerState::Configured(consumer_config),
    };

    Ok(ProsodyClient {
        producer,
        producer_config,
        consumer,
    })
}

fn string_or_vec(value: &Bound<PyAny>) -> PyResult<Vec<String>> {
    if let Ok(string) = value.extract::<String>() {
        return Ok(vec![string]);
    }

    value.extract()
}

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

fn decode_optional_duration(value: &Bound<PyAny>) -> PyResult<Option<Duration>> {
    Ok(if value.is_none() {
        None
    } else {
        Some(decode_duration(value)?)
    })
}

fn format_list(value: &[impl AsRef<str>]) -> String {
    let items = value
        .iter()
        .map(|s| format!("'{}'", s.as_ref()))
        .collect::<Vec<_>>()
        .join(", ");

    format!("[{items}]")
}
