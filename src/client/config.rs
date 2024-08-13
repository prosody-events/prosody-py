//! Configuration utilities for the `ProsodyClient`.
//!
//! This module provides functions for building and configuring a
//! `ProsodyClient`, including utilities for parsing Python configuration
//! objects and handling duration conversions.

use crate::client::ProsodyClient;
use prosody::combined::mode::{Mode, ModeError};
use prosody::combined::CombinedClient;
use prosody::consumer::failure::retry::RetryConfigurationBuilder;
use prosody::consumer::failure::topic::FailureTopicConfigurationBuilder;
use prosody::consumer::ConsumerConfigurationBuilder;
use prosody::producer::ProducerConfigurationBuilder;
use pyo3::exceptions::{PyRuntimeError, PyTypeError, PyValueError};
use pyo3::types::{PyAnyMethods, PyDelta, PyDeltaAccess, PyDict, PyDictMethods};
use pyo3::{Bound, IntoPy, PyAny, PyResult, Python};
use std::time::Duration;

/// Builds a `ProsodyClient` configuration based on the provided Python
/// configuration.
///
/// # Arguments
///
/// * `py` - The Python interpreter context.
/// * `config` - An optional Python dictionary containing configuration options.
///
/// # Returns
///
/// A `PyResult` containing the configured `ProsodyClient`.
///
/// # Errors
///
/// Returns a `PyValueError` if the configuration is invalid or parsing fails.
/// Returns a `PyRuntimeError` if client initialization fails.
pub fn try_build_config(py: Python, config: Option<&Bound<PyDict>>) -> PyResult<ProsodyClient> {
    // Get handles to OpenTelemetry functions
    let get_context = py
        .import_bound("opentelemetry.context")?
        .getattr("get_current")?
        .into_py(py);

    let inject = py
        .import_bound("opentelemetry.propagate")?
        .getattr("inject")?
        .into_py(py);

    // If no config is provided, create a client with default configurations
    let Some(config) = config else {
        let client = CombinedClient::new(
            Mode::default(),
            &ProducerConfigurationBuilder::default(),
            &ConsumerConfigurationBuilder::default(),
            &RetryConfigurationBuilder::default(),
            &FailureTopicConfigurationBuilder::default(),
        )
        .map_err(|e| PyValueError::new_err(e.to_string()))?;

        return Ok(ProsodyClient {
            client,
            get_context,
            inject,
        });
    };

    // Extract and set configuration options
    let mode = match config.get_item("mode")? {
        Some(mode_str) => mode_str
            .extract::<String>()?
            .parse()
            .map_err(|e: ModeError| PyValueError::new_err(e.to_string()))?,

        None => Mode::default(),
    };

    let producer_config = build_producer_config(config)?;
    let consumer_config = build_consumer_config(config)?;
    let retry_config = build_retry_config(config)?;
    let failure_topic_config = build_failure_topic_config(config)?;

    let client = CombinedClient::new(
        mode,
        &producer_config,
        &consumer_config,
        &retry_config,
        &failure_topic_config,
    )
    .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

    Ok(ProsodyClient {
        client,
        get_context,
        inject,
    })
}

/// Builds a `ProducerConfigurationBuilder` from the provided Python
/// configuration.
///
/// # Arguments
///
/// * `config` - A Python dictionary containing configuration options.
///
/// # Returns
///
/// A `PyResult` containing the constructed `ProducerConfigurationBuilder`.
///
/// # Errors
///
/// Returns a `PyErr` if extraction of configuration values fails.
fn build_producer_config(config: &Bound<PyDict>) -> PyResult<ProducerConfigurationBuilder> {
    let mut builder = ProducerConfigurationBuilder::default();

    if let Some(bootstrap) = config.get_item("bootstrap_servers")? {
        builder.bootstrap_servers(string_or_vec(&bootstrap)?);
    }

    if let Some(mock) = config.get_item("mock")? {
        builder.mock(mock.extract::<bool>()?);
    }

    if let Some(send_timeout) = config.get_item("send_timeout")? {
        builder.send_timeout(decode_optional_duration(&send_timeout)?);
    }

    Ok(builder)
}

/// Builds a `ConsumerConfigurationBuilder` from the provided Python
/// configuration.
///
/// # Arguments
///
/// * `config` - A Python dictionary containing configuration options.
///
/// # Returns
///
/// A `PyResult` containing the constructed `ConsumerConfigurationBuilder`.
///
/// # Errors
///
/// Returns a `PyErr` if extraction of configuration values fails.
fn build_consumer_config(config: &Bound<PyDict>) -> PyResult<ConsumerConfigurationBuilder> {
    let mut builder = ConsumerConfigurationBuilder::default();

    if let Some(bootstrap) = config.get_item("bootstrap_servers")? {
        builder.bootstrap_servers(string_or_vec(&bootstrap)?);
    }

    if let Some(mock) = config.get_item("mock")? {
        builder.mock(mock.extract::<bool>()?);
    }

    if let Some(group_id) = config.get_item("group_id")? {
        builder.group_id(group_id.extract::<String>()?);
    }

    if let Some(subscribed_topics) = config.get_item("subscribed_topics")? {
        builder.subscribed_topics(string_or_vec(&subscribed_topics)?);
    }

    if let Some(max_uncommitted) = config.get_item("max_uncommitted")? {
        builder.max_uncommitted(max_uncommitted.extract::<usize>()?);
    }

    if let Some(max_enqueued_per_key) = config.get_item("max_enqueued_per_key")? {
        builder.max_enqueued_per_key(max_enqueued_per_key.extract::<usize>()?);
    }

    if let Some(value) = config.get_item("partition_shutdown_timeout")? {
        builder.partition_shutdown_timeout(decode_optional_duration(&value)?);
    }

    if let Some(poll_interval) = config.get_item("poll_interval")? {
        builder.poll_interval(decode_duration(&poll_interval)?);
    }

    if let Some(commit_interval) = config.get_item("commit_interval")? {
        builder.commit_interval(decode_duration(&commit_interval)?);
    }

    Ok(builder)
}

/// Builds a `RetryConfigurationBuilder` from the provided Python configuration.
///
/// # Arguments
///
/// * `config` - A Python dictionary containing configuration options.
///
/// # Returns
///
/// A `PyResult` containing the constructed `RetryConfigurationBuilder`.
///
/// # Errors
///
/// Returns a `PyErr` if extraction of configuration values fails.
fn build_retry_config(config: &Bound<PyDict>) -> PyResult<RetryConfigurationBuilder> {
    let mut builder = RetryConfigurationBuilder::default();

    if let Some(retry_base) = config.get_item("retry_base")? {
        builder.base(decode_duration(&retry_base)?);
    }

    if let Some(max_retries) = config.get_item("max_retries")? {
        builder.max_retries(max_retries.extract::<u32>()?);
    }

    if let Some(retry_max_delay) = config.get_item("max_retry_delay")? {
        builder.max_delay(decode_duration(&retry_max_delay)?);
    }

    Ok(builder)
}

/// Builds a `FailureTopicConfigurationBuilder` from the provided Python
/// configuration.
///
/// # Arguments
///
/// * `config` - A Python dictionary containing configuration options.
///
/// # Returns
///
/// A `PyResult` containing the constructed `FailureTopicConfigurationBuilder`.
///
/// # Errors
///
/// Returns a `PyErr` if extraction of configuration values fails.
fn build_failure_topic_config(
    config: &Bound<PyDict>,
) -> PyResult<FailureTopicConfigurationBuilder> {
    let mut builder = FailureTopicConfigurationBuilder::default();

    if let Some(topic) = config.get_item("failure_topic")? {
        builder.failure_topic(topic.extract::<String>()?);
    }

    Ok(builder)
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
    // Try to extract a single string first
    if let Ok(string) = value.extract::<String>() {
        return Ok(vec![string]);
    }

    // If not a single string, try to extract a list of strings
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
    // Try to decode as a timedelta first
    if let Ok(delta) = value.downcast::<PyDelta>() {
        let days = u64::try_from(delta.get_days())?;
        let seconds = u64::try_from(delta.get_seconds())?;
        let micros = u64::try_from(delta.get_microseconds())?;

        let mut duration = Duration::from_secs(days * 24 * 60 * 60);
        duration += Duration::from_secs(seconds);
        duration += Duration::from_micros(micros);
        return Ok(duration);
    };

    // If not a timedelta, try to decode as a float
    if let Ok(seconds) = value.extract::<f64>() {
        let duration = Duration::try_from_secs_f64(seconds)
            .map_err(|error| PyValueError::new_err(error.to_string()))?;

        return Ok(duration);
    }

    // If neither a timedelta nor a float, return an error
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
