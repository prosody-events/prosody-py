//! Configuration utilities for the `ProsodyClient`.
//!
//! This module provides functions for building and configuring a
//! `ProsodyClient`, including utilities for parsing Python configuration
//! objects and handling duration conversions.

use std::time::Duration;

use prosody::consumer::failure::retry::RetryConfigurationBuilder;
use prosody::consumer::failure::topic::FailureTopicConfigurationBuilder;
use prosody::consumer::ConsumerConfigurationBuilder;
use prosody::producer::{ProducerConfigurationBuilder, ProsodyProducer};
use prosody::propagator::new_propagator;
use pyo3::exceptions::{PyRuntimeError, PyTypeError, PyValueError};
use pyo3::types::{PyAnyMethods, PyDelta, PyDeltaAccess};
use pyo3::{Bound, IntoPy, PyAny, PyResult, Python};

use crate::client::mode::{Mode, ModeConfiguration};
use crate::client::state::ConsumerState;
use crate::client::ProsodyClient;

/// Builds a `ProsodyClient` configuration based on the provided builders.
///
/// # Arguments
///
/// * `py` - The Python interpreter context.
/// * `mode` - The operational mode for the client.
/// * `producer_builder` - Builder for the producer configuration.
/// * `consumer_builder` - Builder for the consumer configuration.
/// * `retry_builder` - Builder for the retry configuration.
/// * `failure_topic_builder` - Builder for the failure topic configuration.
///
/// # Returns
///
/// A `PyResult` containing the configured `ProsodyClient`.
///
/// # Errors
///
/// Returns a `PyValueError` if producer configuration, retry configuration,
/// or failure topic configuration fails.
/// Returns a `PyRuntimeError` if producer initialization fails.
pub fn try_build_config(
    py: Python,
    mode: Mode,
    producer_builder: &ProducerConfigurationBuilder,
    consumer_builder: &ConsumerConfigurationBuilder,
    retry_builder: &RetryConfigurationBuilder,
    failure_topic_builder: &FailureTopicConfigurationBuilder,
) -> PyResult<ProsodyClient> {
    // Get handles to OpenTelemetry functions
    let get_context = py
        .import_bound("opentelemetry.context")?
        .getattr("get_current")?
        .into_py(py);

    let inject = py
        .import_bound("opentelemetry.propagate")?
        .getattr("inject")?
        .into_py(py);

    let propagator = new_propagator();

    // Build the producer configuration and initialize the producer
    let producer_config = producer_builder.build().map_err(|error| {
        PyValueError::new_err(format!("failed to configure producer: {error:#}"))
    })?;

    let producer = ProsodyProducer::new(&producer_config).map_err(|error| {
        PyRuntimeError::new_err(format!("failed to initialize producer: {error:#}"))
    })?;

    // Build the retry configuration
    let retry = retry_builder.build().map_err(|error| {
        PyValueError::new_err(format!(
            "failed to configure retry failure strategy: {error:#}"
        ))
    })?;

    // Configure the consumer based on the mode
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

    // Create and return the ProsodyClient
    Ok(ProsodyClient {
        producer,
        producer_config,
        consumer,
        propagator,
        get_context,
        inject,
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
pub fn string_or_vec(value: &Bound<PyAny>) -> PyResult<Vec<String>> {
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
pub fn decode_duration(value: &Bound<PyAny>) -> PyResult<Duration> {
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
pub fn decode_optional_duration(value: &Bound<PyAny>) -> PyResult<Option<Duration>> {
    Ok(if value.is_none() {
        None
    } else {
        Some(decode_duration(value)?)
    })
}
