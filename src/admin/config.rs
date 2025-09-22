//! Configuration utilities for the `AdminClient`.
//!
//! This module provides functions for building and configuring an
//! `AdminClient`, including utilities for parsing Python configuration
//! objects and handling duration conversions.

use crate::admin::AdminClient;
use crate::util::string_or_vec;
use prosody::admin::{
    AdminConfiguration, AdminConfigurationBuilder, ProsodyAdminClient, TopicConfiguration,
    TopicConfigurationBuilder,
};
use pyo3::exceptions::{PyRuntimeError, PyTypeError, PyValueError};
use pyo3::types::{PyAnyMethods, PyDelta, PyDeltaAccess, PyDict, PyDictMethods};
use pyo3::{Bound, PyAny, PyResult, Python};
use std::sync::Arc;
use std::time::Duration;

/// Builds an `AdminClient` configuration based on the provided Python
/// configuration.
///
/// # Arguments
///
/// * `py` - The Python interpreter context.
/// * `config` - An optional Python dictionary containing configuration options.
///
/// # Returns
///
/// A `PyResult` containing the configured `AdminClient`.
///
/// # Errors
///
/// Returns a `PyValueError` if the configuration is invalid or parsing fails.
/// Returns a `PyRuntimeError` if client initialization fails.
pub fn try_build_admin_config(
    _py: Python,
    config: Option<&Bound<PyDict>>,
) -> PyResult<AdminClient> {
    // Build configuration - if no config dict is provided, builder will use
    // environment variables
    let admin_config = match config {
        Some(config_dict) => build_admin_config(config_dict)?,
        None => {
            // Use default builder which tries environment variables
            AdminConfigurationBuilder::default()
                .build()
                .map_err(|e| PyValueError::new_err(e.to_string()))?
        }
    };
    let client = ProsodyAdminClient::new(&admin_config)
        .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

    Ok(AdminClient {
        client: Arc::new(client),
    })
}

/// Builds an `AdminConfiguration` from the provided Python configuration.
///
/// # Arguments
///
/// * `config` - A Python dictionary containing configuration options.
///
/// # Returns
///
/// A `PyResult` containing the constructed `AdminConfiguration`.
///
/// # Errors
///
/// Returns a `PyErr` if extraction of configuration values fails.
fn build_admin_config(config: &Bound<PyDict>) -> PyResult<AdminConfiguration> {
    let mut builder = AdminConfigurationBuilder::default();

    if let Some(bootstrap) = config.get_item("bootstrap_servers")? {
        builder.bootstrap_servers(string_or_vec(&bootstrap)?);
    }

    builder
        .build()
        .map_err(|e| PyValueError::new_err(e.to_string()))
}

/// Builds a `TopicConfiguration` from the provided Python configuration.
///
/// # Arguments
///
/// * `config` - A Python dictionary containing configuration options.
/// * `topic_name` - The name of the topic (required).
///
/// # Returns
///
/// A `PyResult` containing the constructed `TopicConfiguration`.
///
/// # Errors
///
/// Returns a `PyErr` if extraction of configuration values fails.
pub fn build_topic_config(
    config: Option<&Bound<PyDict>>,
    topic_name: String,
) -> PyResult<TopicConfiguration> {
    let mut builder = TopicConfigurationBuilder::default();

    // Set the topic name (required)
    builder.name(topic_name);

    if let Some(config) = config {
        if let Some(partition_count) = config.get_item("partition_count")? {
            builder.partition_count(partition_count.extract::<u16>()?);
        }

        if let Some(replication_factor) = config.get_item("replication_factor")? {
            builder.replication_factor(replication_factor.extract::<u16>()?);
        }

        if let Some(cleanup_policy) = config.get_item("cleanup_policy")? {
            builder.cleanup_policy(cleanup_policy.extract::<String>()?);
        }

        if let Some(retention) = config.get_item("retention")? {
            builder.retention(decode_duration(&retention)?);
        }
    }

    builder
        .build()
        .map_err(|e| PyValueError::new_err(e.to_string()))
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
    }

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
