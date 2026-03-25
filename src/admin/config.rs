//! Configuration utilities for the `AdminClient`.
//!
//! This module provides functions for building and configuring an
//! `AdminClient`, including utilities for parsing Python configuration
//! objects and handling duration conversions.

use crate::admin::AdminClient;
use crate::util::{decode_duration, string_or_vec};
use prosody::admin::{
    AdminConfiguration, AdminConfigurationBuilder, ProsodyAdminClient, TopicConfiguration,
    TopicConfigurationBuilder,
};
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::types::{PyAnyMethods, PyDict, PyDictMethods};
use pyo3::{Bound, PyResult, Python};
use std::process;
use std::sync::Arc;

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
        pid: process::id(),
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
