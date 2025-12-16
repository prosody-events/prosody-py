//! Configuration utilities for the `ProsodyClient`.
//!
//! This module provides functions for building and configuring a
//! `ProsodyClient`, including utilities for parsing Python configuration
//! objects and handling duration conversions.

use crate::client::ProsodyClient;
use crate::util::{decode_duration, decode_optional_duration, string_or_vec};
use prosody::cassandra::config::CassandraConfigurationBuilder;
use prosody::consumer::ConsumerConfigurationBuilder;
use prosody::consumer::middleware::defer::DeferConfigurationBuilder;
use prosody::consumer::middleware::monopolization::MonopolizationConfigurationBuilder;
use prosody::consumer::middleware::retry::RetryConfigurationBuilder;
use prosody::consumer::middleware::scheduler::SchedulerConfigurationBuilder;
use prosody::consumer::middleware::timeout::TimeoutConfigurationBuilder;
use prosody::consumer::middleware::topic::FailureTopicConfigurationBuilder;
use prosody::high_level::mode::{Mode, ModeError};
use prosody::high_level::{ConsumerBuilders, HighLevelClient};
use prosody::producer::ProducerConfigurationBuilder;
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::types::{PyAnyMethods, PyDict, PyDictMethods};
use pyo3::{Bound, IntoPyObjectExt, PyResult, Python};
use std::sync::Arc;

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
        .import("opentelemetry.context")?
        .getattr("get_current")?
        .into_py_any(py)?;

    let inject = py
        .import("opentelemetry.propagate")?
        .getattr("inject")?
        .into_py_any(py)?;

    // If no config is provided, create a client with default configurations
    let Some(config) = config else {
        let consumer_builders = ConsumerBuilders {
            consumer: ConsumerConfigurationBuilder::default(),
            retry: RetryConfigurationBuilder::default(),
            failure_topic: FailureTopicConfigurationBuilder::default(),
            scheduler: SchedulerConfigurationBuilder::default(),
            monopolization: MonopolizationConfigurationBuilder::default(),
            defer: DeferConfigurationBuilder::default(),
            timeout: TimeoutConfigurationBuilder::default(),
        };

        let client = HighLevelClient::new(
            Mode::default(),
            &mut ProducerConfigurationBuilder::default(),
            &consumer_builders,
            &CassandraConfigurationBuilder::default(),
        )
        .map_err(|e| PyValueError::new_err(e.to_string()))?;

        return Ok(ProsodyClient {
            client: Arc::new(client),
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

    let mut producer_config = build_producer_config(config)?;
    let consumer_builders = build_consumer_builders(config)?;
    let cassandra_config = build_cassandra_config(config)?;

    let client = HighLevelClient::new(
        mode,
        &mut producer_config,
        &consumer_builders,
        &cassandra_config,
    )
    .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

    Ok(ProsodyClient {
        client: Arc::new(client),
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

    if let Some(source_system) = config.get_item("source_system")? {
        builder.source_system(source_system.extract::<String>()?);
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

    if let Some(allowed_event_types) = config.get_item("allowed_events")? {
        builder.allowed_events(string_or_vec(&allowed_event_types)?);
    }

    if let Some(idempotence_cache_size) = config.get_item("idempotence_cache_size")? {
        builder.idempotence_cache_size(idempotence_cache_size.extract::<usize>()?);
    }

    if let Some(max_uncommitted) = config.get_item("max_uncommitted")? {
        builder.max_uncommitted(max_uncommitted.extract::<usize>()?);
    }

    if let Some(max_enqueued_per_key) = config.get_item("max_enqueued_per_key")? {
        builder.max_enqueued_per_key(max_enqueued_per_key.extract::<usize>()?);
    }

    if let Some(value) = config.get_item("stall_threshold")? {
        builder.stall_threshold(decode_duration(&value)?);
    }

    if let Some(value) = config.get_item("shutdown_timeout")? {
        builder.shutdown_timeout(decode_duration(&value)?);
    }

    if let Some(poll_interval) = config.get_item("poll_interval")? {
        builder.poll_interval(decode_duration(&poll_interval)?);
    }

    if let Some(commit_interval) = config.get_item("commit_interval")? {
        builder.commit_interval(decode_duration(&commit_interval)?);
    }

    if let Some(probe_port) = config.get_item("probe_port")? {
        builder.probe_port(probe_port.extract::<Option<u16>>()?);
    }

    if let Some(slab_size) = config.get_item("slab_size")? {
        builder.slab_size(decode_duration(&slab_size)?);
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

/// Builds a `CassandraConfigurationBuilder` from the provided Python
/// configuration.
///
/// # Arguments
///
/// * `config` - A Python dictionary containing configuration options.
///
/// # Returns
///
/// A `PyResult` containing the constructed `CassandraConfigurationBuilder`.
///
/// # Errors
///
/// Returns a `PyErr` if extraction of configuration values fails.
fn build_cassandra_config(config: &Bound<PyDict>) -> PyResult<CassandraConfigurationBuilder> {
    let mut builder = CassandraConfigurationBuilder::default();

    // Cassandra nodes (optional, uses environment variable if not provided)
    if let Some(nodes) = config.get_item("cassandra_nodes")? {
        builder.nodes(string_or_vec(&nodes)?);
    }

    // Cassandra keyspace (optional, defaults to "prosody")
    if let Some(keyspace) = config.get_item("cassandra_keyspace")? {
        builder.keyspace(keyspace.extract::<String>()?);
    }

    // Cassandra datacenter (optional)
    if let Some(datacenter) = config.get_item("cassandra_datacenter")? {
        builder.datacenter(Some(datacenter.extract::<String>()?));
    }

    // Cassandra rack (optional)
    if let Some(rack) = config.get_item("cassandra_rack")? {
        builder.rack(Some(rack.extract::<String>()?));
    }

    // Cassandra user (optional)
    if let Some(user) = config.get_item("cassandra_user")? {
        builder.user(Some(user.extract::<String>()?));
    }

    // Cassandra password (optional)
    if let Some(password) = config.get_item("cassandra_password")? {
        builder.password(Some(password.extract::<String>()?));
    }

    // Cassandra retention (optional, defaults to 30 days)
    if let Some(retention) = config.get_item("cassandra_retention")? {
        builder.retention(decode_duration(&retention)?);
    }

    Ok(builder)
}

/// Builds a `SchedulerConfigurationBuilder` from the provided Python
/// configuration.
///
/// # Arguments
///
/// * `config` - A Python dictionary containing configuration options.
///
/// # Returns
///
/// A `PyResult` containing the constructed `SchedulerConfigurationBuilder`.
///
/// # Errors
///
/// Returns a `PyErr` if extraction of configuration values fails.
fn build_scheduler_config(config: &Bound<PyDict>) -> PyResult<SchedulerConfigurationBuilder> {
    let mut builder = SchedulerConfigurationBuilder::default();

    if let Some(max_concurrency) = config.get_item("max_concurrency")? {
        builder.max_concurrency(max_concurrency.extract::<usize>()?);
    }

    if let Some(failure_weight) = config.get_item("scheduler_failure_weight")? {
        builder.failure_weight(failure_weight.extract::<f64>()?);
    }

    if let Some(max_wait) = config.get_item("scheduler_max_wait")? {
        builder.max_wait(decode_duration(&max_wait)?);
    }

    if let Some(wait_weight) = config.get_item("scheduler_wait_weight")? {
        builder.wait_weight(wait_weight.extract::<f64>()?);
    }

    if let Some(cache_size) = config.get_item("scheduler_cache_size")? {
        builder.cache_size(cache_size.extract::<usize>()?);
    }

    Ok(builder)
}

/// Builds a `MonopolizationConfigurationBuilder` from the provided Python
/// configuration.
///
/// # Arguments
///
/// * `config` - A Python dictionary containing configuration options.
///
/// # Returns
///
/// A `PyResult` containing the constructed
/// `MonopolizationConfigurationBuilder`.
///
/// # Errors
///
/// Returns a `PyErr` if extraction of configuration values fails.
fn build_monopolization_config(
    config: &Bound<PyDict>,
) -> PyResult<MonopolizationConfigurationBuilder> {
    let mut builder = MonopolizationConfigurationBuilder::default();

    if let Some(enabled) = config.get_item("monopolization_enabled")? {
        builder.enabled(enabled.extract::<bool>()?);
    }

    if let Some(threshold) = config.get_item("monopolization_threshold")? {
        builder.monopolization_threshold(threshold.extract::<f64>()?);
    }

    if let Some(window) = config.get_item("monopolization_window")? {
        builder.window_duration(decode_duration(&window)?);
    }

    if let Some(cache_size) = config.get_item("monopolization_cache_size")? {
        builder.cache_size(cache_size.extract::<usize>()?);
    }

    Ok(builder)
}

/// Builds a `DeferConfigurationBuilder` from the provided Python configuration.
///
/// # Arguments
///
/// * `config` - A Python dictionary containing configuration options.
///
/// # Returns
///
/// A `PyResult` containing the constructed `DeferConfigurationBuilder`.
///
/// # Errors
///
/// Returns a `PyErr` if extraction of configuration values fails.
fn build_defer_config(config: &Bound<PyDict>) -> PyResult<DeferConfigurationBuilder> {
    let mut builder = DeferConfigurationBuilder::default();

    if let Some(enabled) = config.get_item("defer_enabled")? {
        builder.enabled(enabled.extract::<bool>()?);
    }

    if let Some(base) = config.get_item("defer_base")? {
        builder.base(decode_duration(&base)?);
    }

    if let Some(max_delay) = config.get_item("defer_max_delay")? {
        builder.max_delay(decode_duration(&max_delay)?);
    }

    if let Some(failure_threshold) = config.get_item("defer_failure_threshold")? {
        builder.failure_threshold(failure_threshold.extract::<f64>()?);
    }

    if let Some(failure_window) = config.get_item("defer_failure_window")? {
        builder.failure_window(decode_duration(&failure_window)?);
    }

    if let Some(cache_size) = config.get_item("defer_cache_size")? {
        builder.cache_size(cache_size.extract::<usize>()?);
    }

    if let Some(seek_timeout) = config.get_item("defer_seek_timeout")? {
        builder.seek_timeout(decode_duration(&seek_timeout)?);
    }

    if let Some(discard_threshold) = config.get_item("defer_discard_threshold")? {
        builder.discard_threshold(discard_threshold.extract::<i64>()?);
    }

    Ok(builder)
}

/// Builds a `TimeoutConfigurationBuilder` from the provided Python
/// configuration.
///
/// # Arguments
///
/// * `config` - A Python dictionary containing configuration options.
///
/// # Returns
///
/// A `PyResult` containing the constructed `TimeoutConfigurationBuilder`.
///
/// # Errors
///
/// Returns a `PyErr` if extraction of configuration values fails.
fn build_timeout_config(config: &Bound<PyDict>) -> PyResult<TimeoutConfigurationBuilder> {
    let mut builder = TimeoutConfigurationBuilder::default();

    if let Some(timeout) = config.get_item("timeout")? {
        builder.timeout(Some(decode_duration(&timeout)?));
    }

    Ok(builder)
}

/// Builds `ConsumerBuilders` from the provided Python configuration.
///
/// # Arguments
///
/// * `config` - A Python dictionary containing configuration options.
///
/// # Returns
///
/// A `PyResult` containing the constructed `ConsumerBuilders`.
///
/// # Errors
///
/// Returns a `PyErr` if extraction of configuration values fails.
fn build_consumer_builders(config: &Bound<PyDict>) -> PyResult<ConsumerBuilders> {
    Ok(ConsumerBuilders {
        consumer: build_consumer_config(config)?,
        retry: build_retry_config(config)?,
        failure_topic: build_failure_topic_config(config)?,
        scheduler: build_scheduler_config(config)?,
        monopolization: build_monopolization_config(config)?,
        defer: build_defer_config(config)?,
        timeout: build_timeout_config(config)?,
    })
}
