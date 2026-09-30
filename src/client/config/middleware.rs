//! Configuration of the consumer middleware: deduplication, retry, the
//! failure topic, scheduling, monopolization, deferral, and timeouts.

use crate::util::{decode_duration, decode_optional_duration};
use prosody::consumer::middleware::deduplication::DeduplicationConfigurationBuilder;
use prosody::consumer::middleware::defer::DeferConfigurationBuilder;
use prosody::consumer::middleware::monopolization::MonopolizationConfigurationBuilder;
use prosody::consumer::middleware::retry::RetryConfigurationBuilder;
use prosody::consumer::middleware::scheduler::SchedulerConfigurationBuilder;
use prosody::consumer::middleware::timeout::TimeoutConfigurationBuilder;
use prosody::consumer::middleware::topic::FailureTopicConfigurationBuilder;
use pyo3::exceptions::PyValueError;
use pyo3::types::{PyAnyMethods, PyDict, PyDictMethods};
use pyo3::{Bound, PyResult};
use std::num::NonZeroUsize;

/// Builds a `DeduplicationConfigurationBuilder` from the provided Python
/// configuration.
///
/// # Arguments
///
/// * `config` - A Python dictionary containing configuration options.
///
/// # Returns
///
/// A `PyResult` containing the constructed `DeduplicationConfigurationBuilder`.
///
/// # Errors
///
/// Returns a `PyErr` if extraction of configuration values fails.
pub(super) fn build_dedup_config(
    config: &Bound<PyDict>,
) -> PyResult<DeduplicationConfigurationBuilder> {
    let mut builder = DeduplicationConfigurationBuilder::default();

    if let Some(cache_capacity) = config.get_item("idempotence_cache_size")?
        && !cache_capacity.is_none()
    {
        let capacity = NonZeroUsize::new(cache_capacity.extract::<usize>()?).ok_or_else(|| {
            PyValueError::new_err("idempotence_cache_size must be greater than 0")
        })?;
        builder.cache_capacity(capacity);
    }

    if let Some(version) = config.get_item("idempotence_version")?
        && !version.is_none()
    {
        builder.version(version.extract::<String>()?);
    }

    if let Some(ttl) = config.get_item("idempotence_ttl")?
        && let Some(duration) = decode_optional_duration(&ttl)?
    {
        builder.ttl(duration);
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
pub(super) fn build_retry_config(config: &Bound<PyDict>) -> PyResult<RetryConfigurationBuilder> {
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
pub(super) fn build_failure_topic_config(
    config: &Bound<PyDict>,
) -> PyResult<FailureTopicConfigurationBuilder> {
    let mut builder = FailureTopicConfigurationBuilder::default();

    if let Some(topic) = config.get_item("failure_topic")? {
        builder.failure_topic(topic.extract::<String>()?);
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
pub(super) fn build_scheduler_config(
    config: &Bound<PyDict>,
) -> PyResult<SchedulerConfigurationBuilder> {
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
pub(super) fn build_monopolization_config(
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
pub(super) fn build_defer_config(config: &Bound<PyDict>) -> PyResult<DeferConfigurationBuilder> {
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

    if let Some(store_cache_size) = config.get_item("defer_store_cache_size")? {
        let store_cache_size: usize = store_cache_size.extract()?;
        builder.store_cache_size(store_cache_size);
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
pub(super) fn build_timeout_config(
    config: &Bound<PyDict>,
) -> PyResult<TimeoutConfigurationBuilder> {
    let mut builder = TimeoutConfigurationBuilder::default();

    if let Some(timeout) = config.get_item("timeout")? {
        builder.timeout(Some(decode_duration(&timeout)?));
    }

    Ok(builder)
}
