//! Configuration of the consumer middleware: deduplication, retry, the
//! failure topic, scheduling, monopolization, deferral, and timeouts.

use crate::util::{decode_duration, option};
use prosody::consumer::middleware::deduplication::DeduplicationConfigurationBuilder;
use prosody::consumer::middleware::defer::DeferConfigurationBuilder;
use prosody::consumer::middleware::monopolization::MonopolizationConfigurationBuilder;
use prosody::consumer::middleware::retry::RetryConfigurationBuilder;
use prosody::consumer::middleware::scheduler::SchedulerConfigurationBuilder;
use prosody::consumer::middleware::timeout::TimeoutConfigurationBuilder;
use prosody::consumer::middleware::topic::FailureTopicConfigurationBuilder;
use pyo3::exceptions::PyValueError;
use pyo3::types::{PyAnyMethods, PyDict};
use pyo3::{Bound, PyResult};
use std::num::NonZeroUsize;

/// Builds the `DeduplicationConfigurationBuilder` from the client options.
pub(super) fn build_dedup_config(
    config: &Bound<PyDict>,
) -> PyResult<DeduplicationConfigurationBuilder> {
    let mut builder = DeduplicationConfigurationBuilder::default();

    if let Some(cache_capacity) = option(config, "idempotence_cache_size")? {
        let capacity = NonZeroUsize::new(cache_capacity.extract::<usize>()?).ok_or_else(|| {
            PyValueError::new_err("idempotence_cache_size must be greater than 0")
        })?;
        builder.cache_capacity(capacity);
    }

    if let Some(version) = option(config, "idempotence_version")? {
        builder.version(version.extract::<String>()?);
    }

    if let Some(ttl) = option(config, "idempotence_ttl")? {
        builder.ttl(decode_duration(&ttl)?);
    }

    Ok(builder)
}

/// Builds the `RetryConfigurationBuilder` from the client options.
pub(super) fn build_retry_config(config: &Bound<PyDict>) -> PyResult<RetryConfigurationBuilder> {
    let mut builder = RetryConfigurationBuilder::default();

    if let Some(retry_base) = option(config, "retry_base")? {
        builder.base(decode_duration(&retry_base)?);
    }

    if let Some(max_retries) = option(config, "max_retries")? {
        builder.max_retries(max_retries.extract::<u32>()?);
    }

    if let Some(retry_max_delay) = option(config, "max_retry_delay")? {
        builder.max_delay(decode_duration(&retry_max_delay)?);
    }

    Ok(builder)
}

/// Builds the `FailureTopicConfigurationBuilder` from the client options.
pub(super) fn build_failure_topic_config(
    config: &Bound<PyDict>,
) -> PyResult<FailureTopicConfigurationBuilder> {
    let mut builder = FailureTopicConfigurationBuilder::default();

    if let Some(topic) = option(config, "failure_topic")? {
        builder.failure_topic(topic.extract::<String>()?);
    }

    Ok(builder)
}

/// Builds the `SchedulerConfigurationBuilder` from the client options.
pub(super) fn build_scheduler_config(
    config: &Bound<PyDict>,
) -> PyResult<SchedulerConfigurationBuilder> {
    let mut builder = SchedulerConfigurationBuilder::default();

    if let Some(max_concurrency) = option(config, "max_concurrency")? {
        builder.max_concurrency(max_concurrency.extract::<usize>()?);
    }

    if let Some(failure_weight) = option(config, "scheduler_failure_weight")? {
        builder.failure_weight(failure_weight.extract::<f64>()?);
    }

    if let Some(max_wait) = option(config, "scheduler_max_wait")? {
        builder.max_wait(decode_duration(&max_wait)?);
    }

    if let Some(wait_weight) = option(config, "scheduler_wait_weight")? {
        builder.wait_weight(wait_weight.extract::<f64>()?);
    }

    if let Some(cache_size) = option(config, "scheduler_cache_size")? {
        builder.cache_size(cache_size.extract::<usize>()?);
    }

    Ok(builder)
}

/// Builds the `MonopolizationConfigurationBuilder` from the client options.
pub(super) fn build_monopolization_config(
    config: &Bound<PyDict>,
) -> PyResult<MonopolizationConfigurationBuilder> {
    let mut builder = MonopolizationConfigurationBuilder::default();

    if let Some(enabled) = option(config, "monopolization_enabled")? {
        builder.enabled(enabled.extract::<bool>()?);
    }

    if let Some(threshold) = option(config, "monopolization_threshold")? {
        builder.monopolization_threshold(threshold.extract::<f64>()?);
    }

    if let Some(window) = option(config, "monopolization_window")? {
        builder.window_duration(decode_duration(&window)?);
    }

    if let Some(cache_size) = option(config, "monopolization_cache_size")? {
        builder.cache_size(cache_size.extract::<usize>()?);
    }

    Ok(builder)
}

/// Builds the `DeferConfigurationBuilder` from the client options.
pub(super) fn build_defer_config(config: &Bound<PyDict>) -> PyResult<DeferConfigurationBuilder> {
    let mut builder = DeferConfigurationBuilder::default();

    if let Some(enabled) = option(config, "defer_enabled")? {
        builder.enabled(enabled.extract::<bool>()?);
    }

    if let Some(base) = option(config, "defer_base")? {
        builder.base(decode_duration(&base)?);
    }

    if let Some(max_delay) = option(config, "defer_max_delay")? {
        builder.max_delay(decode_duration(&max_delay)?);
    }

    if let Some(failure_threshold) = option(config, "defer_failure_threshold")? {
        builder.failure_threshold(failure_threshold.extract::<f64>()?);
    }

    if let Some(failure_window) = option(config, "defer_failure_window")? {
        builder.failure_window(decode_duration(&failure_window)?);
    }

    if let Some(store_cache_size) = option(config, "defer_store_cache_size")? {
        let store_cache_size: usize = store_cache_size.extract()?;
        builder.store_cache_size(store_cache_size);
    }

    Ok(builder)
}

/// Builds the `TimeoutConfigurationBuilder` from the client options.
pub(super) fn build_timeout_config(
    config: &Bound<PyDict>,
) -> PyResult<TimeoutConfigurationBuilder> {
    let mut builder = TimeoutConfigurationBuilder::default();

    if let Some(timeout) = option(config, "timeout")? {
        builder.timeout(Some(decode_duration(&timeout)?));
    }

    Ok(builder)
}
