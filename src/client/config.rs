//! Configuration utilities for the `ProsodyClient`.
//!
//! This module provides functions for building and configuring a
//! `ProsodyClient`, including utilities for parsing Python configuration
//! objects and handling duration conversions.

use crate::client::ProsodyClient;
use crate::util::{decode_duration, decode_optional_duration, string_or_vec};
use prosody::JsonCodec;
use prosody::cassandra::config::CassandraConfigurationBuilder;
use prosody::consumer::ConsumerConfigurationBuilder;
use prosody::consumer::KeyedStateConfiguration;
use prosody::consumer::SpanRelation;
use prosody::consumer::kafka_state::{message_deque_state, message_map_state, message_state};
use prosody::consumer::middleware::deduplication::DeduplicationConfigurationBuilder;
use prosody::consumer::middleware::defer::DeferConfigurationBuilder;
use prosody::consumer::middleware::monopolization::MonopolizationConfigurationBuilder;
use prosody::consumer::middleware::retry::RetryConfigurationBuilder;
use prosody::consumer::middleware::scheduler::SchedulerConfigurationBuilder;
use prosody::consumer::middleware::timeout::TimeoutConfigurationBuilder;
use prosody::consumer::middleware::topic::FailureTopicConfigurationBuilder;
use prosody::high_level::mode::{Mode, ModeError};
use prosody::high_level::{ConsumerBuilders, HighLevelClient};
use prosody::loader::KafkaLoader;
use prosody::loader::KafkaLoaderConfiguration;
use prosody::producer::ProducerConfigurationBuilder;
use prosody::state::descriptor::{
    DequeDescriptor, MapDescriptor, StateDescriptor, deque_state, map_state, value_state,
};
use prosody::state::order_codec::Utf8KeyCodec;
use prosody::telemetry::emitter::TelemetryEmitterConfiguration;
use prosody::timers::duration::CompactDuration;
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::types::{PyAnyMethods, PyBool, PyDict, PyDictMethods};
use pyo3::{Bound, IntoPyObjectExt, PyResult, Python};
use pyo3_async_runtimes::tokio::get_runtime;
use std::collections::HashSet;
use std::num::{NonZeroU64, NonZeroUsize};
use std::path::PathBuf;
use std::process;
use std::sync::Arc;

/// The Cassandra TTL ceiling in seconds (`630_720_000`, twenty years). Core
/// also validates this at consumer build; enforcing it here yields an earlier,
/// field-named error.
const TTL_CEILING_SECONDS: u32 = 630_720_000;

/// The inclusive upper bound for a map keyset limit.
const KEYSET_LIMIT_MAX: u32 = 4096;

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
            dedup: DeduplicationConfigurationBuilder::default(),
            retry: RetryConfigurationBuilder::default(),
            failure_topic: FailureTopicConfigurationBuilder::default(),
            scheduler: SchedulerConfigurationBuilder::default(),
            monopolization: MonopolizationConfigurationBuilder::default(),
            defer: DeferConfigurationBuilder::default(),
            timeout: TimeoutConfigurationBuilder::default(),
            keyed_state: KeyedStateConfiguration::builder()
                .build()
                .map_err(|error| PyValueError::new_err(error.to_string()))?,
            emitter: TelemetryEmitterConfiguration::builder()
                .build()
                .map_err(|e| PyValueError::new_err(e.to_string()))?,
        };

        let _guard = get_runtime().handle().enter();
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
            pid: process::id(),
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

    let _guard = get_runtime().handle().enter();
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
        pid: process::id(),
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

    if let Some(max_uncommitted) = config.get_item("max_uncommitted")? {
        builder.max_uncommitted(max_uncommitted.extract::<usize>()?);
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

    if let Some(message_spans) = config.get_item("message_spans")?
        && !message_spans.is_none()
    {
        let s: String = message_spans.extract()?;
        let relation = s
            .parse::<SpanRelation>()
            .map_err(|e| PyValueError::new_err(format!("message_spans: {e}")))?;
        builder.message_spans(relation);
    }

    if let Some(timer_spans) = config.get_item("timer_spans")?
        && !timer_spans.is_none()
    {
        let s: String = timer_spans.extract()?;
        let relation = s
            .parse::<SpanRelation>()
            .map_err(|e| PyValueError::new_err(format!("timer_spans: {e}")))?;
        builder.timer_spans(relation);
    }

    // Kafka message loader tuning (deferred-retry reload and keyed-state
    // message resolution). Only build a loader configuration if at least one
    // knob is provided, otherwise the consumer keeps its own defaults.
    let defer_cache_size = config.get_item("defer_cache_size")?;
    let defer_seek_timeout = config.get_item("defer_seek_timeout")?;
    let defer_discard_threshold = config.get_item("defer_discard_threshold")?;
    if defer_cache_size.is_some()
        || defer_seek_timeout.is_some()
        || defer_discard_threshold.is_some()
    {
        let mut loader = KafkaLoaderConfiguration::builder();

        if let Some(cache_size) = defer_cache_size {
            loader.cache_size(cache_size.extract::<usize>()?);
        }

        if let Some(seek_timeout) = defer_seek_timeout {
            loader.seek_timeout(decode_duration(&seek_timeout)?);
        }

        if let Some(discard_threshold) = defer_discard_threshold {
            loader.discard_threshold(discard_threshold.extract::<i64>()?);
        }

        let loader = loader
            .build()
            .map_err(|e| PyValueError::new_err(e.to_string()))?;
        builder.loader(loader);
    }

    Ok(builder)
}

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
fn build_dedup_config(config: &Bound<PyDict>) -> PyResult<DeduplicationConfigurationBuilder> {
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
fn build_timeout_config(config: &Bound<PyDict>) -> PyResult<TimeoutConfigurationBuilder> {
    let mut builder = TimeoutConfigurationBuilder::default();

    if let Some(timeout) = config.get_item("timeout")? {
        builder.timeout(Some(decode_duration(&timeout)?));
    }

    Ok(builder)
}

/// Builds a `TelemetryEmitterConfiguration` from the provided Python
/// configuration.
///
/// # Arguments
///
/// * `config` - A Python dictionary containing configuration options.
///
/// # Returns
///
/// A `PyResult` containing the constructed `TelemetryEmitterConfiguration`.
///
/// # Errors
///
/// Returns a `PyErr` if extraction of configuration values fails.
fn build_telemetry_emitter_config(
    config: &Bound<PyDict>,
) -> PyResult<TelemetryEmitterConfiguration> {
    let mut builder = TelemetryEmitterConfiguration::builder();

    if let Some(topic) = config.get_item("telemetry_topic")? {
        builder.topic(topic.extract::<String>()?);
    }

    if let Some(enabled) = config.get_item("telemetry_enabled")? {
        builder.enabled(enabled.extract::<bool>()?);
    }

    builder
        .build()
        .map_err(|e| PyValueError::new_err(e.to_string()))
}

/// The kind of a keyed-state collection.
enum CollectionKind {
    /// A single-value collection.
    Value,
    /// A `String`-keyed ordered map.
    Map,
    /// A deque.
    Deque,
}

/// The item payload of a keyed-state collection.
enum CollectionPayload {
    /// JSON values.
    Json,
    /// The full Kafka message the handler received.
    Message,
}

/// Parses a collection-kind token.
///
/// # Errors
///
/// Returns a `PyValueError` if the token is not `"value"`, `"map"`, or
/// `"deque"`.
fn parse_kind(index: usize, kind: &str) -> PyResult<CollectionKind> {
    match kind {
        "value" => Ok(CollectionKind::Value),
        "map" => Ok(CollectionKind::Map),
        "deque" => Ok(CollectionKind::Deque),
        other => Err(PyValueError::new_err(format!(
            "state_collections[{index}].kind: expected \"value\", \"map\", or \"deque\", got \
             {other:?}"
        ))),
    }
}

/// Parses a collection-payload token.
///
/// # Errors
///
/// Returns a `PyValueError` if the token is not `"json"` or `"message"`.
fn parse_payload(index: usize, payload: &str) -> PyResult<CollectionPayload> {
    match payload {
        "json" => Ok(CollectionPayload::Json),
        "message" => Ok(CollectionPayload::Message),
        other => Err(PyValueError::new_err(format!(
            "state_collections[{index}].payload: expected \"json\" or \"message\", got {other:?}"
        ))),
    }
}

/// Validates a Python number field as a whole number within `min..=max`.
///
/// The field arrives as an `f64` (the raw Python number) so that fractional,
/// negative, and non-finite values reach this guard instead of being silently
/// truncated or wrapped by an earlier integer conversion.
///
/// # Errors
///
/// Returns a `PyValueError` if the value is not a whole number in range.
fn whole_number_field(value: f64, field: &str, min: u32, max: u32) -> PyResult<u32> {
    if value.is_finite()
        && value.fract() == 0.0
        && value >= f64::from(min)
        && value <= f64::from(max)
    {
        Ok(value as u32)
    } else {
        Err(PyValueError::new_err(format!(
            "{field}: must be a whole number in {min}..={max}"
        )))
    }
}

/// Applies the shared descriptor options (TTL, commit mode) fluently.
fn with_def<D: StateDescriptor>(
    descriptor: D,
    ttl_seconds: Option<u32>,
    read_uncommitted: Option<bool>,
) -> D {
    let mut descriptor = descriptor;
    if let Some(ttl) = ttl_seconds {
        descriptor = descriptor.ttl(CompactDuration::new(ttl));
    }
    if read_uncommitted == Some(true) {
        descriptor = descriptor.read_uncommitted();
    }
    descriptor
}

/// Applies the map-only keyset bound when configured.
fn with_keyset<KC, V>(
    descriptor: MapDescriptor<KC, V>,
    keyset_limit: Option<u32>,
) -> MapDescriptor<KC, V> {
    match keyset_limit {
        Some(limit) => descriptor.keyset_limit(limit as usize),
        None => descriptor,
    }
}

/// Applies the deque-only push capacity when configured.
///
/// No bound on `T`: core's `capacity` builder is an unconstrained inherent
/// method on the deque descriptor. Capacity is runtime-only — never persisted,
/// not part of identity — so it is applied at registration alongside the shared
/// descriptor options and enforced lazily on push (see the deque docs).
fn with_capacity<T>(
    descriptor: DequeDescriptor<T>,
    capacity: Option<NonZeroUsize>,
) -> DequeDescriptor<T> {
    match capacity {
        Some(c) => descriptor.capacity(c),
        None => descriptor,
    }
}

/// Reads a required non-empty string field from a collection's config dict.
///
/// # Errors
///
/// Returns a `PyValueError` if the field is missing/None, or a `PyErr` if the
/// value is not a string.
fn required_str(cfg: &Bound<PyDict>, index: usize, field: &str) -> PyResult<String> {
    match cfg.get_item(field)? {
        Some(value) if !value.is_none() => value.extract::<String>(),
        _ => Err(PyValueError::new_err(format!(
            "state_collections[{index}].{field}: missing"
        ))),
    }
}

/// Reads an optional `f64` field from a collection's config dict (None when
/// absent or `None`).
fn optional_f64(cfg: &Bound<PyDict>, field: &str) -> PyResult<Option<f64>> {
    match cfg.get_item(field)? {
        Some(value) if !value.is_none() => Ok(Some(value.extract::<f64>()?)),
        _ => Ok(None),
    }
}

/// Parses the deque-only `capacity` field into a `NonZeroUsize` push bound.
///
/// Rejects a capacity on a non-deque collection and validates the value as a
/// whole number in `1..=u32::MAX`. That ceiling is a guardrail, not a real cap:
/// at ~100 B/slot it is ~400 GiB, far past the per-instance memory budget.
/// Every client validates a host int against this same core `NonZeroUsize`
/// primitive — equal power, only host-int overhead differs.
///
/// # Errors
///
/// Returns a `PyValueError` naming the offending field.
fn parse_capacity(
    cfg: &Bound<PyDict>,
    index: usize,
    kind: &CollectionKind,
) -> PyResult<Option<NonZeroUsize>> {
    let Some(value) = optional_f64(cfg, "capacity")? else {
        return Ok(None);
    };
    if !matches!(kind, CollectionKind::Deque) {
        return Err(PyValueError::new_err(format!(
            "state_collections[{index}].capacity: only valid for deque collections"
        )));
    }
    let n = whole_number_field(
        value,
        &format!("state_collections[{index}].capacity"),
        1,
        u32::MAX,
    )?;
    // `n >= 1`, so `NonZeroUsize::new` is always `Some`; the `None` arm is
    // unreachable but keeps the conversion panic-free (`unwrap` is denied).
    match NonZeroUsize::new(n as usize) {
        Some(nz) => Ok(Some(nz)),
        None => Err(PyValueError::new_err(format!(
            "state_collections[{index}].capacity: must be a whole number in 1..={}",
            u32::MAX
        ))),
    }
}

/// Reads an optional `bool` field from a collection's config dict.
fn optional_bool(cfg: &Bound<PyDict>, field: &str) -> PyResult<Option<bool>> {
    match cfg.get_item(field)? {
        Some(value) if !value.is_none() => Ok(Some(value.extract::<bool>()?)),
        _ => Ok(None),
    }
}

/// Validates one collection's config dict and registers its descriptor.
///
/// The config dict is produced by the definition's `to_config()` method with
/// keys `name`, `kind`, `payload`, `ttl_seconds`, `read_uncommitted`,
/// `keyset_limit` (map-only), and `capacity` (deque-only). Field-level
/// validation names the offending field; core validates the remaining rules
/// (TTL exceeding the recovery delay, identity conflicts) at consumer build.
///
/// # Errors
///
/// Returns a `PyValueError` if a field is invalid (the field name is named in
/// the message).
fn register_state_collection(
    keyed: &mut KeyedStateConfiguration,
    index: usize,
    cfg: &Bound<PyDict>,
) -> PyResult<()> {
    let name = required_str(cfg, index, "name")?;
    if name.is_empty() {
        return Err(PyValueError::new_err(format!(
            "state_collections[{index}].name: must not be empty"
        )));
    }

    let kind = parse_kind(index, &required_str(cfg, index, "kind")?)?;
    let payload = parse_payload(index, &required_str(cfg, index, "payload")?)?;

    let ttl_seconds = match optional_f64(cfg, "ttl_seconds")? {
        Some(value) => Some(whole_number_field(
            value,
            &format!("state_collections[{index}].ttl_seconds"),
            1,
            TTL_CEILING_SECONDS,
        )?),
        None => None,
    };

    let keyset_limit = match optional_f64(cfg, "keyset_limit")? {
        Some(value) => {
            if !matches!(kind, CollectionKind::Map) {
                return Err(PyValueError::new_err(format!(
                    "state_collections[{index}].keyset_limit: only valid for map collections"
                )));
            }
            Some(whole_number_field(
                value,
                &format!("state_collections[{index}].keyset_limit"),
                0,
                KEYSET_LIMIT_MAX,
            )?)
        }
        None => None,
    };

    let capacity = parse_capacity(cfg, index, &kind)?;

    let read_uncommitted = optional_bool(cfg, "read_uncommitted")?;
    let name = name.as_str();
    match (kind, payload) {
        (CollectionKind::Value, CollectionPayload::Json) => {
            let _ = keyed.register(with_def(
                value_state::<JsonCodec>(name),
                ttl_seconds,
                read_uncommitted,
            ));
        }
        (CollectionKind::Map, CollectionPayload::Json) => {
            let descriptor = with_def(
                map_state::<Utf8KeyCodec, JsonCodec>(name),
                ttl_seconds,
                read_uncommitted,
            );
            let _ = keyed.register(with_keyset(descriptor, keyset_limit));
        }
        (CollectionKind::Deque, CollectionPayload::Json) => {
            let descriptor = with_def(
                deque_state::<JsonCodec>(name),
                ttl_seconds,
                read_uncommitted,
            );
            let _ = keyed.register(with_capacity(descriptor, capacity));
        }
        (CollectionKind::Value, CollectionPayload::Message) => {
            let _ = keyed.register(with_def(
                message_state::<KafkaLoader<JsonCodec>>(name),
                ttl_seconds,
                read_uncommitted,
            ));
        }
        (CollectionKind::Map, CollectionPayload::Message) => {
            let descriptor = with_def(
                message_map_state::<Utf8KeyCodec, KafkaLoader<JsonCodec>>(name),
                ttl_seconds,
                read_uncommitted,
            );
            let _ = keyed.register(with_keyset(descriptor, keyset_limit));
        }
        (CollectionKind::Deque, CollectionPayload::Message) => {
            let descriptor = with_def(
                message_deque_state::<KafkaLoader<JsonCodec>>(name),
                ttl_seconds,
                read_uncommitted,
            );
            let _ = keyed.register(with_capacity(descriptor, capacity));
        }
    }

    Ok(())
}

/// Builds the `KeyedStateConfiguration` from the provided Python configuration.
///
/// Reads the keyed-state cache, recovery, and collection settings, registering
/// every declared collection synchronously and rejecting duplicate names.
///
/// # Errors
///
/// Returns a `PyValueError` if a keyed-state field is invalid or a collection
/// name is duplicated.
fn build_keyed_state_config(config: &Bound<PyDict>) -> PyResult<KeyedStateConfiguration> {
    let mut builder = KeyedStateConfiguration::builder();

    if let Some(dir) = config.get_item("state_cache_dir")?
        && !dir.is_none()
    {
        let dir: String = dir.extract()?;
        if dir.is_empty() {
            return Err(PyValueError::new_err(
                "state_cache_dir: must not be an empty string",
            ));
        }
        builder.cache_dir(PathBuf::from(dir));
    }

    if let Some(delay) = config.get_item("state_recovery_delay")?
        && !delay.is_none()
    {
        let duration = decode_duration(&delay).map_err(|error| {
            PyValueError::new_err(format!("state_recovery_delay: {}", error.value(delay.py())))
        })?;
        if duration.subsec_nanos() != 0 {
            return Err(PyValueError::new_err(
                "state_recovery_delay: must be a whole number of seconds",
            ));
        }
        let seconds = u32::try_from(duration.as_secs()).map_err(|_| {
            PyValueError::new_err("state_recovery_delay: exceeds the u32 seconds range")
        })?;
        if seconds < 1 {
            return Err(PyValueError::new_err(
                "state_recovery_delay: must be >= 1 second",
            ));
        }
        builder.recovery_delay(CompactDuration::new(seconds));
    }

    if let Some(bytes) = config.get_item("state_cache_size_bytes")?
        && !bytes.is_none()
    {
        if bytes.is_instance_of::<PyBool>() {
            return Err(PyValueError::new_err(
                "state_cache_size_bytes: must be an integer in 1..=18446744073709551615",
            ));
        }
        let bytes: u64 = bytes.extract().map_err(|_| {
            PyValueError::new_err(
                "state_cache_size_bytes: must be an integer in 1..=18446744073709551615",
            )
        })?;
        let bytes = NonZeroU64::new(bytes).ok_or_else(|| {
            PyValueError::new_err("state_cache_size_bytes: must be greater than 0")
        })?;
        builder.cache_size_bytes(Some(bytes));
    }

    let mut keyed = builder
        .build()
        .map_err(|error| PyValueError::new_err(error.to_string()))?;

    if let Some(collections) = config.get_item("state_collections")?
        && !collections.is_none()
    {
        let mut seen: HashSet<String> = HashSet::new();
        for (index, entry) in collections.try_iter()?.enumerate() {
            let entry = entry?;
            let cfg = entry.call_method0("to_config")?;
            let cfg = cfg.cast::<PyDict>().map_err(|_| {
                PyValueError::new_err(format!(
                    "state_collections[{index}]: to_config() must return a dict"
                ))
            })?;
            let name = required_str(cfg, index, "name")?;
            if !seen.insert(name.clone()) {
                return Err(PyValueError::new_err(format!(
                    "state_collections[{index}].name: duplicate collection name {name:?}"
                )));
            }
            register_state_collection(&mut keyed, index, cfg)?;
        }
    }

    Ok(keyed)
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
        dedup: build_dedup_config(config)?,
        retry: build_retry_config(config)?,
        failure_topic: build_failure_topic_config(config)?,
        scheduler: build_scheduler_config(config)?,
        monopolization: build_monopolization_config(config)?,
        defer: build_defer_config(config)?,
        timeout: build_timeout_config(config)?,
        keyed_state: build_keyed_state_config(config)?,
        emitter: build_telemetry_emitter_config(config)?,
    })
}
