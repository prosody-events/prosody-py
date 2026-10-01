//! Configuration of the `ProsodyClient` from Python keyword arguments.
//!
//! This module reads the producer, consumer, Cassandra, telemetry, and peer
//! settings. The `middleware` module reads the middleware settings, and the
//! `state` module reads the keyed-state settings.

use crate::client::ProsodyClient;
use crate::state::StateEnv;
use crate::util::{decode_duration, decode_optional_duration, option, string_or_vec};
use middleware::{
    build_dedup_config, build_defer_config, build_failure_topic_config,
    build_monopolization_config, build_retry_config, build_scheduler_config, build_timeout_config,
};
use prosody::PeerConfiguration;
use prosody::PeerEndpoint;
use prosody::cassandra::config::CassandraConfigurationBuilder;
use prosody::consumer::ConsumerConfigurationBuilder;
use prosody::consumer::SpanRelation;
use prosody::high_level::ConsumerBuilders;
use prosody::high_level::erased::new_erased;
use prosody::high_level::mode::{Mode, ModeError};
use prosody::loader::KafkaLoaderConfiguration;
use prosody::producer::ProducerConfigurationBuilder;
use prosody::propagator::new_propagator;
use prosody::telemetry::emitter::TelemetryEmitterConfiguration;
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::types::{PyAnyMethods, PyDict, PyDictMethods};
use pyo3::{Bound, PyResult, Python};
use state::build_keyed_state_config;
use std::net::SocketAddr;
use std::process;
use std::sync::Arc;

mod collection;
mod middleware;
mod state;

/// The parsed client options, ready to connect.
pub struct PreparedClient {
    mode: Mode,
    producer: ProducerConfigurationBuilder,
    consumer: ConsumerBuilders,
    cassandra: CassandraConfigurationBuilder,
    env: StateEnv,
}

impl PreparedClient {
    pub async fn connect(mut self) -> PyResult<ProsodyClient> {
        let client = Box::pin(new_erased(
            self.mode,
            &mut self.producer,
            &self.consumer,
            &self.cassandra,
        ))
        .await
        .map_err(|error| PyRuntimeError::new_err(error.to_string()))?;

        Ok(ProsodyClient {
            shutdown: super::shutdown(&client),
            client,
            env: self.env,
            handler: Arc::new(parking_lot::Mutex::new(None)),
            pid: process::id(),
        })
    }
}

/// Parses the client keyword options into a [`PreparedClient`].
///
/// # Errors
///
/// Returns a `PyValueError` if an option is invalid.
pub fn prepare_config(py: Python, config: Option<&Bound<PyDict>>) -> PyResult<PreparedClient> {
    let env = StateEnv::resolve(py, Arc::new(new_propagator()))?;

    let config = config.cloned().unwrap_or_else(|| PyDict::new(py));
    let config = &config;

    // Extract and set configuration options
    let mode = match option(config, "mode")? {
        Some(mode_str) => mode_str
            .extract::<String>()?
            .parse()
            .map_err(|e: ModeError| PyValueError::new_err(e.to_string()))?,

        None => Mode::default(),
    };

    Ok(PreparedClient {
        mode,
        producer: build_producer_config(config)?,
        consumer: build_consumer_builders(config)?,
        cassandra: build_cassandra_config(config)?,
        env,
    })
}

/// Builds the `ProducerConfigurationBuilder` from the client options.
fn build_producer_config(config: &Bound<PyDict>) -> PyResult<ProducerConfigurationBuilder> {
    let mut builder = ProducerConfigurationBuilder::default();

    if let Some(bootstrap) = option(config, "bootstrap_servers")? {
        builder.bootstrap_servers(string_or_vec(&bootstrap)?);
    }

    if let Some(mock) = option(config, "mock")? {
        builder.mock(mock.extract::<bool>()?);
    }

    if let Some(source_system) = option(config, "source_system")? {
        builder.source_system(source_system.extract::<String>()?);
    }

    if let Some(send_timeout) = config.get_item("send_timeout")? {
        builder.send_timeout(decode_optional_duration(&send_timeout)?);
    }

    if let Some(cache_size) = option(config, "idempotence_cache_size")? {
        builder.idempotence_cache_size(cache_size.extract::<usize>()?);
    }

    Ok(builder)
}

/// Builds the `ConsumerConfigurationBuilder` from the client options.
fn build_consumer_config(config: &Bound<PyDict>) -> PyResult<ConsumerConfigurationBuilder> {
    let mut builder = ConsumerConfigurationBuilder::default();

    if let Some(bootstrap) = option(config, "bootstrap_servers")? {
        builder.bootstrap_servers(string_or_vec(&bootstrap)?);
    }

    if let Some(mock) = option(config, "mock")? {
        builder.mock(mock.extract::<bool>()?);
    }

    if let Some(group_id) = option(config, "group_id")? {
        builder.group_id(group_id.extract::<String>()?);
    }

    if let Some(subscribed_topics) = option(config, "subscribed_topics")? {
        builder.subscribed_topics(string_or_vec(&subscribed_topics)?);
    }

    if let Some(allowed_event_types) = option(config, "allowed_events")? {
        builder.allowed_events(string_or_vec(&allowed_event_types)?);
    }

    if let Some(max_uncommitted) = option(config, "max_uncommitted")? {
        builder.max_uncommitted(max_uncommitted.extract::<usize>()?);
    }

    if let Some(value) = option(config, "stall_threshold")? {
        builder.stall_threshold(decode_duration(&value)?);
    }

    if let Some(value) = option(config, "shutdown_timeout")? {
        builder.shutdown_timeout(decode_duration(&value)?);
    }

    if let Some(poll_interval) = option(config, "poll_interval")? {
        builder.poll_interval(decode_duration(&poll_interval)?);
    }

    if let Some(commit_interval) = option(config, "commit_interval")? {
        builder.commit_interval(decode_duration(&commit_interval)?);
    }

    if let Some(statistics_interval) = option(config, "statistics_interval")? {
        builder.statistics_interval(decode_duration(&statistics_interval)?);
    }

    // An explicit `None` turns the probe server off, so only this option
    // reads `None` as a value.
    if let Some(probe_port) = config.get_item("probe_port")? {
        builder.probe_port(probe_port.extract::<Option<u16>>()?);
    }

    if let Some(slab_size) = option(config, "slab_size")? {
        builder.slab_size(decode_duration(&slab_size)?);
    }

    if let Some(message_spans) = option(config, "message_spans")? {
        let s: String = message_spans.extract()?;
        let relation = s
            .parse::<SpanRelation>()
            .map_err(|e| PyValueError::new_err(format!("message_spans: {e}")))?;
        builder.message_spans(relation);
    }

    if let Some(timer_spans) = option(config, "timer_spans")? {
        let s: String = timer_spans.extract()?;
        let relation = s
            .parse::<SpanRelation>()
            .map_err(|e| PyValueError::new_err(format!("timer_spans: {e}")))?;
        builder.timer_spans(relation);
    }

    // Kafka message loader tuning (deferred-retry reload and keyed-state
    // message resolution). Only build a loader configuration if at least one
    // knob is provided, otherwise the consumer keeps its own defaults.
    let loader_cache_size = option(config, "loader_cache_size")?;
    let loader_seek_timeout = option(config, "loader_seek_timeout")?;
    let loader_discard_threshold = option(config, "loader_discard_threshold")?;
    if loader_cache_size.is_some()
        || loader_seek_timeout.is_some()
        || loader_discard_threshold.is_some()
    {
        let mut loader = KafkaLoaderConfiguration::builder();

        if let Some(cache_size) = loader_cache_size {
            loader.cache_size(cache_size.extract::<usize>()?);
        }

        if let Some(seek_timeout) = loader_seek_timeout {
            loader.seek_timeout(decode_duration(&seek_timeout)?);
        }

        if let Some(discard_threshold) = loader_discard_threshold {
            loader.discard_threshold(discard_threshold.extract::<i64>()?);
        }

        let loader = loader
            .build()
            .map_err(|e| PyValueError::new_err(e.to_string()))?;
        builder.loader(loader);
    }

    Ok(builder)
}

/// Builds the `CassandraConfigurationBuilder` from the client options.
fn build_cassandra_config(config: &Bound<PyDict>) -> PyResult<CassandraConfigurationBuilder> {
    let mut builder = CassandraConfigurationBuilder::default();

    // Cassandra nodes (optional, uses environment variable if not provided)
    if let Some(nodes) = option(config, "cassandra_nodes")? {
        builder.nodes(string_or_vec(&nodes)?);
    }

    // Cassandra keyspace (optional, defaults to "prosody")
    if let Some(keyspace) = option(config, "cassandra_keyspace")? {
        builder.keyspace(keyspace.extract::<String>()?);
    }

    // Cassandra datacenter (optional)
    if let Some(datacenter) = option(config, "cassandra_datacenter")? {
        builder.datacenter(Some(datacenter.extract::<String>()?));
    }

    // Cassandra rack (optional)
    if let Some(rack) = option(config, "cassandra_rack")? {
        builder.rack(Some(rack.extract::<String>()?));
    }

    // Cassandra user (optional)
    if let Some(user) = option(config, "cassandra_user")? {
        builder.user(Some(user.extract::<String>()?));
    }

    // Cassandra password (optional)
    if let Some(password) = option(config, "cassandra_password")? {
        builder.password(Some(password.extract::<String>()?));
    }

    // Cassandra retention (optional, defaults to 30 days)
    if let Some(retention) = option(config, "cassandra_retention")? {
        builder.retention(decode_duration(&retention)?);
    }

    Ok(builder)
}

/// Builds the `TelemetryEmitterConfiguration` from the client options.
fn build_telemetry_emitter_config(
    config: &Bound<PyDict>,
) -> PyResult<TelemetryEmitterConfiguration> {
    let mut builder = TelemetryEmitterConfiguration::builder();

    if let Some(topic) = option(config, "telemetry_topic")? {
        builder.topic(topic.extract::<String>()?);
    }

    if let Some(enabled) = option(config, "telemetry_enabled")? {
        builder.enabled(enabled.extract::<bool>()?);
    }

    builder
        .build()
        .map_err(|e| PyValueError::new_err(e.to_string()))
}

/// Builds the `ConsumerBuilders` from the client options.
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
        peer: build_peer_config(config)?,
    })
}

fn build_peer_config(config: &Bound<PyDict>) -> PyResult<PeerConfiguration> {
    let mut builder = PeerConfiguration::builder();
    if let Some(value) = option(config, "peer_bind_address")? {
        builder.bind_address(
            value
                .extract::<String>()?
                .parse::<SocketAddr>()
                .map_err(|error| PyValueError::new_err(format!("peer_bind_address: {error}")))?,
        );
    }
    if let Some(value) = option(config, "peer_advertised_connect")? {
        builder.advertised_connect(
            PeerEndpoint::try_from(value.extract::<String>()?).map_err(|error| {
                PyValueError::new_err(format!("peer_advertised_connect: {error}"))
            })?,
        );
    }
    if let Some(value) = option(config, "peer_network_name")? {
        builder.network_name(value.extract::<String>()?);
    }
    if let Some(value) = option(config, "peer_cache_capacity")? {
        builder.peer_cache_capacity(value.extract::<usize>()?);
    }
    if let Some(value) = option(config, "peer_registration_ttl")? {
        builder.registration_ttl(decode_duration(&value)?);
    }
    builder
        .build()
        .map_err(|error| PyValueError::new_err(error.to_string()))
}
