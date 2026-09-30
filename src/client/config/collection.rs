//! Parses one `state_collections` entry and registers it as a Prosody
//! descriptor.

use crate::util::option;
use prosody::JsonCodec;
use prosody::consumer::KeyedStateConfiguration;
use prosody::consumer::kafka_state::{message_deque_state, message_map_state, message_state};
use prosody::loader::KafkaLoader;
use prosody::state::descriptor::{
    DequeDescriptor, MapDescriptor, StateDescriptor, deque_state, map_state, set_state, value_state,
};
use prosody::state::order_codec::Utf8KeyCodec;
use prosody::timers::duration::CompactDuration;
use pyo3::exceptions::PyValueError;
use pyo3::types::{PyAnyMethods, PyDict};
use pyo3::{Bound, FromPyObject, PyResult};
use std::num::NonZeroUsize;

/// The kind of a keyed-state collection.
enum CollectionKind {
    /// A single-value collection.
    Value,
    /// A `String`-keyed ordered map.
    Map,
    /// A presence-only ordered set of `String` members.
    Set,
    /// A deque.
    Deque,
}

/// The item payload of a value, map, or deque collection. A set has none.
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
/// Returns a `PyValueError` if the token is not `"value"`, `"map"`, `"set"`,
/// or `"deque"`.
fn parse_kind(index: usize, kind: &str) -> PyResult<CollectionKind> {
    match kind {
        "value" => Ok(CollectionKind::Value),
        "map" => Ok(CollectionKind::Map),
        "set" => Ok(CollectionKind::Set),
        "deque" => Ok(CollectionKind::Deque),
        other => Err(PyValueError::new_err(format!(
            "state_collections[{index}].kind: expected \"value\", \"map\", \"set\", or \"deque\", \
             got {other:?}"
        ))),
    }
}

/// Parses the optional collection-payload token.
///
/// # Errors
///
/// Returns a `PyValueError` if the token is not `"json"` or `"message"`.
fn parse_payload(cfg: &Bound<PyDict>, index: usize) -> PyResult<Option<CollectionPayload>> {
    let Some(payload) = option(cfg, "payload")? else {
        return Ok(None);
    };
    match payload.extract::<String>()?.as_str() {
        "json" => Ok(Some(CollectionPayload::Json)),
        "message" => Ok(Some(CollectionPayload::Message)),
        other => Err(PyValueError::new_err(format!(
            "state_collections[{index}].payload: expected \"json\" or \"message\", got {other:?}"
        ))),
    }
}

/// Applies the shared descriptor options (TTL, commit mode) fluently.
fn with_def<D: StateDescriptor>(
    descriptor: D,
    ttl_seconds: Option<u32>,
    read_uncommitted: Option<bool>,
    published: Option<bool>,
) -> D {
    let mut descriptor = descriptor;
    if let Some(ttl) = ttl_seconds {
        descriptor = descriptor.ttl(CompactDuration::new(ttl));
    }
    if read_uncommitted == Some(true) {
        descriptor = descriptor.read_uncommitted();
    }
    if let Some(published) = published {
        descriptor = descriptor.published(published);
    }
    descriptor
}

/// Applies the keyset bound to a map descriptor when configured.
fn with_keyset<KC, V>(
    descriptor: MapDescriptor<KC, V>,
    keyset_limit: Option<usize>,
) -> MapDescriptor<KC, V> {
    match keyset_limit {
        Some(limit) => descriptor.keyset_limit(limit),
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
    match option(cfg, field)? {
        Some(value) => value.extract::<String>(),
        None => Err(PyValueError::new_err(format!(
            "state_collections[{index}].{field}: missing"
        ))),
    }
}

/// Reads an optional whole-number field from a collection's config dict.
///
/// The host integer conversion rejects a float, a negative value, and a value
/// out of range for `T`, so no value is truncated.
///
/// # Errors
///
/// Returns a `PyValueError` that names the field and states `rule`.
fn whole_number<'py, T>(
    cfg: &Bound<'py, PyDict>,
    index: usize,
    field: &str,
    rule: &str,
) -> PyResult<Option<T>>
where
    T: for<'a> FromPyObject<'a, 'py>,
{
    let Some(value) = option(cfg, field)? else {
        return Ok(None);
    };
    value.extract().map(Some).map_err(|_| {
        PyValueError::new_err(format!(
            "state_collections[{index}].{field}: must be {rule}"
        ))
    })
}

/// Parses the deque-only `capacity` field into a push bound.
///
/// # Errors
///
/// Returns a `PyValueError` naming the offending field.
fn parse_capacity(
    cfg: &Bound<PyDict>,
    index: usize,
    kind: &CollectionKind,
) -> PyResult<Option<NonZeroUsize>> {
    let capacity = whole_number(cfg, index, "capacity", "a positive whole number")?;
    if capacity.is_some() && !matches!(kind, CollectionKind::Deque) {
        return Err(PyValueError::new_err(format!(
            "state_collections[{index}].capacity: only valid for deque collections"
        )));
    }
    Ok(capacity)
}

/// Parses the `keyset_limit` field that only maps and sets accept.
///
/// # Errors
///
/// Returns a `PyValueError` naming the offending field.
fn parse_keyset_limit(
    cfg: &Bound<PyDict>,
    index: usize,
    kind: &CollectionKind,
) -> PyResult<Option<usize>> {
    let limit = whole_number(cfg, index, "keyset_limit", "a non-negative whole number")?;
    if limit.is_some() && !matches!(kind, CollectionKind::Map | CollectionKind::Set) {
        return Err(PyValueError::new_err(format!(
            "state_collections[{index}].keyset_limit: only valid for map and set collections"
        )));
    }
    Ok(limit)
}

/// Reads an optional `bool` field from a collection's config dict.
fn optional_bool(cfg: &Bound<PyDict>, field: &str) -> PyResult<Option<bool>> {
    option(cfg, field)?
        .map(|value| value.extract::<bool>())
        .transpose()
}

/// Validates one collection's config dict and registers its descriptor.
///
/// The config dict is produced by the definition's `to_config()` method with
/// keys `name`, `kind`, `payload` (absent for a set), `ttl_seconds`,
/// `read_uncommitted`, `keyset_limit` (map and set only), and `capacity`
/// (deque-only). This
/// function checks only whether host values map into the corresponding Prosody
/// types.
///
/// # Errors
///
/// Returns a `PyValueError` if a field is invalid (the field name is named in
/// the message).
pub(super) fn register_state_collection(
    keyed: &mut KeyedStateConfiguration,
    index: usize,
    cfg: &Bound<PyDict>,
) -> PyResult<()> {
    let name = required_str(cfg, index, "name")?;
    let kind = parse_kind(index, &required_str(cfg, index, "kind")?)?;
    let payload = parse_payload(cfg, index)?;

    let ttl_seconds = whole_number(cfg, index, "ttl_seconds", "a whole number of seconds")?;

    let keyset_limit = parse_keyset_limit(cfg, index, &kind)?;
    let capacity = parse_capacity(cfg, index, &kind)?;

    let read_uncommitted = optional_bool(cfg, "read_uncommitted")?;
    let published = optional_bool(cfg, "published")?;
    let name = name.as_str();
    match (kind, payload) {
        (CollectionKind::Value, Some(CollectionPayload::Json)) => {
            let _ = keyed.register(with_def(
                value_state::<JsonCodec>(name),
                ttl_seconds,
                read_uncommitted,
                published,
            ));
        }
        (CollectionKind::Map, Some(CollectionPayload::Json)) => {
            let descriptor = with_def(
                map_state::<Utf8KeyCodec, JsonCodec>(name),
                ttl_seconds,
                read_uncommitted,
                published,
            );
            let _ = keyed.register(with_keyset(descriptor, keyset_limit));
        }
        (CollectionKind::Set, None) => {
            let descriptor = with_def(
                set_state::<Utf8KeyCodec>(name),
                ttl_seconds,
                read_uncommitted,
                published,
            );
            let _ = keyed.register(match keyset_limit {
                Some(limit) => descriptor.keyset_limit(limit),
                None => descriptor,
            });
        }
        (CollectionKind::Set, Some(_)) => {
            return Err(PyValueError::new_err(format!(
                "state_collections[{index}].payload: a set collection takes no payload"
            )));
        }
        (_, None) => {
            return Err(PyValueError::new_err(format!(
                "state_collections[{index}].payload: missing"
            )));
        }
        (CollectionKind::Deque, Some(CollectionPayload::Json)) => {
            let descriptor = with_def(
                deque_state::<JsonCodec>(name),
                ttl_seconds,
                read_uncommitted,
                published,
            );
            let _ = keyed.register(with_capacity(descriptor, capacity));
        }
        (CollectionKind::Value, Some(CollectionPayload::Message)) => {
            let _ = keyed.register(with_def(
                message_state::<KafkaLoader<JsonCodec>>(name),
                ttl_seconds,
                read_uncommitted,
                published,
            ));
        }
        (CollectionKind::Map, Some(CollectionPayload::Message)) => {
            let descriptor = with_def(
                message_map_state::<Utf8KeyCodec, KafkaLoader<JsonCodec>>(name),
                ttl_seconds,
                read_uncommitted,
                published,
            );
            let _ = keyed.register(with_keyset(descriptor, keyset_limit));
        }
        (CollectionKind::Deque, Some(CollectionPayload::Message)) => {
            let descriptor = with_def(
                message_deque_state::<KafkaLoader<JsonCodec>>(name),
                ttl_seconds,
                read_uncommitted,
                published,
            );
            let _ = keyed.register(with_capacity(descriptor, capacity));
        }
    }

    Ok(())
}
