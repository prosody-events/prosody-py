//! Keyed-state configuration: the cache settings and the registered
//! collections.

use super::collection::register_state_collection;
use crate::util::{decode_duration, option};
use prosody::ByteSize;
use prosody::consumer::KeyedStateConfiguration;
use prosody::subsystem::SubsystemName;
use pyo3::exceptions::PyValueError;
use pyo3::types::{PyAnyMethods, PyBool, PyDict};
use pyo3::{Bound, PyResult};
use std::path::PathBuf;
use std::time::Duration;

/// Builds the `KeyedStateConfiguration` from the provided Python configuration.
///
/// Reads the keyed-state cache and collection settings. It maps
/// every definition into a Prosody descriptor. The normal Prosody construction
/// path validates the resulting configuration.
///
/// # Errors
///
/// Returns a `PyValueError` if a host value cannot be mapped.
pub(super) fn build_keyed_state_config(
    config: &Bound<PyDict>,
) -> PyResult<KeyedStateConfiguration> {
    let mut builder = KeyedStateConfiguration::builder();

    if let Some(dir) = option(config, "state_cache_dir")? {
        let dir: String = dir.extract()?;
        builder.cache_dir(PathBuf::from(dir));
    }

    if let Some(subsystem) = option(config, "subsystem")? {
        let subsystem: String = subsystem.extract()?;
        builder.subsystem(Some(
            SubsystemName::try_new(subsystem)
                .map_err(|error| PyValueError::new_err(error.to_string()))?,
        ));
    }

    if let Some(size) = optional_byte_size(config, "state_owned_cache_size")? {
        builder.owned_cache_size(Some(size));
    }
    if let Some(size) = optional_byte_size(config, "state_memtable_size")? {
        builder.memtable_size(Some(size));
    }
    if let Some(size) = optional_byte_size(config, "state_read_cache_size")? {
        builder.read_cache_size(Some(size));
    }

    match read_cache_ttl(config)? {
        ReadCacheTtl::Inherit => {}
        ReadCacheTtl::Disabled => {
            builder.read_cache_ttl(None);
        }
        ReadCacheTtl::Ttl(ttl) => {
            builder.read_cache_ttl(Some(ttl));
        }
    }

    let mut keyed = builder
        .build()
        .map_err(|error| PyValueError::new_err(error.to_string()))?;

    if let Some(collections) = option(config, "state_collections")? {
        for (index, entry) in collections.try_iter()?.enumerate() {
            let entry = entry?;
            let cfg = entry.call_method0("to_config")?;
            let cfg = cfg.cast::<PyDict>().map_err(|_| {
                PyValueError::new_err(format!(
                    "state_collections[{index}]: to_config() must return a dict"
                ))
            })?;
            register_state_collection(&mut keyed, index, cfg)?;
        }
    }

    Ok(keyed)
}

enum ReadCacheTtl {
    Inherit,
    Disabled,
    Ttl(Duration),
}

fn read_cache_ttl(config: &Bound<PyDict>) -> PyResult<ReadCacheTtl> {
    let Some(cache) = option(config, "state_read_cache")? else {
        return Ok(ReadCacheTtl::Inherit);
    };
    if cache.is_instance_of::<PyBool>() {
        if cache.is_truthy()? {
            return Err(PyValueError::new_err(
                "state_read_cache: True is ambiguous; use a duration or False",
            ));
        }
        return Ok(ReadCacheTtl::Disabled);
    }
    let duration = decode_duration(&cache).map_err(|error| {
        PyValueError::new_err(format!("state_read_cache: {}", error.value(cache.py())))
    })?;
    Ok(ReadCacheTtl::Ttl(duration))
}

/// Reads an optional byte size, such as `"64 MiB"`, from the client config.
///
/// Returns `None` when the field is absent or `None`, so the core default
/// and its environment variable still apply.
///
/// # Errors
///
/// Returns a `PyValueError` naming `field` if the value is not a size string.
fn optional_byte_size(config: &Bound<PyDict>, field: &str) -> PyResult<Option<ByteSize>> {
    let Some(size) = option(config, field)? else {
        return Ok(None);
    };
    let size: String = size
        .extract()
        .map_err(|_| PyValueError::new_err(format!("{field}: must be a size string")))?;
    size.parse::<ByteSize>()
        .map(Some)
        .map_err(|error| PyValueError::new_err(format!("{field}: {error}")))
}
