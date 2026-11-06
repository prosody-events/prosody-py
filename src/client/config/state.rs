//! Keyed-state configuration: the cache settings and the registered
//! collections.

use super::collection::register_state_collection;
use crate::util::{option, optional_byte_size, parse_read_cache};
use prosody::consumer::KeyedStateConfiguration;
use prosody::high_level::erased::ErasedReadCache;
use prosody::subsystem::SubsystemName;
use pyo3::exceptions::PyValueError;
use pyo3::types::{PyAnyMethods, PyDict};
use pyo3::{Bound, PyResult};
use std::path::PathBuf;

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

    // Every keyed-state option error is a `ValueError`, also a wrong type.
    let cache = option(config, "state_read_cache")?;
    let cache = parse_read_cache("state_read_cache", cache.as_ref())
        .map_err(|error| PyValueError::new_err(error.value(config.py()).to_string()))?;
    match cache {
        ErasedReadCache::Inherit => {}
        ErasedReadCache::Disabled => {
            builder.read_cache_ttl(None);
        }
        ErasedReadCache::Ttl(ttl) => {
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
