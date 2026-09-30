//! Tests for the option spellings that read `None` as a value.

use super::build_producer_config;
use pyo3::types::{PyDict, PyDictMethods};
use pyo3::{PyResult, Python};
use std::time::Duration;

/// `send_timeout=None` reaches core as no timeout, and an omitted option
/// leaves the core default of 1 second.
#[test]
fn send_timeout_none_reaches_core_as_no_timeout() -> PyResult<()> {
    Python::initialize();
    Python::attach(|py| {
        let config = PyDict::new(py);
        config.set_item("bootstrap_servers", "localhost:9092")?;
        config.set_item("source_system", "test")?;
        let omitted = build_producer_config(&config)?.build();

        config.set_item("send_timeout", py.None())?;
        let none = build_producer_config(&config)?.build();

        let timeouts = [omitted, none].map(|built| built.map(|config| config.send_timeout).ok());
        assert_eq!(
            timeouts,
            [Some(Some(Duration::from_secs(1))), Some(None)],
            "omitted must keep the default and None must turn the timeout off"
        );
        Ok(())
    })
}
