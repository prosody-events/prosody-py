#![allow(missing_docs)] // todo: remove

use once_cell::sync::Lazy;
use pyo3::types::{PyModule, PyModuleMethods};
use pyo3::{pymodule, Bound, PyResult};
use tokio::runtime::Runtime;

use crate::client::ProsodyClient;
use crate::message::{Context, Message};

mod client;
mod handler;
mod message;

#[allow(clippy::expect_used)]
static RUNTIME: Lazy<Runtime> =
    Lazy::new(|| Runtime::new().expect("Failed to create Tokio runtime"));

#[pymodule]
fn prosody(m: &Bound<PyModule>) -> PyResult<()> {
    pyo3_log::init();
    m.add_class::<ProsodyClient>()?;
    m.add_class::<Context>()?;
    m.add_class::<Message>()?;

    Ok(())
}
