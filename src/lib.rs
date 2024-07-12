#![allow(missing_docs)] // todo: remove

use pyo3::types::{PyModule, PyModuleMethods};
use pyo3::{pymodule, Bound, PyResult};

use crate::entrypoint::Prosody;
use crate::message::Message;

mod entrypoint;
mod message;

#[pymodule]
fn prosody(m: &Bound<PyModule>) -> PyResult<()> {
    pyo3_log::init();
    m.add_class::<Prosody>()?;
    m.add_class::<Message>()?;

    Ok(())
}
