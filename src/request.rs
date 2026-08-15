//! Python outcome values for subsystem responses.

use prosody::requester::ResponseError;
use pyo3::types::{PyAnyMethods, PyModule};
use pyo3::{Bound, Py, PyAny, PyResult, Python};
use pythonize::pythonize;
use serde_json::Value;

pub(crate) fn to_python(
    py: Python,
    module: &Bound<'_, PyModule>,
    result: Result<Value, ResponseError>,
) -> PyResult<Py<PyAny>> {
    match result {
        Ok(value) => module
            .getattr("Success")?
            .call1((pythonize(py, &value)?,))
            .map(Bound::unbind),
        Err(error) => {
            let error = match error {
                ResponseError::Handler { message } => {
                    module.getattr("HandlerError")?.call1((message,))?
                }
                ResponseError::Timeout => module.getattr("Timeout")?.call0()?,
                ResponseError::FormatMismatch => module.getattr("FormatMismatch")?.call0()?,
                ResponseError::Malformed => module.getattr("MalformedResponse")?.call0()?,
            };
            module
                .getattr("Failure")?
                .call1((error,))
                .map(Bound::unbind)
        }
    }
}
