//! Request arguments and Python outcome values for subsystem responses.

use crate::util::decode_duration;
use prosody::requester::ResponseError;
use prosody::subsystem::SubsystemName;
use pyo3::exceptions::PyValueError;
use pyo3::types::{PyAnyMethods, PyDict, PyDictMethods, PyModule};
use pyo3::{Bound, Py, PyAny, PyResult, Python};
use pythonize::pythonize;
use serde_json::Value;
use std::time::Duration;

fn to_python(
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

pub(crate) fn request_parameters(
    subsystems: Vec<String>,
    timeout: &Bound<'_, PyAny>,
) -> PyResult<(Vec<SubsystemName>, Duration)> {
    let subsystems = subsystems
        .into_iter()
        .map(|name| {
            SubsystemName::try_new(name).map_err(|error| PyValueError::new_err(error.to_string()))
        })
        .collect::<PyResult<Vec<_>>>()?;
    Ok((subsystems, decode_duration(timeout)?))
}

pub(crate) fn request_outcomes<I>(py: Python, results: I) -> PyResult<Py<PyAny>>
where
    I: IntoIterator<Item = (SubsystemName, Result<Value, ResponseError>)>,
{
    let module = py.import("prosody.request")?;
    let outcomes = PyDict::new(py);
    for (subsystem, result) in results {
        outcomes.set_item(subsystem.as_str(), to_python(py, &module, result)?)?;
    }
    Ok(outcomes.into_any().unbind())
}
