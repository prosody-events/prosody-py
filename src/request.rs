//! Python result types for one peer response.

use prosody::error::ErrorCategory;
use prosody::requester::ResponseError;
use pyo3::exceptions::PyException;
use pyo3::types::PyAnyMethods;
use pyo3::{Py, PyAny, PyResult, Python, pyclass};
use pythonize::pythonize;
use serde_json::Value;

/// Base class for one subsystem failure.
#[pyclass(frozen, subclass, extends = PyException, name = "ResponseError", module = "prosody")]
pub struct PythonResponseError;

/// A remote handler failure.
#[pyclass(frozen, extends = PythonResponseError, module = "prosody")]
pub struct HandlerResponseError {
    #[pyo3(get)]
    category: &'static str,
    #[pyo3(get)]
    handler_message: String,
}

/// No response arrived before the deadline.
#[pyclass(frozen, extends = PythonResponseError, module = "prosody")]
pub struct ResponseTimeoutError;

/// The responder used another response format.
#[pyclass(frozen, extends = PythonResponseError, module = "prosody")]
pub struct ResponseFormatMismatchError;

/// The response payload was malformed.
#[pyclass(frozen, extends = PythonResponseError, module = "prosody")]
pub struct MalformedResponseError;

pub(crate) fn to_python(py: Python, result: Result<Value, ResponseError>) -> PyResult<Py<PyAny>> {
    match result {
        Ok(value) => Ok(pythonize(py, &value)?.unbind()),
        Err(error) => error_to_python(py, error),
    }
}

fn error_to_python(py: Python, error: ResponseError) -> PyResult<Py<PyAny>> {
    let message = error.to_string();
    let exception = match error {
        ResponseError::Handler { category, message } => Py::new(
            py,
            (
                HandlerResponseError {
                    category: category_name(category),
                    handler_message: message,
                },
                PythonResponseError,
            ),
        )
        .map(Py::into_any),
        ResponseError::Timeout => {
            Py::new(py, (ResponseTimeoutError, PythonResponseError)).map(Py::into_any)
        }
        ResponseError::FormatMismatch => {
            Py::new(py, (ResponseFormatMismatchError, PythonResponseError)).map(Py::into_any)
        }
        ResponseError::Malformed => {
            Py::new(py, (MalformedResponseError, PythonResponseError)).map(Py::into_any)
        }
    }?;
    exception.bind(py).setattr("args", (message,))?;
    Ok(exception)
}

fn category_name(category: ErrorCategory) -> &'static str {
    match category {
        ErrorCategory::Transient => "transient",
        ErrorCategory::Permanent => "permanent",
        ErrorCategory::Terminal => "terminal",
    }
}
