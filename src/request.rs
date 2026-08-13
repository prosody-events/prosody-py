//! Python result types for one peer response.

use prosody::error::ErrorCategory;
use prosody::requester::ResponseError;
use pyo3::types::{PyAnyMethods, PyType};
use pyo3::{Bound, Py, PyAny, PyResult, Python, pyclass, pymethods};
use pythonize::pythonize;
use serde_json::Value;

/// A successful peer response.
#[pyclass(frozen, name = "Ok", module = "prosody")]
pub struct RequestOk {
    #[pyo3(get)]
    value: Py<PyAny>,
}

#[pymethods]
impl RequestOk {
    #[classmethod]
    fn __class_getitem__(class: &Bound<'_, PyType>, _item: &Bound<'_, PyAny>) -> Py<PyAny> {
        class.clone().into_any().unbind()
    }

    fn __repr__(&self, py: Python) -> PyResult<String> {
        Ok(format!("Ok({})", self.value.bind(py).repr()?))
    }
}

/// A failed peer response.
#[pyclass(frozen, name = "Err", module = "prosody")]
pub struct RequestErr {
    #[pyo3(get)]
    error: Py<PyAny>,
}

#[pymethods]
impl RequestErr {
    fn __repr__(&self, py: Python) -> PyResult<String> {
        Ok(format!("Err({})", self.error.bind(py).repr()?))
    }
}

/// A remote handler failure.
#[pyclass(frozen, module = "prosody")]
pub struct HandlerResponseError {
    #[pyo3(get)]
    category: &'static str,
    #[pyo3(get)]
    message: String,
}

/// No response arrived before the deadline.
#[pyclass(frozen, module = "prosody")]
pub struct ResponseTimeoutError;

/// The responder used another response format.
#[pyclass(frozen, module = "prosody")]
pub struct ResponseFormatMismatchError;

/// The response payload was malformed.
#[pyclass(frozen, module = "prosody")]
pub struct MalformedResponseError;

pub(crate) fn to_python(py: Python, result: Result<Value, ResponseError>) -> PyResult<Py<PyAny>> {
    match result {
        Ok(value) => Py::new(
            py,
            RequestOk {
                value: pythonize(py, &value)?.unbind(),
            },
        )
        .map(Py::into_any),
        Err(error) => Py::new(
            py,
            RequestErr {
                error: error_to_python(py, error)?,
            },
        )
        .map(Py::into_any),
    }
}

fn error_to_python(py: Python, error: ResponseError) -> PyResult<Py<PyAny>> {
    match error {
        ResponseError::Handler { category, message } => Py::new(
            py,
            HandlerResponseError {
                category: category_name(category),
                message,
            },
        )
        .map(Py::into_any),
        ResponseError::Timeout => Py::new(py, ResponseTimeoutError).map(Py::into_any),
        ResponseError::FormatMismatch => Py::new(py, ResponseFormatMismatchError).map(Py::into_any),
        ResponseError::Malformed => Py::new(py, MalformedResponseError).map(Py::into_any),
    }
}

fn category_name(category: ErrorCategory) -> &'static str {
    match category {
        ErrorCategory::Transient => "transient",
        ErrorCategory::Permanent => "permanent",
        ErrorCategory::Terminal => "terminal",
    }
}
