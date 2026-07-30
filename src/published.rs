//! Python-native read-only views over published keyed state.

use prosody::JsonCodec;
use prosody::high_level::erased::{
    ErasedDirection, SharedDequeReader, SharedMapReader, SharedStateStream, SharedValueReader,
};
use prosody::state_reader::StateReaderError;
use pyo3::exceptions::{PyRuntimeError, PyStopAsyncIteration};
use pyo3::{Bound, Py, PyAny, PyRef, PyResult, Python, pyclass, pymethods};
use pyo3_async_runtimes::tokio::future_into_py;
use pythonize::pythonize;
use serde_json::Value;

fn runtime_error(error: &StateReaderError) -> pyo3::PyErr {
    PyRuntimeError::new_err(error.to_string())
}

fn direction(backward: bool) -> ErasedDirection {
    if backward {
        ErasedDirection::Backward
    } else {
        ErasedDirection::Forward
    }
}

/// A read-only published value collection.
#[pyclass]
pub struct PublishedValue {
    pub(crate) inner: SharedValueReader<JsonCodec>,
}

#[pymethods]
impl PublishedValue {
    fn get<'p>(&self, py: Python<'p>, key: String) -> PyResult<Bound<'p, PyAny>> {
        let inner = self.inner.clone();
        future_into_py(py, async move {
            let value = inner
                .get(key)
                .await
                .map_err(|error| runtime_error(&error))?;
            Python::attach(|py| Ok(pythonize(py, &value)?.unbind()))
        })
    }
}

/// A read-only published map collection.
#[pyclass]
pub struct PublishedMap {
    pub(crate) inner: SharedMapReader<JsonCodec>,
}

#[pymethods]
impl PublishedMap {
    fn get<'p>(&self, py: Python<'p>, key: String, map_key: String) -> PyResult<Bound<'p, PyAny>> {
        let inner = self.inner.clone();
        future_into_py(py, async move {
            let value = inner
                .get(key, map_key)
                .await
                .map_err(|error| runtime_error(&error))?;
            Python::attach(|py| Ok(pythonize(py, &value)?.unbind()))
        })
    }

    fn get_many<'p>(
        &self,
        py: Python<'p>,
        key: String,
        map_keys: Vec<String>,
    ) -> PyResult<Bound<'p, PyAny>> {
        let inner = self.inner.clone();
        future_into_py(py, async move {
            let values = inner
                .get_many(key, map_keys)
                .await
                .map_err(|error| runtime_error(&error))?;
            Python::attach(|py| Ok(pythonize(py, &values)?.unbind()))
        })
    }

    #[pyo3(signature = (key, *, backward = false))]
    fn scan<'p>(&self, py: Python<'p>, key: String, backward: bool) -> PyResult<Bound<'p, PyAny>> {
        let inner = self.inner.clone();
        future_into_py(py, async move {
            let stream = inner
                .stream(key, direction(backward))
                .await
                .map_err(|error| runtime_error(&error))?;
            Python::attach(|py| Ok(Py::new(py, PyPublishedMapScan { inner: stream })?.into_any()))
        })
    }
}

/// Async iterator over published map entries.
#[pyclass(name = "_PublishedMapScan")]
pub struct PyPublishedMapScan {
    inner: SharedStateStream<(String, Value)>,
}

#[pymethods]
impl PyPublishedMapScan {
    fn __aiter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __anext__<'p>(&self, py: Python<'p>) -> PyResult<Bound<'p, PyAny>> {
        let inner = self.inner.clone();
        future_into_py(py, async move {
            let Some(item) = inner.next().await else {
                return Err(PyStopAsyncIteration::new_err(()));
            };
            let item = item.map_err(|error| runtime_error(&error))?;
            Python::attach(|py| Ok(pythonize(py, &item)?.unbind()))
        })
    }
}

/// A read-only published deque collection.
#[pyclass]
pub struct PublishedDeque {
    pub(crate) inner: SharedDequeReader<JsonCodec>,
}

#[pymethods]
impl PublishedDeque {
    fn get<'p>(&self, py: Python<'p>, key: String, index: usize) -> PyResult<Bound<'p, PyAny>> {
        let inner = self.inner.clone();
        future_into_py(py, async move {
            let value = inner
                .get(key, index)
                .await
                .map_err(|error| runtime_error(&error))?;
            Python::attach(|py| Ok(pythonize(py, &value)?.unbind()))
        })
    }

    fn length<'p>(&self, py: Python<'p>, key: String) -> PyResult<Bound<'p, PyAny>> {
        let inner = self.inner.clone();
        future_into_py(py, async move {
            inner.len(key).await.map_err(|error| runtime_error(&error))
        })
    }

    #[pyo3(signature = (key, *, backward = false))]
    fn scan<'p>(&self, py: Python<'p>, key: String, backward: bool) -> PyResult<Bound<'p, PyAny>> {
        let inner = self.inner.clone();
        future_into_py(py, async move {
            let stream = inner
                .stream(key, direction(backward))
                .await
                .map_err(|error| runtime_error(&error))?;
            Python::attach(|py| Ok(Py::new(py, PyPublishedDequeScan { inner: stream })?.into_any()))
        })
    }
}

/// Async iterator over published deque elements.
#[pyclass(name = "_PublishedDequeScan")]
pub struct PyPublishedDequeScan {
    inner: SharedStateStream<Value>,
}

#[pymethods]
impl PyPublishedDequeScan {
    fn __aiter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __anext__<'p>(&self, py: Python<'p>) -> PyResult<Bound<'p, PyAny>> {
        let inner = self.inner.clone();
        future_into_py(py, async move {
            let Some(item) = inner.next().await else {
                return Err(PyStopAsyncIteration::new_err(()));
            };
            let item = item.map_err(|error| runtime_error(&error))?;
            Python::attach(|py| Ok(pythonize(py, &item)?.unbind()))
        })
    }
}
