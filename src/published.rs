//! Python-native read-only views over published keyed state.

use crate::state::{
    NativeJsonDequeScan, NativeJsonMapScan, NativeMapKeyScan, StateEnv, parse_direction,
    state_error,
};
use prosody::JsonCodec;
use prosody::consumer::event_context::ErasedStateError;
use prosody::high_level::erased::{
    ErasedDirection, SharedDequeReader, SharedMapReader, SharedValueReader,
};
use prosody::state::Direction;
use pyo3::{Bound, Py, PyAny, PyResult, Python, pyclass, pymethods};
use pyo3_async_runtimes::tokio::future_into_py;
use pythonize::pythonize;

fn published_error(env: &StateEnv, error: &ErasedStateError) -> pyo3::PyErr {
    Python::attach(|py| state_error(py, env, error))
}

fn erased_direction(direction: Direction) -> ErasedDirection {
    match direction {
        Direction::Forward => ErasedDirection::Forward,
        Direction::Backward => ErasedDirection::Backward,
    }
}

/// A read-only published value collection.
#[pyclass(name = "_NativePublishedValue")]
pub struct PublishedValue {
    pub(crate) inner: SharedValueReader<JsonCodec>,
    pub(crate) env: StateEnv,
}

#[pymethods]
impl PublishedValue {
    fn get<'p>(&self, py: Python<'p>, key: String) -> PyResult<Bound<'p, PyAny>> {
        let inner = self.inner.clone();
        let env = self.env.clone();
        future_into_py(py, async move {
            let value = inner
                .get(key)
                .await
                .map_err(|error| published_error(&env, &error))?;
            Python::attach(|py| Ok(pythonize(py, &value)?.unbind()))
        })
    }
}

/// A read-only published map collection.
#[pyclass(name = "_NativePublishedMap")]
pub struct PublishedMap {
    pub(crate) inner: SharedMapReader<JsonCodec>,
    pub(crate) env: StateEnv,
}

#[pymethods]
impl PublishedMap {
    fn get<'p>(&self, py: Python<'p>, key: String, map_key: String) -> PyResult<Bound<'p, PyAny>> {
        let inner = self.inner.clone();
        let env = self.env.clone();
        future_into_py(py, async move {
            let value = inner
                .get(key, map_key)
                .await
                .map_err(|error| published_error(&env, &error))?;
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
        let env = self.env.clone();
        future_into_py(py, async move {
            let values = inner
                .get_many(key, map_keys)
                .await
                .map_err(|error| published_error(&env, &error))?;
            Python::attach(|py| Ok(pythonize(py, &values)?.unbind()))
        })
    }

    fn contains_key<'p>(
        &self,
        py: Python<'p>,
        key: String,
        map_key: String,
    ) -> PyResult<Bound<'p, PyAny>> {
        let inner = self.inner.clone();
        let env = self.env.clone();
        future_into_py(py, async move {
            inner
                .contains_key(key, map_key)
                .await
                .map_err(|error| published_error(&env, &error))
        })
    }

    fn scan<'p>(&self, py: Python<'p>, key: String, direction: &str) -> PyResult<Bound<'p, PyAny>> {
        let direction = erased_direction(parse_direction(py, &self.env, direction)?);
        let inner = self.inner.clone();
        let env = self.env.clone();
        future_into_py(py, async move {
            let cursor = inner
                .stream(key, direction)
                .await
                .map_err(|error| published_error(&env, &error))?;
            Python::attach(|py| Ok(Py::new(py, NativeJsonMapScan::new(cursor, env))?.into_any()))
        })
    }

    fn keys<'p>(&self, py: Python<'p>, key: String, direction: &str) -> PyResult<Bound<'p, PyAny>> {
        let direction = erased_direction(parse_direction(py, &self.env, direction)?);
        let inner = self.inner.clone();
        let env = self.env.clone();
        future_into_py(py, async move {
            let cursor = inner
                .keys(key, direction)
                .await
                .map_err(|error| published_error(&env, &error))?;
            Python::attach(|py| Ok(Py::new(py, NativeMapKeyScan::new(cursor, env))?.into_any()))
        })
    }
}

/// A read-only published deque collection.
#[pyclass(name = "_NativePublishedDeque")]
pub struct PublishedDeque {
    pub(crate) inner: SharedDequeReader<JsonCodec>,
    pub(crate) env: StateEnv,
}

#[pymethods]
impl PublishedDeque {
    fn get<'p>(&self, py: Python<'p>, key: String, index: usize) -> PyResult<Bound<'p, PyAny>> {
        let inner = self.inner.clone();
        let env = self.env.clone();
        future_into_py(py, async move {
            let value = inner
                .get(key, index)
                .await
                .map_err(|error| published_error(&env, &error))?;
            Python::attach(|py| Ok(pythonize(py, &value)?.unbind()))
        })
    }

    fn len<'p>(&self, py: Python<'p>, key: String) -> PyResult<Bound<'p, PyAny>> {
        let inner = self.inner.clone();
        let env = self.env.clone();
        future_into_py(py, async move {
            inner
                .len(key)
                .await
                .map_err(|error| published_error(&env, &error))
        })
    }

    fn is_empty<'p>(&self, py: Python<'p>, key: String) -> PyResult<Bound<'p, PyAny>> {
        let inner = self.inner.clone();
        let env = self.env.clone();
        future_into_py(py, async move {
            inner
                .is_empty(key)
                .await
                .map_err(|error| published_error(&env, &error))
        })
    }

    fn peek_front<'p>(&self, py: Python<'p>, key: String) -> PyResult<Bound<'p, PyAny>> {
        let inner = self.inner.clone();
        let env = self.env.clone();
        future_into_py(py, async move {
            let value = inner
                .peek_front(key)
                .await
                .map_err(|error| published_error(&env, &error))?;
            Python::attach(|py| Ok(pythonize(py, &value)?.unbind()))
        })
    }

    fn peek_back<'p>(&self, py: Python<'p>, key: String) -> PyResult<Bound<'p, PyAny>> {
        let inner = self.inner.clone();
        let env = self.env.clone();
        future_into_py(py, async move {
            let value = inner
                .peek_back(key)
                .await
                .map_err(|error| published_error(&env, &error))?;
            Python::attach(|py| Ok(pythonize(py, &value)?.unbind()))
        })
    }

    fn scan<'p>(&self, py: Python<'p>, key: String, direction: &str) -> PyResult<Bound<'p, PyAny>> {
        let direction = erased_direction(parse_direction(py, &self.env, direction)?);
        let inner = self.inner.clone();
        let env = self.env.clone();
        future_into_py(py, async move {
            let cursor = inner
                .stream(key, direction)
                .await
                .map_err(|error| published_error(&env, &error))?;
            Python::attach(|py| Ok(Py::new(py, NativeJsonDequeScan::new(cursor, env))?.into_any()))
        })
    }
}
