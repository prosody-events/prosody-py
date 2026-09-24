//! Python-native read-only views over published keyed state.

use crate::state::{
    KeyQuery, NativeJsonDequeScan, NativeJsonMapScan, NativeKeyScan, PositionQuery, StateEnv,
    state_error,
};
use prosody::consumer::event_context::ErasedStateError;
use prosody::high_level::erased::{
    SharedDequeReader, SharedMapReader, SharedSetReader, SharedValueReader,
};
use pyo3::{Bound, PyAny, PyResult, Python, pyclass, pymethods};
use pyo3_async_runtimes::tokio::future_into_py;
use pythonize::pythonize;
use serde_json::Value;

fn published_error(env: &StateEnv, error: &ErasedStateError) -> pyo3::PyErr {
    Python::attach(|py| state_error(py, env, error))
}

/// A read-only published value collection.
#[pyclass(name = "_NativePublishedValue")]
pub struct PublishedValue {
    pub(crate) inner: SharedValueReader<Value>,
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
    pub(crate) inner: SharedMapReader<Value>,
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

    fn scan(&self, py: Python, key: String, query: KeyQuery) -> PyResult<NativeJsonMapScan> {
        let cursor = query.stream(py, &self.env, self.inner.entries(key))?;
        Ok(NativeJsonMapScan::new(cursor, self.env.clone()))
    }

    fn keys(&self, py: Python, key: String, query: KeyQuery) -> PyResult<NativeKeyScan> {
        let cursor = query.stream(py, &self.env, self.inner.keys(key))?;
        Ok(NativeKeyScan::new(cursor, self.env.clone()))
    }
}

/// A read-only published set collection.
#[pyclass(name = "_NativePublishedSet")]
pub struct PublishedSet {
    pub(crate) inner: SharedSetReader,
    pub(crate) env: StateEnv,
}

#[pymethods]
impl PublishedSet {
    fn contains<'p>(
        &self,
        py: Python<'p>,
        key: String,
        member: String,
    ) -> PyResult<Bound<'p, PyAny>> {
        let inner = self.inner.clone();
        let env = self.env.clone();
        future_into_py(py, async move {
            inner
                .contains(key, member)
                .await
                .map_err(|error| published_error(&env, &error))
        })
    }

    fn contains_many<'p>(
        &self,
        py: Python<'p>,
        key: String,
        members: Vec<String>,
    ) -> PyResult<Bound<'p, PyAny>> {
        let inner = self.inner.clone();
        let env = self.env.clone();
        future_into_py(py, async move {
            inner
                .contains_many(key, members)
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

    fn keys(&self, py: Python, key: String, query: KeyQuery) -> PyResult<NativeKeyScan> {
        let cursor = query.stream(py, &self.env, self.inner.keys(key))?;
        Ok(NativeKeyScan::new(cursor, self.env.clone()))
    }
}

/// A read-only published deque collection.
#[pyclass(name = "_NativePublishedDeque")]
pub struct PublishedDeque {
    pub(crate) inner: SharedDequeReader<Value>,
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

    fn scan(&self, py: Python, key: String, query: PositionQuery) -> PyResult<NativeJsonDequeScan> {
        let cursor = query.stream(py, &self.env, self.inner.values(key))?;
        Ok(NativeJsonDequeScan::new(cursor, self.env.clone()))
    }
}
