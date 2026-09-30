//! Python-native read-only views over published keyed state.

use crate::state::{
    KeyQuery, NativeJsonDequeScan, NativeJsonMapScan, NativeMapKeyScan, PositionQuery, StateEnv,
    json_object, run, run_item,
};
use prosody::high_level::erased::{
    SharedDequeReader, SharedMapReader, SharedSetReader, SharedValueReader,
};
use pyo3::{Bound, PyAny, PyResult, Python, pyclass, pymethods};
use pythonize::pythonize;
use serde_json::Value;

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
        run_item(
            py,
            &self.env,
            async move { inner.get(key).await },
            json_object,
        )
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
        run_item(
            py,
            &self.env,
            async move { inner.get(key, map_key).await },
            json_object,
        )
    }

    fn get_many<'p>(
        &self,
        py: Python<'p>,
        key: String,
        map_keys: Vec<String>,
    ) -> PyResult<Bound<'p, PyAny>> {
        let inner = self.inner.clone();
        let op = async move { inner.get_many(key, map_keys).await };
        run(py, &self.env, op, |py, _, values| {
            Ok(pythonize(py, &values)?.unbind())
        })
    }

    fn contains_key<'p>(
        &self,
        py: Python<'p>,
        key: String,
        map_key: String,
    ) -> PyResult<Bound<'p, PyAny>> {
        let inner = self.inner.clone();
        let op = async move { inner.contains_key(key, map_key).await };
        run(py, &self.env, op, |_, _, found| Ok(found))
    }

    fn contains_many<'p>(
        &self,
        py: Python<'p>,
        key: String,
        map_keys: Vec<String>,
    ) -> PyResult<Bound<'p, PyAny>> {
        let inner = self.inner.clone();
        let op = async move { inner.contains_many(key, map_keys).await };
        run(py, &self.env, op, |_, _, found| Ok(found))
    }

    fn is_empty<'p>(&self, py: Python<'p>, key: String) -> PyResult<Bound<'p, PyAny>> {
        let inner = self.inner.clone();
        let op = async move { inner.is_empty(key).await };
        run(py, &self.env, op, |_, _, empty| Ok(empty))
    }

    fn scan(&self, key: String, query: KeyQuery) -> NativeJsonMapScan {
        NativeJsonMapScan::new(query.stream(self.inner.entries(key)), self.env.clone())
    }

    fn keys(&self, key: String, query: KeyQuery) -> NativeMapKeyScan {
        NativeMapKeyScan::new(query.stream(self.inner.keys(key)), self.env.clone())
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
        let op = async move { inner.contains(key, member).await };
        run(py, &self.env, op, |_, _, found| Ok(found))
    }

    fn contains_many<'p>(
        &self,
        py: Python<'p>,
        key: String,
        members: Vec<String>,
    ) -> PyResult<Bound<'p, PyAny>> {
        let inner = self.inner.clone();
        let op = async move { inner.contains_many(key, members).await };
        run(py, &self.env, op, |_, _, found| Ok(found))
    }

    fn is_empty<'p>(&self, py: Python<'p>, key: String) -> PyResult<Bound<'p, PyAny>> {
        let inner = self.inner.clone();
        let op = async move { inner.is_empty(key).await };
        run(py, &self.env, op, |_, _, empty| Ok(empty))
    }

    fn keys(&self, key: String, query: KeyQuery) -> NativeMapKeyScan {
        NativeMapKeyScan::new(query.stream(self.inner.keys(key)), self.env.clone())
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
        run_item(
            py,
            &self.env,
            async move { inner.get(key, index).await },
            json_object,
        )
    }

    fn len<'p>(&self, py: Python<'p>, key: String) -> PyResult<Bound<'p, PyAny>> {
        let inner = self.inner.clone();
        let op = async move { inner.len(key).await };
        run(py, &self.env, op, |_, _, len| Ok(len))
    }

    fn is_empty<'p>(&self, py: Python<'p>, key: String) -> PyResult<Bound<'p, PyAny>> {
        let inner = self.inner.clone();
        let op = async move { inner.is_empty(key).await };
        run(py, &self.env, op, |_, _, empty| Ok(empty))
    }

    fn peek_front<'p>(&self, py: Python<'p>, key: String) -> PyResult<Bound<'p, PyAny>> {
        let inner = self.inner.clone();
        run_item(
            py,
            &self.env,
            async move { inner.peek_front(key).await },
            json_object,
        )
    }

    fn peek_back<'p>(&self, py: Python<'p>, key: String) -> PyResult<Bound<'p, PyAny>> {
        let inner = self.inner.clone();
        run_item(
            py,
            &self.env,
            async move { inner.peek_back(key).await },
            json_object,
        )
    }

    fn scan(&self, key: String, query: PositionQuery) -> NativeJsonDequeScan {
        NativeJsonDequeScan::new(query.stream(self.inner.values(key)), self.env.clone())
    }
}
