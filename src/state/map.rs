//! Ordered-map state handles.

use super::{
    Arc, Bound, BoxMapState, ConsumerMessage, FutureExt, KeyQuery, NativeJsonMapScan,
    NativeKeyScan, NativeMessageMapScan, Py, PyAny, PyResult, PyTraverseError, PyVisit, Python,
    StateEnv, Value, build_message, future_into_py, json_write_item, message_write_item, pyclass,
    pymethods, pythonize, state_error,
};

macro_rules! map_state {
    ($name:ident, $payload:ty, $scan:ident, $prepare:expr, $restore:expr) => {
        /// Ordered-map state handle with one payload type.
        #[pyclass]
        pub struct $name {
            pub(crate) state: Arc<BoxMapState<$payload>>,
            pub(crate) env: StateEnv,
        }

        #[pymethods]
        impl $name {
            /// Reads one entry.
            fn get<'p>(&self, py: Python<'p>, key: String) -> PyResult<Bound<'p, PyAny>> {
                let ctx = self.env.op_context(py)?;
                let state = Arc::clone(&self.state);
                let env = self.env.clone();
                future_into_py(py, async move {
                    let out = state.get(key).with_context(ctx).await;
                    Python::attach(|py| match out {
                        Ok(item) => item.map(|item| ($restore)(py, &env, &item)).transpose(),
                        Err(error) => Err(state_error(py, &env, &error)),
                    })
                })
            }

            /// Reads several entries in input order.
            fn get_many<'p>(
                &self,
                py: Python<'p>,
                keys: Vec<String>,
            ) -> PyResult<Bound<'p, PyAny>> {
                let ctx = self.env.op_context(py)?;
                let state = Arc::clone(&self.state);
                let env = self.env.clone();
                future_into_py(py, async move {
                    let out = state.get_many(keys).with_context(ctx).await;
                    Python::attach(|py| match out {
                        Ok(items) => items
                            .into_iter()
                            .map(|item| item.map(|item| ($restore)(py, &env, &item)).transpose())
                            .collect::<PyResult<Vec<Option<Py<PyAny>>>>>(),
                        Err(error) => Err(state_error(py, &env, &error)),
                    })
                })
            }

            /// Reports whether one entry exists.
            fn contains_key<'p>(&self, py: Python<'p>, key: String) -> PyResult<Bound<'p, PyAny>> {
                let ctx = self.env.op_context(py)?;
                let state = Arc::clone(&self.state);
                let env = self.env.clone();
                future_into_py(py, async move {
                    let out = state.contains_key(key).with_context(ctx).await;
                    Python::attach(|py| out.map_err(|error| state_error(py, &env, &error)))
                })
            }

            /// Inserts or overwrites one entry.
            fn set<'p>(
                &self,
                py: Python<'p>,
                key: String,
                item: &Bound<'p, PyAny>,
            ) -> PyResult<Bound<'p, PyAny>> {
                let ctx = self.env.op_context(py)?;
                let item = ($prepare)(py, &self.env, item)?;
                let state = Arc::clone(&self.state);
                let env = self.env.clone();
                future_into_py(py, async move {
                    let out = state.set(key, item).with_context(ctx).await;
                    Python::attach(|py| out.map_err(|error| state_error(py, &env, &error)))
                })
            }

            /// Removes one entry.
            fn remove<'p>(&self, py: Python<'p>, key: String) -> PyResult<Bound<'p, PyAny>> {
                let ctx = self.env.op_context(py)?;
                let state = Arc::clone(&self.state);
                let env = self.env.clone();
                future_into_py(py, async move {
                    let out = state.remove(key).with_context(ctx).await;
                    Python::attach(|py| out.map_err(|error| state_error(py, &env, &error)))
                })
            }

            /// Removes every entry.
            fn clear<'p>(&self, py: Python<'p>) -> PyResult<Bound<'p, PyAny>> {
                let ctx = self.env.op_context(py)?;
                let state = Arc::clone(&self.state);
                let env = self.env.clone();
                future_into_py(py, async move {
                    let out = state.clear().with_context(ctx).await;
                    Python::attach(|py| out.map_err(|error| state_error(py, &env, &error)))
                })
            }

            /// Opens an entry cursor.
            fn scan(&self, py: Python, query: KeyQuery) -> PyResult<$scan> {
                let cursor = query.stream(py, &self.env, self.state.entries())?;
                Ok($scan::new(cursor, self.env.clone()))
            }

            /// Opens a key cursor.
            fn keys(&self, py: Python, query: KeyQuery) -> PyResult<NativeKeyScan> {
                let cursor = query.stream(py, &self.env, self.state.keys())?;
                Ok(NativeKeyScan::new(cursor, self.env.clone()))
            }

            /// Durably commits the buffered operations.
            fn commit<'p>(&self, py: Python<'p>) -> PyResult<Bound<'p, PyAny>> {
                let ctx = self.env.op_context(py)?;
                let state = Arc::clone(&self.state);
                let env = self.env.clone();
                future_into_py(py, async move {
                    let out = state.commit().with_context(ctx).await;
                    Python::attach(|py| {
                        out.map(drop).map_err(|error| state_error(py, &env, &error))
                    })
                })
            }

            /// Discards the buffered operations.
            fn rollback<'p>(&self, py: Python<'p>) -> PyResult<Bound<'p, PyAny>> {
                let ctx = self.env.op_context(py)?;
                let state = Arc::clone(&self.state);
                future_into_py(py, async move {
                    state.rollback().with_context(ctx).await;
                    Ok(())
                })
            }

            /// Traverses the Python handles this state holds for GC.
            fn __traverse__(&self, visit: PyVisit) -> Result<(), PyTraverseError> {
                self.env.traverse(visit).map(|_| ())
            }
        }
    };
}

map_state!(
    NativeJsonMapState,
    Value,
    NativeJsonMapScan,
    |py, env, item| json_write_item(py, env, item, "; use remove(key) to remove the entry"),
    |py, _env: &StateEnv, item| Ok(pythonize(py, item)?.unbind())
);
map_state!(
    NativeMessageMapState,
    ConsumerMessage<Value>,
    NativeMessageMapScan,
    message_write_item,
    build_message
);
