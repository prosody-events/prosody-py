//! Ordered-map state handles.

use super::{
    Arc, Bound, BoxMapState, ConsumerMessage, KeyQuery, NativeJsonMapScan, NativeMapKeyScan,
    NativeMessageMapScan, PyAny, PyResult, Python, StateEnv, Value, build_message, json_object,
    json_write_item, message_write_item, outcome_token, pyclass, pymethods, run, run_item,
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
                let state = Arc::clone(&self.state);
                run_item(py, &self.env, async move { state.get(key).await }, $restore)
            }

            /// Reads several entries in input order.
            fn get_many<'p>(
                &self,
                py: Python<'p>,
                keys: Vec<String>,
            ) -> PyResult<Bound<'p, PyAny>> {
                let state = Arc::clone(&self.state);
                let op = async move { state.get_many(keys).await };
                run(py, &self.env, op, |py, env, items| {
                    items
                        .iter()
                        .map(|item| {
                            item.as_ref()
                                .map(|item| $restore(py, env, item))
                                .transpose()
                        })
                        .collect::<PyResult<Vec<_>>>()
                })
            }

            /// Reports whether one entry exists.
            fn contains_key<'p>(&self, py: Python<'p>, key: String) -> PyResult<Bound<'p, PyAny>> {
                let state = Arc::clone(&self.state);
                let op = async move { state.contains_key(key).await };
                run(py, &self.env, op, |_, _, found| Ok(found))
            }

            /// Reports whether each key exists, in input order.
            fn contains_many<'p>(
                &self,
                py: Python<'p>,
                keys: Vec<String>,
            ) -> PyResult<Bound<'p, PyAny>> {
                let state = Arc::clone(&self.state);
                let op = async move { state.contains_many(keys).await };
                run(py, &self.env, op, |_, _, found| Ok(found))
            }

            /// Reports whether the map is empty.
            fn is_empty<'p>(&self, py: Python<'p>) -> PyResult<Bound<'p, PyAny>> {
                let state = Arc::clone(&self.state);
                let op = async move { state.is_empty().await };
                run(py, &self.env, op, |_, _, empty| Ok(empty))
            }

            /// Inserts or overwrites one entry.
            fn set<'p>(
                &self,
                py: Python<'p>,
                key: String,
                item: &Bound<'p, PyAny>,
            ) -> PyResult<Bound<'p, PyAny>> {
                let item = ($prepare)(py, &self.env, item)?;
                let state = Arc::clone(&self.state);
                let op = async move { state.set(key, item).await };
                run(py, &self.env, op, |_, _, ()| Ok(()))
            }

            /// Removes one entry.
            fn remove<'p>(&self, py: Python<'p>, key: String) -> PyResult<Bound<'p, PyAny>> {
                let state = Arc::clone(&self.state);
                let op = async move { state.remove(key).await };
                run(py, &self.env, op, |_, _, ()| Ok(()))
            }

            /// Removes every entry.
            fn clear<'p>(&self, py: Python<'p>) -> PyResult<Bound<'p, PyAny>> {
                let state = Arc::clone(&self.state);
                let op = async move { state.clear().await };
                run(py, &self.env, op, |_, _, ()| Ok(()))
            }

            /// Opens an entry cursor.
            fn scan(&self, py: Python, query: KeyQuery) -> PyResult<$scan> {
                let cursor = query.stream(py, &self.env, self.state.entries())?;
                Ok($scan::new(cursor, self.env.clone()))
            }

            /// Opens a key cursor.
            fn keys(&self, py: Python, query: KeyQuery) -> PyResult<NativeMapKeyScan> {
                let cursor = query.stream(py, &self.env, self.state.keys())?;
                Ok(NativeMapKeyScan::new(cursor, self.env.clone()))
            }

            /// Durably commits the buffered operations and reports the outcome.
            fn commit<'p>(&self, py: Python<'p>) -> PyResult<Bound<'p, PyAny>> {
                let state = Arc::clone(&self.state);
                let op = async move { state.commit().await };
                run(py, &self.env, op, |_, _, outcome| {
                    Ok(outcome_token(outcome))
                })
            }

            /// Discards the buffered operations and reports the outcome.
            fn rollback<'p>(&self, py: Python<'p>) -> PyResult<Bound<'p, PyAny>> {
                let state = Arc::clone(&self.state);
                let op = async move { Ok(state.rollback().await) };
                run(py, &self.env, op, |_, _, outcome| {
                    Ok(outcome_token(outcome))
                })
            }
        }
    };
}

map_state!(
    NativeJsonMapState,
    Value,
    NativeJsonMapScan,
    json_write_item,
    json_object
);
map_state!(
    NativeMessageMapState,
    ConsumerMessage<Value>,
    NativeMessageMapScan,
    message_write_item,
    build_message
);
