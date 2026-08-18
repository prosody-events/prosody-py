use super::{
    Arc, Bound, BoxDequeState, BoxMapState, BoxValueState, ConsumerMessage, FutureExt,
    NativeJsonDequeScan, NativeJsonMapScan, NativeMapKeyScan, NativeMessageDequeScan,
    NativeMessageMapScan, Py, PyAny, PyAnyMethods, PyResult, PyTraverseError, PyVisit, Python,
    StateEnv, Value, build_message, consumer_message, future_into_py, json_write_item,
    parse_direction, pyclass, pymethods, pythonize, state_error, transient_error,
};

fn message_write_item(
    py: Python,
    env: &StateEnv,
    item: &Bound<PyAny>,
) -> PyResult<ConsumerMessage<Value>> {
    if item.is_instance(env.0.message_class.bind(py))? {
        consumer_message(py, env, item)
    } else {
        Err(transient_error(py, env, "expected a Kafka message"))
    }
}

macro_rules! value_state {
    ($name:ident, $payload:ty, $prepare:expr, $restore:expr) => {
        /// Single-value state handle with one payload type.
        #[pyclass]
        pub struct $name {
            pub(crate) state: Arc<BoxValueState<$payload>>,
            pub(crate) env: StateEnv,
        }

        #[pymethods]
        impl $name {
            /// Reads the current value.
            fn get<'p>(&self, py: Python<'p>) -> PyResult<Bound<'p, PyAny>> {
                let ctx = self.env.op_context(py)?;
                let state = Arc::clone(&self.state);
                let env = self.env.clone();
                future_into_py(py, async move {
                    let out = state.get().with_context(ctx).await;
                    Python::attach(|py| match out {
                        Ok(item) => item.map(|item| ($restore)(py, &env, &item)).transpose(),
                        Err(error) => Err(state_error(py, &env, &error)),
                    })
                })
            }

            /// Buffers a write of the value.
            fn set<'p>(
                &self,
                py: Python<'p>,
                item: &Bound<'p, PyAny>,
            ) -> PyResult<Bound<'p, PyAny>> {
                let ctx = self.env.op_context(py)?;
                let item = ($prepare)(py, &self.env, item)?;
                let state = Arc::clone(&self.state);
                let env = self.env.clone();
                future_into_py(py, async move {
                    let out = state.set(item).with_context(ctx).await;
                    Python::attach(|py| out.map_err(|error| state_error(py, &env, &error)))
                })
            }

            /// Buffers a clear of the value.
            fn clear<'p>(&self, py: Python<'p>) -> PyResult<Bound<'p, PyAny>> {
                let ctx = self.env.op_context(py)?;
                let state = Arc::clone(&self.state);
                let env = self.env.clone();
                future_into_py(py, async move {
                    let out = state.clear().with_context(ctx).await;
                    Python::attach(|py| out.map_err(|error| state_error(py, &env, &error)))
                })
            }

            /// Durably commits the buffered operations.
            fn commit<'p>(&self, py: Python<'p>) -> PyResult<Bound<'p, PyAny>> {
                let ctx = self.env.op_context(py)?;
                let state = Arc::clone(&self.state);
                let env = self.env.clone();
                future_into_py(py, async move {
                    let out = state.commit().with_context(ctx).await;
                    Python::attach(|py| out.map_err(|error| state_error(py, &env, &error)))
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

value_state!(
    NativeJsonValueState,
    Value,
    |py, env, item| json_write_item(py, env, item, "; use clear() to remove the value"),
    |py, _env: &StateEnv, item| Ok(pythonize(py, item)?.unbind())
);
value_state!(
    NativeMessageValueState,
    ConsumerMessage<Value>,
    message_write_item,
    build_message
);

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
            fn scan(&self, py: Python, direction: &str) -> PyResult<$scan> {
                let direction = parse_direction(py, &self.env, direction)?;
                let _guard = self.env.op_context(py)?.attach();
                Ok($scan::new(self.state.scan(direction), self.env.clone()))
            }

            /// Opens a key cursor.
            fn keys(&self, py: Python, direction: &str) -> PyResult<NativeMapKeyScan> {
                let direction = parse_direction(py, &self.env, direction)?;
                let _guard = self.env.op_context(py)?.attach();
                Ok(NativeMapKeyScan::new(
                    self.state.keys(direction),
                    self.env.clone(),
                ))
            }

            /// Durably commits the buffered operations.
            fn commit<'p>(&self, py: Python<'p>) -> PyResult<Bound<'p, PyAny>> {
                let ctx = self.env.op_context(py)?;
                let state = Arc::clone(&self.state);
                let env = self.env.clone();
                future_into_py(py, async move {
                    let out = state.commit().with_context(ctx).await;
                    Python::attach(|py| out.map_err(|error| state_error(py, &env, &error)))
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

macro_rules! deque_state {
    ($name:ident, $payload:ty, $scan:ident, $prepare:expr, $restore:expr) => {
        /// Deque state handle with one payload type.
        #[pyclass]
        pub struct $name {
            pub(crate) state: Arc<BoxDequeState<$payload>>,
            pub(crate) env: StateEnv,
        }

        #[pymethods]
        impl $name {
            /// Returns the number of live elements.
            fn len<'p>(&self, py: Python<'p>) -> PyResult<Bound<'p, PyAny>> {
                let ctx = self.env.op_context(py)?;
                let state = Arc::clone(&self.state);
                let env = self.env.clone();
                future_into_py(py, async move {
                    let out = state.len().with_context(ctx).await;
                    Python::attach(|py| match out {
                        Ok(len) => u32::try_from(len).map_err(|_| {
                            transient_error(
                                py,
                                &env,
                                &format!("deque length {len} exceeds the u32 range"),
                            )
                        }),
                        Err(error) => Err(state_error(py, &env, &error)),
                    })
                })
            }

            /// Reports whether the deque is empty.
            fn is_empty<'p>(&self, py: Python<'p>) -> PyResult<Bound<'p, PyAny>> {
                let ctx = self.env.op_context(py)?;
                let state = Arc::clone(&self.state);
                let env = self.env.clone();
                future_into_py(py, async move {
                    let out = state.is_empty().with_context(ctx).await;
                    Python::attach(|py| out.map_err(|error| state_error(py, &env, &error)))
                })
            }

            /// Reads one element by its position from the front.
            fn get<'p>(&self, py: Python<'p>, index: u32) -> PyResult<Bound<'p, PyAny>> {
                let ctx = self.env.op_context(py)?;
                let state = Arc::clone(&self.state);
                let env = self.env.clone();
                future_into_py(py, async move {
                    let out = state.get(index as usize).with_context(ctx).await;
                    Python::attach(|py| match out {
                        Ok(item) => item.map(|item| ($restore)(py, &env, &item)).transpose(),
                        Err(error) => Err(state_error(py, &env, &error)),
                    })
                })
            }

            /// Appends one element.
            fn push_back<'p>(
                &self,
                py: Python<'p>,
                item: &Bound<'p, PyAny>,
            ) -> PyResult<Bound<'p, PyAny>> {
                let ctx = self.env.op_context(py)?;
                let item = ($prepare)(py, &self.env, item)?;
                let state = Arc::clone(&self.state);
                let env = self.env.clone();
                future_into_py(py, async move {
                    let out = state.push_back(item).with_context(ctx).await;
                    Python::attach(|py| out.map_err(|error| state_error(py, &env, &error)))
                })
            }

            /// Prepends one element.
            fn push_front<'p>(
                &self,
                py: Python<'p>,
                item: &Bound<'p, PyAny>,
            ) -> PyResult<Bound<'p, PyAny>> {
                let ctx = self.env.op_context(py)?;
                let item = ($prepare)(py, &self.env, item)?;
                let state = Arc::clone(&self.state);
                let env = self.env.clone();
                future_into_py(py, async move {
                    let out = state.push_front(item).with_context(ctx).await;
                    Python::attach(|py| out.map_err(|error| state_error(py, &env, &error)))
                })
            }

            /// Removes and returns the front element.
            fn pop_front<'p>(&self, py: Python<'p>) -> PyResult<Bound<'p, PyAny>> {
                let ctx = self.env.op_context(py)?;
                let state = Arc::clone(&self.state);
                let env = self.env.clone();
                future_into_py(py, async move {
                    let out = state.pop_front().with_context(ctx).await;
                    Python::attach(|py| match out {
                        Ok(item) => item.map(|item| ($restore)(py, &env, &item)).transpose(),
                        Err(error) => Err(state_error(py, &env, &error)),
                    })
                })
            }

            /// Removes and returns the back element.
            fn pop_back<'p>(&self, py: Python<'p>) -> PyResult<Bound<'p, PyAny>> {
                let ctx = self.env.op_context(py)?;
                let state = Arc::clone(&self.state);
                let env = self.env.clone();
                future_into_py(py, async move {
                    let out = state.pop_back().with_context(ctx).await;
                    Python::attach(|py| match out {
                        Ok(item) => item.map(|item| ($restore)(py, &env, &item)).transpose(),
                        Err(error) => Err(state_error(py, &env, &error)),
                    })
                })
            }

            /// Reads the front endpoint.
            fn peek_front<'p>(&self, py: Python<'p>) -> PyResult<Bound<'p, PyAny>> {
                let ctx = self.env.op_context(py)?;
                let state = Arc::clone(&self.state);
                let env = self.env.clone();
                future_into_py(py, async move {
                    let out = state.peek_front().with_context(ctx).await;
                    Python::attach(|py| match out {
                        Ok(item) => item.map(|item| ($restore)(py, &env, &item)).transpose(),
                        Err(error) => Err(state_error(py, &env, &error)),
                    })
                })
            }

            /// Reads the back endpoint.
            fn peek_back<'p>(&self, py: Python<'p>) -> PyResult<Bound<'p, PyAny>> {
                let ctx = self.env.op_context(py)?;
                let state = Arc::clone(&self.state);
                let env = self.env.clone();
                future_into_py(py, async move {
                    let out = state.peek_back().with_context(ctx).await;
                    Python::attach(|py| match out {
                        Ok(item) => item.map(|item| ($restore)(py, &env, &item)).transpose(),
                        Err(error) => Err(state_error(py, &env, &error)),
                    })
                })
            }

            /// Removes every element.
            fn clear<'p>(&self, py: Python<'p>) -> PyResult<Bound<'p, PyAny>> {
                let ctx = self.env.op_context(py)?;
                let state = Arc::clone(&self.state);
                let env = self.env.clone();
                future_into_py(py, async move {
                    let out = state.clear().with_context(ctx).await;
                    Python::attach(|py| out.map_err(|error| state_error(py, &env, &error)))
                })
            }

            /// Opens an element cursor.
            fn scan(&self, py: Python, direction: &str) -> PyResult<$scan> {
                let direction = parse_direction(py, &self.env, direction)?;
                let _guard = self.env.op_context(py)?.attach();
                Ok($scan::new(self.state.scan(direction), self.env.clone()))
            }

            /// Durably commits the buffered operations.
            fn commit<'p>(&self, py: Python<'p>) -> PyResult<Bound<'p, PyAny>> {
                let ctx = self.env.op_context(py)?;
                let state = Arc::clone(&self.state);
                let env = self.env.clone();
                future_into_py(py, async move {
                    let out = state.commit().with_context(ctx).await;
                    Python::attach(|py| out.map_err(|error| state_error(py, &env, &error)))
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

deque_state!(
    NativeJsonDequeState,
    Value,
    NativeJsonDequeScan,
    |py, env, item| json_write_item(py, env, item, " in a deque"),
    |py, _env: &StateEnv, item| Ok(pythonize(py, item)?.unbind())
);
deque_state!(
    NativeMessageDequeState,
    ConsumerMessage<Value>,
    NativeMessageDequeScan,
    message_write_item,
    build_message
);
