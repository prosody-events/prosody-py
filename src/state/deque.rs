//! Deque state handles.

use super::{
    Arc, Bound, BoxDequeState, ConsumerMessage, FutureExt, NativeJsonDequeScan,
    NativeMessageDequeScan, PositionQuery, PyAny, PyResult, PyTraverseError, PyVisit, Python,
    StateEnv, Value, build_message, future_into_py, json_write_item, message_write_item,
    outcome_token, pyclass, pymethods, pythonize, state_error, transient_error,
};

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
            fn scan(&self, py: Python, query: PositionQuery) -> PyResult<$scan> {
                let cursor = query.stream(py, &self.env, self.state.values())?;
                Ok($scan::new(cursor, self.env.clone()))
            }

            /// Durably commits the buffered operations and reports the outcome.
            fn commit<'p>(&self, py: Python<'p>) -> PyResult<Bound<'p, PyAny>> {
                let ctx = self.env.op_context(py)?;
                let state = Arc::clone(&self.state);
                let env = self.env.clone();
                future_into_py(py, async move {
                    let out = state.commit().with_context(ctx).await;
                    Python::attach(|py| {
                        out.map(outcome_token)
                            .map_err(|error| state_error(py, &env, &error))
                    })
                })
            }

            /// Discards the buffered operations and reports the outcome.
            fn rollback<'p>(&self, py: Python<'p>) -> PyResult<Bound<'p, PyAny>> {
                let ctx = self.env.op_context(py)?;
                let state = Arc::clone(&self.state);
                future_into_py(py, async move {
                    Ok(outcome_token(state.rollback().with_context(ctx).await))
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
