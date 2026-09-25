//! Single-value state handles.

use super::{
    Arc, Bound, BoxValueState, ConsumerMessage, FutureExt, PyAny, PyResult, PyTraverseError,
    PyVisit, Python, StateEnv, Value, build_message, future_into_py, json_write_item,
    message_write_item, outcome_token, pyclass, pymethods, pythonize, state_error,
};

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
