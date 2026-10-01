//! Single-value state handles.

use super::{
    Arc, Bound, BoxValueState, ConsumerMessage, PyAny, PyResult, Python, StateEnv, Value,
    build_message, json_object, json_write_item, message_write_item, outcome_token, pyclass,
    pymethods, run, run_item,
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
                let state = Arc::clone(&self.state);
                run_item(py, &self.env, async move { state.get().await }, $restore)
            }

            /// Buffers a write of the value.
            fn set<'p>(
                &self,
                py: Python<'p>,
                item: &Bound<'p, PyAny>,
            ) -> PyResult<Bound<'p, PyAny>> {
                let item = ($prepare)(py, &self.env, item)?;
                let state = Arc::clone(&self.state);
                let op = async move { state.set(item).await };
                run(py, &self.env, op, |_, _, ()| Ok(()))
            }

            /// Buffers a clear of the value.
            fn clear<'p>(&self, py: Python<'p>) -> PyResult<Bound<'p, PyAny>> {
                let state = Arc::clone(&self.state);
                let op = async move { state.clear().await };
                run(py, &self.env, op, |_, _, ()| Ok(()))
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

value_state!(NativeJsonValueState, Value, json_write_item, json_object);
value_state!(
    NativeMessageValueState,
    ConsumerMessage<Value>,
    message_write_item,
    build_message
);
