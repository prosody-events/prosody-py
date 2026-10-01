//! Deque state handles.

use super::{
    Arc, Bound, BoxDequeState, ConsumerMessage, NativeJsonDequeScan, NativeMessageDequeScan,
    PositionQuery, PyAny, PyResult, Python, StateEnv, Value, build_message, json_object,
    json_write_item, message_write_item, outcome_token, pyclass, pymethods, run, run_item,
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
                let state = Arc::clone(&self.state);
                let op = async move { state.len().await };
                run(py, &self.env, op, |_, _, len| Ok(len))
            }

            /// Reports whether the deque is empty.
            fn is_empty<'p>(&self, py: Python<'p>) -> PyResult<Bound<'p, PyAny>> {
                let state = Arc::clone(&self.state);
                let op = async move { state.is_empty().await };
                run(py, &self.env, op, |_, _, empty| Ok(empty))
            }

            /// Reads one element by its position from the front.
            fn get<'p>(&self, py: Python<'p>, index: usize) -> PyResult<Bound<'p, PyAny>> {
                let state = Arc::clone(&self.state);
                run_item(
                    py,
                    &self.env,
                    async move { state.get(index).await },
                    $restore,
                )
            }

            /// Appends one element.
            fn push_back<'p>(
                &self,
                py: Python<'p>,
                item: &Bound<'p, PyAny>,
            ) -> PyResult<Bound<'p, PyAny>> {
                let item = ($prepare)(py, &self.env, item)?;
                let state = Arc::clone(&self.state);
                let op = async move { state.push_back(item).await };
                run(py, &self.env, op, |_, _, ()| Ok(()))
            }

            /// Prepends one element.
            fn push_front<'p>(
                &self,
                py: Python<'p>,
                item: &Bound<'p, PyAny>,
            ) -> PyResult<Bound<'p, PyAny>> {
                let item = ($prepare)(py, &self.env, item)?;
                let state = Arc::clone(&self.state);
                let op = async move { state.push_front(item).await };
                run(py, &self.env, op, |_, _, ()| Ok(()))
            }

            /// Removes and returns the front element.
            fn pop_front<'p>(&self, py: Python<'p>) -> PyResult<Bound<'p, PyAny>> {
                let state = Arc::clone(&self.state);
                run_item(
                    py,
                    &self.env,
                    async move { state.pop_front().await },
                    $restore,
                )
            }

            /// Removes and returns the back element.
            fn pop_back<'p>(&self, py: Python<'p>) -> PyResult<Bound<'p, PyAny>> {
                let state = Arc::clone(&self.state);
                run_item(
                    py,
                    &self.env,
                    async move { state.pop_back().await },
                    $restore,
                )
            }

            /// Reads the front endpoint.
            fn peek_front<'p>(&self, py: Python<'p>) -> PyResult<Bound<'p, PyAny>> {
                let state = Arc::clone(&self.state);
                run_item(
                    py,
                    &self.env,
                    async move { state.peek_front().await },
                    $restore,
                )
            }

            /// Reads the back endpoint.
            fn peek_back<'p>(&self, py: Python<'p>) -> PyResult<Bound<'p, PyAny>> {
                let state = Arc::clone(&self.state);
                run_item(
                    py,
                    &self.env,
                    async move { state.peek_back().await },
                    $restore,
                )
            }

            /// Removes every element.
            fn clear<'p>(&self, py: Python<'p>) -> PyResult<Bound<'p, PyAny>> {
                let state = Arc::clone(&self.state);
                let op = async move { state.clear().await };
                run(py, &self.env, op, |_, _, ()| Ok(()))
            }

            /// Opens an element cursor.
            fn scan(&self, query: PositionQuery) -> $scan {
                $scan::new(query.stream(self.state.values()), self.env.clone())
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

deque_state!(
    NativeJsonDequeState,
    Value,
    NativeJsonDequeScan,
    json_write_item,
    json_object
);
deque_state!(
    NativeMessageDequeState,
    ConsumerMessage<Value>,
    NativeMessageDequeScan,
    message_write_item,
    build_message
);
