//! The presence-only set state handle.

use super::{
    Arc, Bound, BoxSetState, KeyQuery, NativeMapKeyScan, PyAny, PyResult, Python, StateEnv,
    outcome_token, pyclass, pymethods, run,
};

/// Ordered set state handle over string members.
#[pyclass]
pub struct NativeSetState {
    pub(crate) state: Arc<BoxSetState>,
    pub(crate) env: StateEnv,
}

#[pymethods]
impl NativeSetState {
    /// Reports whether `member` belongs to the set.
    fn contains<'p>(&self, py: Python<'p>, member: String) -> PyResult<Bound<'p, PyAny>> {
        let state = Arc::clone(&self.state);
        let op = async move { state.contains(member).await };
        run(py, &self.env, op, |_, _, found| Ok(found))
    }

    /// Reports whether each member belongs to the set, in input order.
    fn contains_many<'p>(
        &self,
        py: Python<'p>,
        members: Vec<String>,
    ) -> PyResult<Bound<'p, PyAny>> {
        let state = Arc::clone(&self.state);
        let op = async move { state.contains_many(members).await };
        run(py, &self.env, op, |_, _, found| Ok(found))
    }

    /// Reports whether the set has no members.
    fn is_empty<'p>(&self, py: Python<'p>) -> PyResult<Bound<'p, PyAny>> {
        let state = Arc::clone(&self.state);
        let op = async move { state.is_empty().await };
        run(py, &self.env, op, |_, _, empty| Ok(empty))
    }

    /// Adds one member.
    fn insert<'p>(&self, py: Python<'p>, member: String) -> PyResult<Bound<'p, PyAny>> {
        let state = Arc::clone(&self.state);
        let op = async move { state.insert(member).await };
        run(py, &self.env, op, |_, _, ()| Ok(()))
    }

    /// Removes one member.
    fn remove<'p>(&self, py: Python<'p>, member: String) -> PyResult<Bound<'p, PyAny>> {
        let state = Arc::clone(&self.state);
        let op = async move { state.remove(member).await };
        run(py, &self.env, op, |_, _, ()| Ok(()))
    }

    /// Removes every member.
    fn clear<'p>(&self, py: Python<'p>) -> PyResult<Bound<'p, PyAny>> {
        let state = Arc::clone(&self.state);
        let op = async move { state.clear().await };
        run(py, &self.env, op, |_, _, ()| Ok(()))
    }

    /// Opens a member cursor.
    fn keys(&self, query: KeyQuery) -> NativeMapKeyScan {
        NativeMapKeyScan::new(query.stream(self.state.keys()), self.env.clone())
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
