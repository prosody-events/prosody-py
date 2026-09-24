//! The presence-only set state handle.

use super::{
    Arc, Bound, BoxSetState, FutureExt, KeyQuery, NativeKeyScan, PyAny, PyResult, PyTraverseError,
    PyVisit, Python, StateEnv, future_into_py, pyclass, pymethods, state_error,
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
        let ctx = self.env.op_context(py)?;
        let state = Arc::clone(&self.state);
        let env = self.env.clone();
        future_into_py(py, async move {
            let out = state.contains(member).with_context(ctx).await;
            Python::attach(|py| out.map_err(|error| state_error(py, &env, &error)))
        })
    }

    /// Reports whether each member belongs to the set, in input order.
    fn contains_many<'p>(
        &self,
        py: Python<'p>,
        members: Vec<String>,
    ) -> PyResult<Bound<'p, PyAny>> {
        let ctx = self.env.op_context(py)?;
        let state = Arc::clone(&self.state);
        let env = self.env.clone();
        future_into_py(py, async move {
            let out = state.contains_many(members).with_context(ctx).await;
            Python::attach(|py| out.map_err(|error| state_error(py, &env, &error)))
        })
    }

    /// Reports whether the set has no members.
    fn is_empty<'p>(&self, py: Python<'p>) -> PyResult<Bound<'p, PyAny>> {
        let ctx = self.env.op_context(py)?;
        let state = Arc::clone(&self.state);
        let env = self.env.clone();
        future_into_py(py, async move {
            let out = state.is_empty().with_context(ctx).await;
            Python::attach(|py| out.map_err(|error| state_error(py, &env, &error)))
        })
    }

    /// Adds one member.
    fn insert<'p>(&self, py: Python<'p>, member: String) -> PyResult<Bound<'p, PyAny>> {
        let ctx = self.env.op_context(py)?;
        let state = Arc::clone(&self.state);
        let env = self.env.clone();
        future_into_py(py, async move {
            let out = state.insert(member).with_context(ctx).await;
            Python::attach(|py| out.map_err(|error| state_error(py, &env, &error)))
        })
    }

    /// Removes one member.
    fn remove<'p>(&self, py: Python<'p>, member: String) -> PyResult<Bound<'p, PyAny>> {
        let ctx = self.env.op_context(py)?;
        let state = Arc::clone(&self.state);
        let env = self.env.clone();
        future_into_py(py, async move {
            let out = state.remove(member).with_context(ctx).await;
            Python::attach(|py| out.map_err(|error| state_error(py, &env, &error)))
        })
    }

    /// Removes every member.
    fn clear<'p>(&self, py: Python<'p>) -> PyResult<Bound<'p, PyAny>> {
        let ctx = self.env.op_context(py)?;
        let state = Arc::clone(&self.state);
        let env = self.env.clone();
        future_into_py(py, async move {
            let out = state.clear().with_context(ctx).await;
            Python::attach(|py| out.map_err(|error| state_error(py, &env, &error)))
        })
    }

    /// Opens a member cursor.
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
            Python::attach(|py| out.map(drop).map_err(|error| state_error(py, &env, &error)))
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
