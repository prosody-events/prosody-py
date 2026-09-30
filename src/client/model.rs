use super::{
    Arc, Bound, ErasedReadCache, Py, PyAny, PyAnyMethods, PyResult, Python, PythonHandler, pyclass,
};
use futures::FutureExt;
use futures::future::{BoxFuture, Shared};
use parking_lot::Mutex;
use prosody::high_level::erased::{ErasedConsumerState, SharedHighLevelClient};
use prosody::propagator::new_propagator;
use pyo3::exceptions::{PyTypeError, PyValueError};
use pyo3::types::PyBool;
use std::time::Duration;

use crate::state::StateEnv;

type Shutdown = Shared<BoxFuture<'static, Result<(), Arc<str>>>>;

/// A client for Kafka production and consumption.
#[pyclass(subclass, name = "_NativeProsodyClient")]
pub struct ProsodyClient {
    pub(super) client: SharedHighLevelClient<PythonHandler>,
    pub(super) shutdown: Shutdown,
    pub(super) get_context: Py<PyAny>,
    pub(super) inject: Py<PyAny>,
    pub(super) handler: Arc<Mutex<Option<PythonHandler>>>,
    pub(super) pid: u32,
}

pub(super) fn shutdown(client: &SharedHighLevelClient<PythonHandler>) -> Shutdown {
    let client = client.clone();
    async move {
        client
            .shutdown()
            .await
            .map_err(|error| Arc::from(error.to_string()))
    }
    .boxed()
    .shared()
}

pub(super) fn parse_read_cache(value: Option<&Bound<'_, PyAny>>) -> PyResult<ErasedReadCache> {
    let Some(value) = value else {
        return Ok(ErasedReadCache::Inherit);
    };
    if value.is_instance_of::<PyBool>() {
        return if value.extract::<bool>()? {
            Err(PyValueError::new_err(
                "read_cache=True is ambiguous; pass a duration, False, or None",
            ))
        } else {
            Ok(ErasedReadCache::Disabled)
        };
    }
    let seconds = if let Ok(seconds) = value.extract::<f64>() {
        seconds
    } else if let Ok(total_seconds) = value.getattr("total_seconds") {
        total_seconds.call0()?.extract::<f64>()?
    } else {
        return Err(PyTypeError::new_err(
            "read_cache must be seconds, timedelta, False, or None",
        ));
    };
    let ttl = Duration::try_from_secs_f64(seconds)
        .map_err(|_| PyValueError::new_err("read_cache must be finite and non-negative"))?;
    Ok(ErasedReadCache::Ttl(ttl))
}

/// Builds the shared environment that a published reader holds.
pub(super) fn published_env(client: &ProsodyClient, py: Python) -> PyResult<StateEnv> {
    let message_class = py.import("prosody")?.getattr("Message")?.unbind();
    StateEnv::resolve(
        py,
        &client.get_context,
        &client.inject,
        Arc::new(new_propagator()),
        &message_class,
    )
}

pub(super) fn consumer_state_name(state: &ErasedConsumerState<PythonHandler>) -> &'static str {
    match state {
        ErasedConsumerState::Shutdown => "shut_down",
        ErasedConsumerState::Unconfigured => "unconfigured",
        ErasedConsumerState::ConfigurationFailed(_) => "configuration_failed",
        ErasedConsumerState::Configured(_) => "configured",
        ErasedConsumerState::Running { .. } => "running",
    }
}
