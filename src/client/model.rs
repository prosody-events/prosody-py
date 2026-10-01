use super::{Arc, PythonHandler, pyclass};
use futures::FutureExt;
use futures::future::{BoxFuture, Shared};
use parking_lot::Mutex;
use prosody::high_level::erased::{ErasedConsumerState, SharedHighLevelClient};

use crate::state::StateEnv;

type Shutdown = Shared<BoxFuture<'static, Result<(), Arc<str>>>>;

/// A client for Kafka production and consumption.
#[pyclass(subclass, name = "_NativeProsodyClient")]
pub struct ProsodyClient {
    pub(super) client: SharedHighLevelClient<PythonHandler>,
    pub(super) shutdown: Shutdown,
    pub(super) env: StateEnv,
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

pub(super) fn consumer_state_name(state: &ErasedConsumerState<PythonHandler>) -> &'static str {
    match state {
        ErasedConsumerState::Shutdown => "shut_down",
        ErasedConsumerState::Unconfigured => "unconfigured",
        ErasedConsumerState::ConfigurationFailed(_) => "configuration_failed",
        ErasedConsumerState::Configured(_) => "configured",
        ErasedConsumerState::Running { .. } => "running",
    }
}
