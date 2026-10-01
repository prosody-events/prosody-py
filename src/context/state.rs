//! Binds keyed-state collections for one event context.
//!
//! `bind` maps a Python definition to the matching core handle and wraps it
//! in the typed Python handle. It caches the wrapper on the `Context` for the
//! rest of the event.

use super::Context;
use crate::state::{
    NativeJsonDequeState, NativeJsonMapState, NativeJsonValueState, NativeMessageDequeState,
    NativeMessageMapState, NativeMessageValueState, NativeSetState, StateEnv, state_error,
    transient_error,
};
use prosody::consumer::event_context::ErasedStateError;
use pyo3::types::{PyAnyMethods, PyModule};
use pyo3::{Bound, Py, PyAny, PyResult, Python};
use std::sync::Arc;

/// The definition class that selects a collection's handle and wrapper.
#[derive(Clone, Copy, Eq, Hash, PartialEq)]
pub(crate) enum StateDefinitionKind {
    Value,
    Map,
    Set,
    Deque,
    MessageValue,
    MessageMap,
    MessageDeque,
}

fn state_definition_kind(
    prosody: &Bound<PyModule>,
    env: &StateEnv,
    definition: &Bound<PyAny>,
) -> PyResult<StateDefinitionKind> {
    const CLASSES: [(&str, StateDefinitionKind); 7] = [
        ("ValueDefinition", StateDefinitionKind::Value),
        ("MapDefinition", StateDefinitionKind::Map),
        ("SetDefinition", StateDefinitionKind::Set),
        ("DequeDefinition", StateDefinitionKind::Deque),
        ("MessageValueDefinition", StateDefinitionKind::MessageValue),
        ("MessageMapDefinition", StateDefinitionKind::MessageMap),
        ("MessageDequeDefinition", StateDefinitionKind::MessageDeque),
    ];
    for (name, kind) in CLASSES {
        if definition.is_instance(&prosody.getattr(name)?)? {
            return Ok(kind);
        }
    }
    Err(transient_error(
        prosody.py(),
        env,
        "state: definition must come from a Prosody state definition constructor",
    ))
}

/// Binds the collection that `definition` names and returns its typed Python
/// wrapper. `Context.state` documents the contract.
///
/// # Errors
///
/// Returns `TransientStateError` for a malformed definition, and the
/// permanent state error that the vend raises for an unregistered one.
pub(super) fn bind(
    context: &Context,
    py: Python,
    definition: &Bound<PyAny>,
) -> PyResult<Py<PyAny>> {
    let prosody = py.import("prosody")?;

    let kind = state_definition_kind(&prosody, &context.env, definition)?;
    let name = definition
        .getattr("name")
        .and_then(|name| name.extract::<String>())
        .map_err(|_| {
            transient_error(py, &context.env, "state: definition name must be a string")
        })?;
    let cache_key = (kind, name.clone());
    if let Some(existing) = context.state_handles.lock().get(&cache_key) {
        return Ok(existing.clone_ref(py));
    }

    let env = context.env.clone();
    let inner = &context.inner;
    let (native, wrapper) = match kind {
        StateDefinitionKind::Value => {
            let state = vend(py, &env, inner.value_state(&name))?;
            let handle = Py::new(py, NativeJsonValueState { state, env })?.into_any();
            (handle, "ValueState")
        }
        StateDefinitionKind::Map => {
            let state = vend(py, &env, inner.map_state(&name))?;
            let handle = Py::new(py, NativeJsonMapState { state, env })?.into_any();
            (handle, "MapState")
        }
        StateDefinitionKind::Set => {
            let state = vend(py, &env, inner.set_state(&name))?;
            let handle = Py::new(py, NativeSetState { state, env })?.into_any();
            (handle, "SetState")
        }
        StateDefinitionKind::Deque => {
            let state = vend(py, &env, inner.deque_state(&name))?;
            let handle = Py::new(py, NativeJsonDequeState { state, env })?.into_any();
            (handle, "DequeState")
        }
        StateDefinitionKind::MessageValue => {
            let state = vend(py, &env, inner.message_value_state(&name))?;
            let handle = Py::new(py, NativeMessageValueState { state, env })?.into_any();
            (handle, "ValueState")
        }
        StateDefinitionKind::MessageMap => {
            let state = vend(py, &env, inner.message_map_state(&name))?;
            let handle = Py::new(py, NativeMessageMapState { state, env })?.into_any();
            (handle, "MapState")
        }
        StateDefinitionKind::MessageDeque => {
            let state = vend(py, &env, inner.message_deque_state(&name))?;
            let handle = Py::new(py, NativeMessageDequeState { state, env })?.into_any();
            (handle, "DequeState")
        }
    };
    let wrapper = prosody.getattr(wrapper)?.call1((native,))?.unbind();
    context
        .state_handles
        .lock()
        .insert(cache_key, wrapper.clone_ref(py));
    Ok(wrapper)
}

/// Wraps the core handle that a vend returned.
///
/// Core checks the collection's registration when it vends the handle. The
/// vend opens no span: the handle outlives the call, and every operation opens
/// its own span.
///
/// # Errors
///
/// Returns a permanent error if the name is unregistered or its registered
/// identity mismatches.
fn vend<H>(py: Python, env: &StateEnv, handle: Result<H, ErasedStateError>) -> PyResult<Arc<H>> {
    handle
        .map(Arc::new)
        .map_err(|error| state_error(py, env, &error))
}
