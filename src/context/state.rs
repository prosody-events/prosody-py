//! Binds keyed-state collections for one event context.
//!
//! `bind` maps a Python definition to the matching core handle and wraps it
//! in the typed Python handle. It caches the wrapper on the `Context` for the
//! rest of the event.

use super::{Context, state_env};
use crate::state::{
    NativeJsonDequeState, NativeJsonMapState, NativeJsonValueState, NativeMessageDequeState,
    NativeMessageMapState, NativeMessageValueState, NativeSetState, StateEnv, raise, state_error,
};
use prosody::consumer::event_context::ErasedStateError;
use pyo3::types::{PyAnyMethods, PyModule};
use pyo3::{Bound, Py, PyAny, PyErr, PyResult, Python};
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

/// Builds a `TransientStateError` for a malformed or hostile state definition.
///
/// A malformed definition is a caller mistake, so it is transient.
fn transient_state_error(prosody: &Bound<PyModule>, message: &str) -> PyErr {
    match prosody.getattr("TransientStateError") {
        Ok(class) => raise(&class, message),
        Err(error) => error,
    }
}

fn state_definition_kind(
    prosody: &Bound<PyModule>,
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
    Err(transient_state_error(
        prosody,
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

    let kind = state_definition_kind(&prosody, definition)?;
    let name = definition
        .getattr("name")
        .and_then(|name| name.extract::<String>())
        .map_err(|_| transient_state_error(&prosody, "state: definition name must be a string"))?;
    let cache_key = (kind, name.clone());
    if let Some(existing) = context.state_handles.lock().get(&cache_key) {
        return Ok(existing.clone_ref(py));
    }

    let env = state_env(context, py)?;
    let inner = &context.inner;
    let native: Py<PyAny> = match kind {
        StateDefinitionKind::Value => {
            let state = vend(py, &env, inner.value_state(&name))?;
            Py::new(py, NativeJsonValueState { state, env })?.into_any()
        }
        StateDefinitionKind::Map => {
            let state = vend(py, &env, inner.map_state(&name))?;
            Py::new(py, NativeJsonMapState { state, env })?.into_any()
        }
        StateDefinitionKind::Set => {
            let state = vend(py, &env, inner.set_state(&name))?;
            Py::new(py, NativeSetState { state, env })?.into_any()
        }
        StateDefinitionKind::Deque => {
            let state = vend(py, &env, inner.deque_state(&name))?;
            Py::new(py, NativeJsonDequeState { state, env })?.into_any()
        }
        StateDefinitionKind::MessageValue => {
            let state = vend(py, &env, inner.message_value_state(&name))?;
            Py::new(py, NativeMessageValueState { state, env })?.into_any()
        }
        StateDefinitionKind::MessageMap => {
            let state = vend(py, &env, inner.message_map_state(&name))?;
            Py::new(py, NativeMessageMapState { state, env })?.into_any()
        }
        StateDefinitionKind::MessageDeque => {
            let state = vend(py, &env, inner.message_deque_state(&name))?;
            Py::new(py, NativeMessageDequeState { state, env })?.into_any()
        }
    };
    let wrapper_name = match kind {
        StateDefinitionKind::Value | StateDefinitionKind::MessageValue => "ValueState",
        StateDefinitionKind::Map | StateDefinitionKind::MessageMap => "MapState",
        StateDefinitionKind::Set => "SetState",
        StateDefinitionKind::Deque | StateDefinitionKind::MessageDeque => "DequeState",
    };
    let wrapper = prosody.getattr(wrapper_name)?.call1((native,))?.unbind();
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
