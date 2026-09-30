//! Binds keyed-state collections for one event context.
//!
//! `bind` maps a Python definition to the matching core handle and wraps it
//! in the typed Python handle. It caches the wrapper on the `Context` for the
//! rest of the event.

use super::Context;
use crate::state::{
    NativeJsonDequeState, NativeJsonMapState, NativeJsonValueState, NativeMessageDequeState,
    NativeMessageMapState, NativeMessageValueState, NativeSetState, state_error,
};
use pyo3::exceptions::PyRuntimeError;
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

/// Builds a `TransientStateError` for a malformed or hostile state definition —
/// a caller mistake, so transient rather than a message-discarding permanent.
fn transient_state_error(prosody: &Bound<PyModule>, message: &str) -> PyErr {
    match prosody
        .getattr("TransientStateError")
        .and_then(|class| class.call1((message,)))
    {
        Ok(instance) => PyErr::from_value(instance),
        // Constructing the exception itself failed — surface that error.
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
    {
        let cache = context
            .state_handles
            .lock()
            .map_err(|_| PyRuntimeError::new_err("state cache mutex poisoned"))?;
        if let Some(existing) = cache.get(&cache_key) {
            return Ok(existing.clone_ref(py));
        }
    }

    let native: Py<PyAny> = match kind {
        StateDefinitionKind::Value => Py::new(py, value_state(context, py, &name)?)?.into_any(),
        StateDefinitionKind::Map => Py::new(py, map_state(context, py, &name)?)?.into_any(),
        StateDefinitionKind::Set => Py::new(py, set_state(context, py, &name)?)?.into_any(),
        StateDefinitionKind::Deque => Py::new(py, deque_state(context, py, &name)?)?.into_any(),
        StateDefinitionKind::MessageValue => {
            Py::new(py, message_value_state(context, py, &name)?)?.into_any()
        }
        StateDefinitionKind::MessageMap => {
            Py::new(py, message_map_state(context, py, &name)?)?.into_any()
        }
        StateDefinitionKind::MessageDeque => {
            Py::new(py, message_deque_state(context, py, &name)?)?.into_any()
        }
    };
    let wrapper_name = match kind {
        StateDefinitionKind::Value | StateDefinitionKind::MessageValue => "ValueState",
        StateDefinitionKind::Map | StateDefinitionKind::MessageMap => "MapState",
        StateDefinitionKind::Set => "SetState",
        StateDefinitionKind::Deque | StateDefinitionKind::MessageDeque => "DequeState",
    };
    let wrapper = prosody.getattr(wrapper_name)?.call1((native,))?.unbind();
    {
        let mut cache = context
            .state_handles
            .lock()
            .map_err(|_| PyRuntimeError::new_err("state cache mutex poisoned"))?;
        cache.insert(cache_key, wrapper.clone_ref(py));
    };
    Ok(wrapper)
}

/// Vends the low-level handle for the named JSON value collection.
///
/// Vending verifies the collection's registration (core-side); no span is
/// opened here — vended handles outlive the call, and every operation opens
/// its own span.
///
/// # Errors
///
/// Returns a permanent error if the name is unregistered or its registered
/// identity mismatches.
fn value_state(context: &Context, py: Python, name: &str) -> PyResult<NativeJsonValueState> {
    let env = context.state_env(py)?;
    let handle = context
        .inner
        .value_state(name)
        .map_err(|e| state_error(py, &env, &e))?;
    Ok(NativeJsonValueState {
        state: Arc::new(handle),
        env,
    })
}

/// Vends the low-level handle for the named JSON map collection.
///
/// # Errors
///
/// Returns a permanent error if the name is unregistered or its registered
/// identity mismatches.
fn map_state(context: &Context, py: Python, name: &str) -> PyResult<NativeJsonMapState> {
    let env = context.state_env(py)?;
    let handle = context
        .inner
        .map_state(name)
        .map_err(|e| state_error(py, &env, &e))?;
    Ok(NativeJsonMapState {
        state: Arc::new(handle),
        env,
    })
}

/// Vends the low-level handle for the named set collection.
///
/// # Errors
///
/// Returns a permanent error if the name is unregistered or its registered
/// identity mismatches.
fn set_state(context: &Context, py: Python, name: &str) -> PyResult<NativeSetState> {
    let env = context.state_env(py)?;
    let handle = context
        .inner
        .set_state(name)
        .map_err(|e| state_error(py, &env, &e))?;
    Ok(NativeSetState {
        state: Arc::new(handle),
        env,
    })
}

/// Vends the low-level handle for the named JSON deque collection.
///
/// # Errors
///
/// Returns a permanent error if the name is unregistered or its registered
/// identity mismatches.
fn deque_state(context: &Context, py: Python, name: &str) -> PyResult<NativeJsonDequeState> {
    let env = context.state_env(py)?;
    let handle = context
        .inner
        .deque_state(name)
        .map_err(|e| state_error(py, &env, &e))?;
    Ok(NativeJsonDequeState {
        state: Arc::new(handle),
        env,
    })
}

/// Vends the low-level handle for the named Kafka-message value collection.
///
/// Items are the full `Message` the handler received, loader-resolved on
/// read.
///
/// # Errors
///
/// Returns a permanent error if the name is unregistered or its registered
/// identity mismatches.
fn message_value_state(
    context: &Context,
    py: Python,
    name: &str,
) -> PyResult<NativeMessageValueState> {
    let env = context.state_env(py)?;
    let handle = context
        .inner
        .message_value_state(name)
        .map_err(|e| state_error(py, &env, &e))?;
    Ok(NativeMessageValueState {
        state: Arc::new(handle),
        env,
    })
}

/// Vends the low-level handle for the named Kafka-message map collection.
///
/// Items are the full `Message` the handler received, loader-resolved on
/// read.
///
/// # Errors
///
/// Returns a permanent error if the name is unregistered or its registered
/// identity mismatches.
fn message_map_state(context: &Context, py: Python, name: &str) -> PyResult<NativeMessageMapState> {
    let env = context.state_env(py)?;
    let handle = context
        .inner
        .message_map_state(name)
        .map_err(|e| state_error(py, &env, &e))?;
    Ok(NativeMessageMapState {
        state: Arc::new(handle),
        env,
    })
}

/// Vends the low-level handle for the named Kafka-message deque collection.
///
/// Items are the full `Message` the handler received, loader-resolved on
/// read.
///
/// # Errors
///
/// Returns a permanent error if the name is unregistered or its registered
/// identity mismatches.
fn message_deque_state(
    context: &Context,
    py: Python,
    name: &str,
) -> PyResult<NativeMessageDequeState> {
    let env = context.state_env(py)?;
    let handle = context
        .inner
        .message_deque_state(name)
        .map_err(|e| state_error(py, &env, &e))?;
    Ok(NativeMessageDequeState {
        state: Arc::new(handle),
        env,
    })
}
