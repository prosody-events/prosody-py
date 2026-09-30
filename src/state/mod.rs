//! Erased native layer for keyed state.
//!
//! Wraps the boxed erased handles from [`prosody::consumer::event_context`] as
//! `#[pyclass]` types. Collections are addressed by name; JSON payloads cross
//! as `serde_json::Value` (the `pythonize`/`depythonize` bridge, exactly like
//! message payloads) and Kafka-message items cross as the same `Message` object
//! shape handlers already receive.
//!
//! Every operation reads the Python-side OpenTelemetry carrier while the GIL is
//! held, then activates it while polling the erased future off the GIL, letting
//! core's semantic collection span join the event trace without an extra
//! `PyO3` binding span. Opening a scan performs no read. Each pull activates
//! the carrier, and core starts its stream span on the first pull. Pulls
//! transport vectors of up to 256 immediately-ready items without creating
//! per-chunk binding spans.
//!
//! Errors carry their category structurally: an [`ErasedStateError`] is raised
//! as `PermanentStateError` or `TransientStateError` by reading its
//! [`category`](ErasedStateError::category), never by parsing the message. No
//! fencing or cursor safety lives here — those are core-owned and this layer
//! only transports and restores types. Caller-mistake conditions the glue
//! detects (an unrepresentable value, a wrong item shape) reject TRANSIENT — a
//! caller code error retries and stays visible rather than discarding the
//! message. Core rejects a JSON null write as permanent.

use crate::message::{MessageCore, PythonRecord};
use opentelemetry::Context as OtelContext;
use opentelemetry::propagation::{TextMapCompositePropagator, TextMapPropagator};
use opentelemetry::trace::FutureExt;
use prosody::consumer::event_context::{
    BoxDequeState, BoxMapState, BoxSetState, BoxValueState, ErasedCategory, ErasedStateError,
    StateCursor,
};
use prosody::consumer::message::ConsumerMessage;
use prosody::state::StoreOutcome;
use pyo3::exceptions::PyStopAsyncIteration;
use pyo3::types::{PyAnyMethods, PyDict};
use pyo3::{
    Bound, IntoPyObject, IntoPyObjectExt, Py, PyAny, PyErr, PyRef, PyResult, Python, pyclass,
    pymethods,
};
use pyo3_async_runtimes::tokio::future_into_py;
use pythonize::{depythonize, pythonize};
use serde_json::Value;
use std::collections::{HashMap, VecDeque};
use std::future::Future;
use std::num::NonZeroUsize;
use std::sync::Arc;
use tokio::sync::Mutex;

mod deque;
mod map;
mod query;
mod set;
mod value;

pub(crate) use deque::{NativeJsonDequeState, NativeMessageDequeState};
pub(crate) use map::{NativeJsonMapState, NativeMessageMapState};
pub(crate) use query::{KeyQuery, PositionQuery};
pub(crate) use set::NativeSetState;
pub(crate) use value::{NativeJsonValueState, NativeMessageValueState};

/// Maximum number of immediately-ready scan items transported through `PyO3`
/// in one vector. Core owns ready draining, error ordering, and pull
/// serialization; this binding owns only the transport cap and conversion.
const SCAN_READY_CHUNK_SIZE: NonZeroUsize = match NonZeroUsize::new(256) {
    Some(size) => size,
    // Unreachable: 256 is nonzero. `unwrap`/`expect` are clippy-denied.
    None => NonZeroUsize::MIN,
};

/// Cheaply-cloned per-handle environment: the OpenTelemetry carrier accessors,
/// the propagator, the cached Python `Message` class, and the two Python
/// state-error classes. A handle and every cursor it opens share one `Arc`.
///
/// The handles do not visit these objects for GC traversal. They are
/// module-level objects that cannot form a cycle through a handle, and many
/// handles share one reference to each, so a visit from each handle would
/// break the `tp_traverse` contract.
#[derive(Clone)]
pub(crate) struct StateEnv(Arc<StateEnvInner>);

/// The shared, immutable contents of a [`StateEnv`].
struct StateEnvInner {
    /// `opentelemetry.context.get_current`.
    get_current: Py<PyAny>,
    /// `opentelemetry.propagate.inject`.
    inject: Py<PyAny>,
    /// The propagator used to extract the active carrier per operation.
    propagator: Arc<TextMapCompositePropagator>,
    /// The Python `Message` class, positionally constructed like `handler.rs`.
    message_class: Py<PyAny>,
    /// The Python `PermanentStateError` class.
    permanent_error: Py<PyAny>,
    /// The Python `TransientStateError` class.
    transient_error: Py<PyAny>,
}

impl StateEnv {
    /// Resolves the environment at vend time, looking up the two state-error
    /// classes from the `prosody` package.
    ///
    /// The classes are defined in the Python layer; resolving them here (rather
    /// than at handler init) keeps a client that never vends state working even
    /// before that layer exists.
    ///
    /// # Errors
    ///
    /// Returns a `PyErr` if the `prosody` import or a class lookup fails.
    pub(crate) fn resolve(
        py: Python,
        get_current: &Py<PyAny>,
        inject: &Py<PyAny>,
        propagator: Arc<TextMapCompositePropagator>,
        message_class: &Py<PyAny>,
    ) -> PyResult<Self> {
        let prosody = py.import("prosody")?;
        Ok(Self(Arc::new(StateEnvInner {
            get_current: get_current.clone_ref(py),
            inject: inject.clone_ref(py),
            propagator,
            message_class: message_class.clone_ref(py),
            permanent_error: prosody.getattr("PermanentStateError")?.unbind(),
            transient_error: prosody.getattr("TransientStateError")?.unbind(),
        })))
    }

    /// Reads the active Python OpenTelemetry carrier into an activatable
    /// context (GIL held).
    fn op_context(&self, py: Python) -> PyResult<OtelContext> {
        let inner = &self.0;
        let context = inner.get_current.bind(py).call0()?;
        let data = PyDict::new(py);
        inner.inject.call1(py, (&data, context))?;
        let headers: HashMap<String, String> = data.extract()?;
        Ok(inner.propagator.extract(&headers))
    }
}

/// Instantiates a Python exception `class` with `message` and turns it into a
/// `PyErr`.
pub(crate) fn raise(class: &Bound<PyAny>, message: &str) -> PyErr {
    match class.call1((message,)) {
        Ok(instance) => PyErr::from_value(instance),
        // Constructing the exception itself failed — surface that error.
        Err(error) => error,
    }
}

/// Converts an erased state error into the matching Python exception, selecting
/// the class by structural category (never by parsing the message).
pub(crate) fn state_error(py: Python, env: &StateEnv, error: &ErasedStateError) -> PyErr {
    let class = match error.category() {
        ErasedCategory::Permanent => &env.0.permanent_error,
        ErasedCategory::Transient => &env.0.transient_error,
    };
    raise(class.bind(py), error.message())
}

/// Runs one state operation off the GIL and returns its Python awaitable.
///
/// The operation runs inside the caller's OpenTelemetry context. An
/// [`ErasedStateError`] raises by its category. `convert` builds the Python
/// result under the GIL.
pub(crate) fn run<'p, F, C, T, R>(
    py: Python<'p>,
    env: &StateEnv,
    operation: F,
    convert: C,
) -> PyResult<Bound<'p, PyAny>>
where
    F: Future<Output = Result<T, ErasedStateError>> + Send + 'static,
    C: FnOnce(Python, &StateEnv, T) -> PyResult<R> + Send + 'static,
    R: for<'py> IntoPyObject<'py> + Send + 'static,
{
    let ctx = env.op_context(py)?;
    let env = env.clone();
    future_into_py(py, async move {
        let out = operation.with_context(ctx).await;
        Python::attach(|py| match out {
            Ok(value) => convert(py, &env, value),
            Err(error) => Err(state_error(py, &env, &error)),
        })
    })
}

/// Runs a read of one optional stored item, like [`run`], and converts the
/// item with `restore`.
pub(crate) fn run_item<'p, F, T>(
    py: Python<'p>,
    env: &StateEnv,
    read: F,
    restore: fn(Python, &StateEnv, &T) -> PyResult<Py<PyAny>>,
) -> PyResult<Bound<'p, PyAny>>
where
    F: Future<Output = Result<Option<T>, ErasedStateError>> + Send + 'static,
    T: 'static,
{
    run(py, env, read, move |py, env, item| {
        item.map(|item| restore(py, env, &item)).transpose()
    })
}

/// Builds a `TransientStateError` for a caller-caused condition the glue
/// detects (an unrepresentable value or a wrong item shape).
///
/// Caller mistakes are TRANSIENT, never permanent: a permanent error discards
/// the in-flight message and can silently lose data, so a code error retries
/// and stays visible instead.
fn transient_error(py: Python, env: &StateEnv, message: &str) -> PyErr {
    raise(env.0.transient_error.bind(py), message)
}

/// Names a commit or rollback outcome with the token of the Python
/// `StoreOutcome` enum.
fn outcome_token(outcome: StoreOutcome) -> &'static str {
    match outcome {
        StoreOutcome::Applied => "applied",
        StoreOutcome::NoOp => "no_op",
    }
}

/// Builds the Python `Message` for a message read out of a collection.
///
/// The message carries its [`MessageCore`], so it can be stored into another
/// collection, and its consumer permit stays held while Python holds it.
fn build_message(
    py: Python,
    env: &StateEnv,
    message: &ConsumerMessage<Value>,
) -> PyResult<Py<PyAny>> {
    message.to_python(py, &env.0.message_class)
}

/// Prepares a JSON write.
///
/// A JSON null passes through. Core rejects a null write as permanent.
fn json_write_item(py: Python, env: &StateEnv, item: &Bound<PyAny>) -> PyResult<Value> {
    if item.is_instance(env.0.message_class.bind(py))? {
        return Err(transient_error(
            py,
            env,
            "a Kafka-message payload cannot be stored in a JSON collection",
        ));
    }
    depythonize::<Value>(item).map_err(|error| {
        transient_error(
            py,
            env,
            &format!("value is not representable as JSON: {error}"),
        )
    })
}

/// Prepares a Kafka-message write from the consumer message that a delivered
/// `Message` carries.
///
/// The dataclass fields are not enough to rebuild one, and rebuilding is
/// forbidden — see [`MessageCore`] for why. Every `Message` prosody hands to a
/// handler carries its core message, whether it arrived from the topic or was
/// read back out of a collection. One built in Python does not.
///
/// # Errors
///
/// Returns a transient error when `item` carries no core message. Storing
/// something other than a delivered message is a caller mistake, and caller
/// mistakes reject transient so the event stays visible instead of being
/// discarded.
fn message_write_item(
    py: Python,
    env: &StateEnv,
    item: &Bound<PyAny>,
) -> PyResult<ConsumerMessage<Value>> {
    item.getattr("_core")
        .ok()
        .and_then(|core| core.cast_into::<MessageCore>().ok())
        .map(|core| core.get().message())
        .ok_or_else(|| {
            transient_error(
                py,
                env,
                "expected a Kafka message that prosody delivered; one built in Python carries no \
                 Kafka position to store",
            )
        })
}

struct ScanInner<T> {
    cursor: StateCursor<T>,
    retained: VecDeque<T>,
}

/// Converts a stored JSON value into a Python object.
pub(crate) fn json_object(py: Python, _env: &StateEnv, value: &Value) -> PyResult<Py<PyAny>> {
    Ok(pythonize(py, value)?.unbind())
}

fn json_map_entry(
    py: Python,
    env: &StateEnv,
    (key, value): &(String, Value),
) -> PyResult<Py<PyAny>> {
    (key, json_object(py, env, value)?).into_py_any(py)
}

fn message_map_entry(
    py: Python,
    env: &StateEnv,
    (key, message): &(String, ConsumerMessage<Value>),
) -> PyResult<Py<PyAny>> {
    (key, build_message(py, env, message)?).into_py_any(py)
}

macro_rules! native_scan {
    ($name:ident, $item:ty, $restore:expr) => {
        /// Demand-driven state cursor with one element type.
        #[pyclass]
        pub struct $name {
            inner: Arc<Mutex<ScanInner<$item>>>,
            env: StateEnv,
        }

        impl $name {
            pub(crate) fn new(cursor: StateCursor<$item>, env: StateEnv) -> Self {
                Self {
                    inner: Arc::new(Mutex::new(ScanInner {
                        cursor,
                        retained: VecDeque::new(),
                    })),
                    env,
                }
            }
        }

        #[pymethods]
        impl $name {
            /// Returns this iterator.
            fn __aiter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
                slf
            }

            /// Yields the next item.
            fn __anext__<'p>(&self, py: Python<'p>) -> PyResult<Bound<'p, PyAny>> {
                let ctx = self.env.op_context(py)?;
                let inner = Arc::clone(&self.inner);
                let env = self.env.clone();
                future_into_py(py, async move {
                    let mut guard = inner.lock().await;
                    if guard.retained.is_empty() {
                        let pulled = guard
                            .cursor
                            .next_ready_chunk(SCAN_READY_CHUNK_SIZE)
                            .with_context(ctx)
                            .await;
                        match pulled {
                            Err(error) => {
                                return Python::attach(|py| Err(state_error(py, &env, &error)));
                            }
                            Ok(None) => return Err(PyStopAsyncIteration::new_err(())),
                            Ok(Some(items)) => guard.retained.extend(items),
                        }
                    }
                    let Some(item) = guard.retained.front() else {
                        return Err(PyStopAsyncIteration::new_err(()));
                    };
                    let object = Python::attach(|py| ($restore)(py, &env, item))?;
                    guard.retained.pop_front();
                    Ok(object)
                })
            }

            /// Closes the cursor.
            fn aclose<'p>(&self, py: Python<'p>) -> PyResult<Bound<'p, PyAny>> {
                let inner = Arc::clone(&self.inner);
                future_into_py(py, async move {
                    let mut guard = inner.lock().await;
                    guard.retained.clear();
                    guard.cursor.close().await;
                    Ok(())
                })
            }
        }
    };
}

native_scan!(NativeJsonDequeScan, Value, json_object);
native_scan!(NativeJsonMapScan, (String, Value), json_map_entry);
native_scan!(
    NativeMessageDequeScan,
    ConsumerMessage<Value>,
    build_message
);
native_scan!(
    NativeMessageMapScan,
    (String, ConsumerMessage<Value>),
    message_map_entry
);
native_scan!(
    NativeMapKeyScan,
    String,
    |py, _env: &StateEnv, key: &String| key.into_py_any(py)
);
