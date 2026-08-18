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
//! `PyO3` binding span. Scans activate the carrier while core constructs its
//! stream span; pulls transport vectors of up to 256 immediately-ready items
//! without creating per-chunk binding spans.
//!
//! Errors carry their category structurally: an [`ErasedStateError`] is raised
//! as `PermanentStateError` or `TransientStateError` by reading its
//! [`category`](ErasedStateError::category), never by parsing the message. No
//! fencing or cursor safety lives here — those are core-owned and this layer
//! only transports and restores types. Caller-mistake conditions the glue
//! detects (an unrepresentable value, a `null` write, a wrong item shape, an
//! invalid enum token, an out-of-range index) reject TRANSIENT — a caller code
//! error retries and stays visible rather than discarding the message.

use crate::message::MessageCore;
use opentelemetry::Context as OtelContext;
use opentelemetry::propagation::{TextMapCompositePropagator, TextMapPropagator};
use opentelemetry::trace::FutureExt;
use prosody::consumer::Keyed;
use prosody::consumer::event_context::{
    BoxDequeState, BoxMapState, BoxStateCursor, BoxValueState, ErasedCategory, ErasedStateError,
};
use prosody::consumer::message::ConsumerMessage;
use prosody::state::Direction;
use pyo3::exceptions::PyStopAsyncIteration;
use pyo3::gc::{PyTraverseError, PyVisit};
use pyo3::types::{PyAnyMethods, PyDict, PyString, PyTuple};
use pyo3::{Bound, Py, PyAny, PyErr, PyRef, PyResult, Python, pyclass, pymethods};
use pyo3_async_runtimes::tokio::future_into_py;
use pythonize::{depythonize, pythonize};
use serde_json::Value;
use std::collections::{HashMap, VecDeque};
use std::num::NonZeroUsize;
use std::sync::Arc;
use tokio::sync::Mutex;

mod handles;

pub(crate) use handles::*;

/// Maximum number of immediately-ready scan items transported through `PyO3`
/// in one vector. Core owns ready draining, error ordering, and pull
/// serialization; this binding owns only the transport cap and conversion.
const SCAN_READY_CHUNK_SIZE: NonZeroUsize = match NonZeroUsize::new(256) {
    Some(size) => size,
    // Unreachable: 256 is nonzero. `unwrap`/`expect` are clippy-denied.
    None => NonZeroUsize::MIN,
};

/// Cheaply-cloned per-handle environment: the OpenTelemetry carrier accessors,
/// the propagator, the cached Python `Message` class, and the three Python
/// state-error classes. Every vended handle shares one `Arc`, so cloning it per
/// vend is a refcount bump.
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
    /// The Python `NullValueError` class.
    null_value_error: Py<PyAny>,
}

impl StateEnv {
    /// Resolves the environment at vend time, looking up the three state-error
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
            null_value_error: prosody.getattr("NullValueError")?.unbind(),
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

    /// Visits the Python handles this environment holds for GC traversal.
    ///
    /// Takes `visit` by value and returns it so the caller's `__traverse__`
    /// (whose signature is fixed by `PyO3` to receive `PyVisit` by value)
    /// consumes it here rather than only borrowing it.
    fn traverse<'a>(&self, visit: PyVisit<'a>) -> Result<PyVisit<'a>, PyTraverseError> {
        visit.call(self.0.get_current.as_any())?;
        visit.call(self.0.inject.as_any())?;
        visit.call(self.0.message_class.as_any())?;
        visit.call(self.0.permanent_error.as_any())?;
        visit.call(self.0.transient_error.as_any())?;
        visit.call(self.0.null_value_error.as_any())?;
        Ok(visit)
    }
}

/// Instantiates a Python exception `class` with `message` and turns it into a
/// `PyErr`.
fn raise(py: Python, class: &Py<PyAny>, message: &str) -> PyErr {
    match class.bind(py).call1((message,)) {
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
    raise(py, class, error.message())
}

/// Builds a `TransientStateError` for a caller-caused condition the glue
/// detects (an unrepresentable value, a wrong item shape, an invalid enum
/// token, an out-of-range index).
///
/// Caller mistakes are TRANSIENT, never permanent: a permanent error discards
/// the in-flight message and can silently lose data, so a code error retries
/// and stays visible instead.
fn transient_error(py: Python, env: &StateEnv, message: &str) -> PyErr {
    raise(py, &env.0.transient_error, message)
}

/// Builds a `NullValueError` for a JSON-`null` write (a transient caller
/// mistake). `null` is not a storable value; `message` is the fully-formed
/// rejection text (the caller appends the collection's deletion verb).
fn null_value_error(py: Python, env: &StateEnv, message: &str) -> PyErr {
    raise(py, &env.0.null_value_error, message)
}

/// Parses a scan-direction token into the core [`Direction`].
///
/// # Errors
///
/// Returns a transient error if the token is neither `"forward"` nor
/// `"backward"` (a caller mistake — retries, not discarded).
pub(crate) fn parse_direction(py: Python, env: &StateEnv, direction: &str) -> PyResult<Direction> {
    match direction {
        "forward" => Ok(Direction::Forward),
        "backward" => Ok(Direction::Backward),
        other => Err(transient_error(
            py,
            env,
            &format!("direction: expected \"forward\" or \"backward\", got {other:?}"),
        )),
    }
}

/// Recovers the consumer message a delivered `Message` carries.
///
/// The dataclass fields are not enough to rebuild one, and rebuilding is
/// forbidden — see [`MessageCore`] for why. Every `Message` prosody hands to a
/// handler carries its core message, whether it arrived from the topic or was
/// read back out of a collection. One built in Python does not.
///
/// # Errors
///
/// Returns a transient error when the message carries no core message. Storing
/// something other than a delivered message is a caller mistake, and caller
/// mistakes reject transient so the event stays visible instead of being
/// discarded.
fn consumer_message(
    py: Python,
    env: &StateEnv,
    message: &Bound<PyAny>,
) -> PyResult<ConsumerMessage<Value>> {
    message
        .getattr("_core")
        .ok()
        .and_then(|core| core.cast_into::<MessageCore>().ok())
        .map(|core| core.get().message())
        .ok_or_else(|| {
            transient_error(
                py,
                env,
                "only a message prosody delivered can be stored; one built in Python carries no \
                 Kafka position to store",
            )
        })
}

/// Positionally constructs the Python `Message`, identical to `handler.rs`.
///
/// Carries the resolved message as its [`MessageCore`], so a message read back
/// out of a collection can be stored into another one and its consumer permit
/// stays held for as long as Python holds the message.
fn build_message(
    py: Python,
    env: &StateEnv,
    message: &ConsumerMessage<Value>,
) -> PyResult<Py<PyAny>> {
    let payload = pythonize(py, message.payload())?;
    let core = Py::new(py, MessageCore::new(message.clone()))?;
    let object = env.0.message_class.bind(py).call1((
        message.topic().as_ref(),
        message.partition(),
        message.offset(),
        *message.timestamp(),
        message.key().as_ref(),
        payload,
        core,
    ))?;
    Ok(object.unbind())
}

/// Prepares a JSON write.
fn json_write_item(
    py: Python,
    env: &StateEnv,
    item: &Bound<PyAny>,
    deletion_advice: &str,
) -> PyResult<Value> {
    if item.is_instance(env.0.message_class.bind(py))? {
        return Err(transient_error(
            py,
            env,
            "a Kafka-message payload cannot be stored in a JSON collection",
        ));
    }
    let value = depythonize::<Value>(item).map_err(|error| {
        transient_error(
            py,
            env,
            &format!("value is not representable as JSON: {error}"),
        )
    })?;
    if value.is_null() {
        return Err(null_value_error(
            py,
            env,
            &format!("JSON null is not a storable value{deletion_advice}"),
        ));
    }
    Ok(value)
}

struct ScanInner<T> {
    cursor: BoxStateCursor<T>,
    retained: VecDeque<T>,
}

fn json_object(py: Python, _env: &StateEnv, value: &Value) -> PyResult<Py<PyAny>> {
    Ok(pythonize(py, value)?.unbind())
}

fn json_map_entry(
    py: Python,
    _env: &StateEnv,
    (key, value): &(String, Value),
) -> PyResult<Py<PyAny>> {
    let value = pythonize(py, value)?;
    Ok(
        PyTuple::new(py, [PyString::new(py, key).into_any(), value])?
            .into_any()
            .unbind(),
    )
}

fn message_map_entry(
    py: Python,
    env: &StateEnv,
    (key, message): &(String, ConsumerMessage<Value>),
) -> PyResult<Py<PyAny>> {
    let value = build_message(py, env, message)?.into_bound(py);
    Ok(
        PyTuple::new(py, [PyString::new(py, key).into_any(), value])?
            .into_any()
            .unbind(),
    )
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
            pub(crate) fn new(cursor: BoxStateCursor<$item>, env: StateEnv) -> Self {
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
                    if let Some(item) = guard.retained.front() {
                        let object = Python::attach(|py| ($restore)(py, &env, item))?;
                        guard.retained.pop_front();
                        return Ok(object);
                    }
                    let pulled = guard
                        .cursor
                        .next_ready_chunk(SCAN_READY_CHUNK_SIZE)
                        .with_context(ctx)
                        .await;
                    match pulled {
                        Err(error) => Python::attach(|py| Err(state_error(py, &env, &error))),
                        Ok(None) => Err(PyStopAsyncIteration::new_err(())),
                        Ok(Some(items)) => {
                            guard.retained.extend(items);
                            let Some(item) = guard.retained.front() else {
                                return Err(PyStopAsyncIteration::new_err(()));
                            };
                            let object = Python::attach(|py| ($restore)(py, &env, item))?;
                            guard.retained.pop_front();
                            Ok(object)
                        }
                    }
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

            /// Traverses the Python handles this cursor holds for GC.
            fn __traverse__(&self, visit: PyVisit) -> Result<(), PyTraverseError> {
                self.env.traverse(visit).map(|_| ())
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
    |py, _env: &StateEnv, key: &String| {
        Ok::<Py<PyAny>, PyErr>(PyString::new(py, key).into_any().unbind())
    }
);
