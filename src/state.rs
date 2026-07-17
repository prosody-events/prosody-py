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
//! stream
//! span; pulls transport vectors of up to 256 immediately-ready items without
//! creating per-chunk binding spans.
//!
//! Errors carry their category structurally: an [`ErasedStateError`] is raised
//! as `PermanentStateError` or `TransientStateError` by reading its
//! [`category`](ErasedStateError::category), never by parsing the message. No
//! fencing or cursor safety lives here — those are core-owned and this layer
//! only transports and restores types. Caller-mistake conditions the glue
//! detects (an unrepresentable value, a `null` write, a wrong item shape, an
//! invalid enum token, an out-of-range index) reject TRANSIENT — a caller code
//! error retries and stays visible rather than discarding the message.

use chrono::{DateTime, Utc};
use opentelemetry::Context as OtelContext;
use opentelemetry::propagation::{TextMapCompositePropagator, TextMapPropagator};
use opentelemetry::trace::FutureExt;
use prosody::consumer::Keyed;
use prosody::consumer::event_context::{
    BoxDequeState, BoxMapState, BoxStateCursor, BoxValueState, ErasedCategory, ErasedStateError,
};
use prosody::consumer::message::{ConsumerMessage, ConsumerMessageValue};
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
use tokio::sync::{Mutex, Semaphore};
use tracing::Span;

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
/// mistake). `null` is not a storable value; `advice` names the deletion verb.
fn null_value_error(py: Python, env: &StateEnv, message: &str) -> PyErr {
    raise(py, &env.0.null_value_error, message)
}

/// Parses a scan-direction token into the core [`Direction`].
///
/// # Errors
///
/// Returns a transient error if the token is neither `"forward"` nor
/// `"backward"` (a caller mistake — retries, not discarded).
fn parse_direction(py: Python, env: &StateEnv, direction: &str) -> PyResult<Direction> {
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

/// Rebuilds a `ConsumerMessage<Value>` from the Python `Message` for a
/// message-collection write.
///
/// Only topic/partition/offset/key/timestamp/payload feed the stored value; a
/// fresh single-permit semaphore supplies the (never-contended) processing
/// permit. Mirrors the JS `consumer_message()`.
///
/// # Errors
///
/// Returns a transient error if an attribute is missing or the payload is not
/// representable as JSON, and propagates the raw extraction error (an
/// out-of-range offset raises `OverflowError`, transient by default).
fn consumer_message(
    py: Python,
    env: &StateEnv,
    message: &Bound<PyAny>,
) -> PyResult<ConsumerMessage<Value>> {
    let topic: String = message.getattr("topic")?.extract()?;
    let partition: i32 = message.getattr("partition")?.extract()?;
    let offset: i64 = message.getattr("offset")?.extract()?;
    let timestamp: DateTime<Utc> = message.getattr("timestamp")?.extract()?;
    let key: String = message.getattr("key")?.extract()?;
    let payload: Value = depythonize::<Value>(&message.getattr("payload")?).map_err(|error| {
        transient_error(
            py,
            env,
            &format!("message.payload is not representable as JSON: {error}"),
        )
    })?;
    let permit = Arc::new(Semaphore::new(1))
        .try_acquire_owned()
        .map_err(|error| {
            transient_error(
                py,
                env,
                &format!("failed to acquire message permit: {error}"),
            )
        })?;
    let value = ConsumerMessageValue {
        source_system: None,
        topic: topic.as_str().into(),
        partition,
        offset,
        key: key.into(),
        timestamp,
        payload,
    };
    Ok(ConsumerMessage::new(value, Span::current(), permit))
}

/// The collection's item payload kind, used to route a write and its item
/// build.
#[derive(Clone, Copy)]
enum Kind {
    /// JSON values.
    Json,
    /// The full Kafka message the handler received.
    Message,
}

/// An item crossing INTO a state handle, restored to its native shape.
enum StateItem {
    /// A JSON value.
    Json(Value),
    /// A Kafka message.
    Message(ConsumerMessage<Value>),
}

impl StateItem {
    /// Restores the item to its Python shape (GIL held).
    fn to_py(&self, py: Python, env: &StateEnv) -> PyResult<Py<PyAny>> {
        match self {
            StateItem::Json(value) => Ok(pythonize(py, value)?.unbind()),
            StateItem::Message(message) => build_message(py, env, message),
        }
    }
}

/// Positionally constructs the Python `Message`, identical to `handler.rs`.
fn build_message(
    py: Python,
    env: &StateEnv,
    message: &ConsumerMessage<Value>,
) -> PyResult<Py<PyAny>> {
    let payload = pythonize(py, message.payload())?;
    let object = env.0.message_class.bind(py).call1((
        message.topic().as_ref(),
        message.partition(),
        message.offset(),
        *message.timestamp(),
        message.key().as_ref(),
        payload,
    ))?;
    Ok(object.unbind())
}

/// Prepares a write item, detecting the JSON/message kind mismatch and the
/// JSON-`null` write (both transient caller mistakes).
///
/// `deletion_advice` names the deletion verb for the collection kind, appended
/// to the null-rejection message.
///
/// # Errors
///
/// Returns a transient error on a kind mismatch or an unrepresentable value,
/// and a null-value error on a JSON-`null` write.
fn prepare_item(
    py: Python,
    env: &StateEnv,
    kind: Kind,
    item: &Bound<PyAny>,
    deletion_advice: &str,
) -> PyResult<StateItem> {
    let is_message = item.is_instance(env.0.message_class.bind(py))?;
    match kind {
        Kind::Json => {
            if is_message {
                return Err(transient_error(
                    py,
                    env,
                    "a Kafka-message payload cannot be stored in a JSON collection",
                ));
            }
            let value: Value = depythonize::<Value>(item).map_err(|error| {
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
            Ok(StateItem::Json(value))
        }
        Kind::Message => {
            if is_message {
                Ok(StateItem::Message(consumer_message(py, env, item)?))
            } else {
                Err(transient_error(py, env, "expected a Kafka message"))
            }
        }
    }
}

/// The two payload flavours a value handle wraps.
pub(crate) enum ValueStateVariant {
    /// A JSON value collection.
    Json(BoxValueState<Value>),
    /// A Kafka-message collection.
    Message(BoxValueState<ConsumerMessage<Value>>),
}

impl ValueStateVariant {
    /// The payload kind of this variant.
    fn kind(&self) -> Kind {
        match self {
            ValueStateVariant::Json(_) => Kind::Json,
            ValueStateVariant::Message(_) => Kind::Message,
        }
    }
}

/// The two payload flavours a map handle wraps.
pub(crate) enum MapStateVariant {
    /// A JSON value collection.
    Json(BoxMapState<Value>),
    /// A Kafka-message collection.
    Message(BoxMapState<ConsumerMessage<Value>>),
}

impl MapStateVariant {
    /// The payload kind of this variant.
    fn kind(&self) -> Kind {
        match self {
            MapStateVariant::Json(_) => Kind::Json,
            MapStateVariant::Message(_) => Kind::Message,
        }
    }
}

/// The two payload flavours a deque handle wraps.
pub(crate) enum DequeStateVariant {
    /// A JSON value collection.
    Json(BoxDequeState<Value>),
    /// A Kafka-message collection.
    Message(BoxDequeState<ConsumerMessage<Value>>),
}

impl DequeStateVariant {
    /// The payload kind of this variant.
    fn kind(&self) -> Kind {
        match self {
            DequeStateVariant::Json(_) => Kind::Json,
            DequeStateVariant::Message(_) => Kind::Message,
        }
    }
}

/// Erased single-value state handle, vended per event.
#[pyclass]
pub struct NativeValueState {
    /// The wrapped erased value handle.
    pub(crate) state: Arc<ValueStateVariant>,
    /// The shared per-handle environment.
    pub(crate) env: StateEnv,
}

#[pymethods]
impl NativeValueState {
    /// Reads the current value, or `None` when absent/cleared.
    fn get<'p>(&self, py: Python<'p>) -> PyResult<Bound<'p, PyAny>> {
        let ctx = self.env.op_context(py)?;
        let state = Arc::clone(&self.state);
        let env = self.env.clone();
        future_into_py(py, async move {
            let out = match &*state {
                ValueStateVariant::Json(handle) => handle
                    .get()
                    .with_context(ctx)
                    .await
                    .map(|item| item.map(StateItem::Json)),
                ValueStateVariant::Message(handle) => handle
                    .get()
                    .with_context(ctx)
                    .await
                    .map(|item| item.map(StateItem::Message)),
            };
            Python::attach(|py| match out {
                Ok(item) => item.map(|item| item.to_py(py, &env)).transpose(),
                Err(error) => Err(state_error(py, &env, &error)),
            })
        })
    }

    /// Buffers a write of the value.
    ///
    /// A JSON `null` is rejected (transient), naming `clear()` to delete; a
    /// kind mismatch is likewise transient.
    fn set<'p>(&self, py: Python<'p>, item: &Bound<'p, PyAny>) -> PyResult<Bound<'p, PyAny>> {
        let ctx = self.env.op_context(py)?;
        let prepared = prepare_item(
            py,
            &self.env,
            self.state.kind(),
            item,
            "; use clear() to remove the value",
        )?;
        let state = Arc::clone(&self.state);
        let env = self.env.clone();
        future_into_py(py, async move {
            let out = match (&*state, prepared) {
                (ValueStateVariant::Json(handle), StateItem::Json(value)) => {
                    handle.set(value).with_context(ctx).await
                }
                (ValueStateVariant::Message(handle), StateItem::Message(message)) => {
                    handle.set(message).with_context(ctx).await
                }
                (ValueStateVariant::Json(_), StateItem::Message(_))
                | (ValueStateVariant::Message(_), StateItem::Json(_)) => {
                    return Python::attach(|py| {
                        Err(transient_error(py, &env, "item/collection kind mismatch"))
                    });
                }
            };
            Python::attach(|py| out.map_err(|error| state_error(py, &env, &error)))
        })
    }

    /// Buffers a clear of the value.
    fn clear<'p>(&self, py: Python<'p>) -> PyResult<Bound<'p, PyAny>> {
        let ctx = self.env.op_context(py)?;
        let state = Arc::clone(&self.state);
        let env = self.env.clone();
        future_into_py(py, async move {
            let out = match &*state {
                ValueStateVariant::Json(handle) => handle.clear().with_context(ctx).await,
                ValueStateVariant::Message(handle) => handle.clear().with_context(ctx).await,
            };
            Python::attach(|py| out.map_err(|error| state_error(py, &env, &error)))
        })
    }

    /// Durably commits the buffered operations mid-handler.
    fn commit<'p>(&self, py: Python<'p>) -> PyResult<Bound<'p, PyAny>> {
        let ctx = self.env.op_context(py)?;
        let state = Arc::clone(&self.state);
        let env = self.env.clone();
        future_into_py(py, async move {
            let out = match &*state {
                ValueStateVariant::Json(handle) => handle.commit().with_context(ctx).await,
                ValueStateVariant::Message(handle) => handle.commit().with_context(ctx).await,
            };
            Python::attach(|py| out.map_err(|error| state_error(py, &env, &error)))
        })
    }

    /// Discards the buffered uncommitted operations (infallible).
    fn rollback<'p>(&self, py: Python<'p>) -> PyResult<Bound<'p, PyAny>> {
        let ctx = self.env.op_context(py)?;
        let state = Arc::clone(&self.state);
        future_into_py(py, async move {
            match &*state {
                ValueStateVariant::Json(handle) => handle.rollback().with_context(ctx).await,
                ValueStateVariant::Message(handle) => handle.rollback().with_context(ctx).await,
            }
            Ok(())
        })
    }

    /// Traverses the Python handles this state holds for GC.
    fn __traverse__(&self, visit: PyVisit) -> Result<(), PyTraverseError> {
        self.env.traverse(visit).map(|_| ())
    }
}

/// Erased ordered-map state handle, keyed by `String`, vended per event.
#[pyclass]
pub struct NativeMapState {
    /// The wrapped erased map handle.
    pub(crate) state: Arc<MapStateVariant>,
    /// The shared per-handle environment.
    pub(crate) env: StateEnv,
}

#[pymethods]
impl NativeMapState {
    /// Reads the value for `key`, or `None` when absent.
    fn get<'p>(&self, py: Python<'p>, key: String) -> PyResult<Bound<'p, PyAny>> {
        let ctx = self.env.op_context(py)?;
        let state = Arc::clone(&self.state);
        let env = self.env.clone();
        future_into_py(py, async move {
            let out = match &*state {
                MapStateVariant::Json(handle) => handle
                    .get(key)
                    .with_context(ctx)
                    .await
                    .map(|item| item.map(StateItem::Json)),
                MapStateVariant::Message(handle) => handle
                    .get(key)
                    .with_context(ctx)
                    .await
                    .map(|item| item.map(StateItem::Message)),
            };
            Python::attach(|py| match out {
                Ok(item) => item.map(|item| item.to_py(py, &env)).transpose(),
                Err(error) => Err(state_error(py, &env, &error)),
            })
        })
    }

    /// Reads several keys in one isolated batch, one result per input key in
    /// order (`None` for an absent key). A straight pass-through of the erased
    /// batch API.
    fn get_many<'p>(&self, py: Python<'p>, keys: Vec<String>) -> PyResult<Bound<'p, PyAny>> {
        let ctx = self.env.op_context(py)?;
        let state = Arc::clone(&self.state);
        let env = self.env.clone();
        future_into_py(py, async move {
            let out = match &*state {
                MapStateVariant::Json(handle) => handle
                    .get_many(keys)
                    .with_context(ctx)
                    .await
                    .map(|items| items.into_iter().map(|i| i.map(StateItem::Json)).collect()),
                MapStateVariant::Message(handle) => {
                    handle.get_many(keys).with_context(ctx).await.map(|items| {
                        items
                            .into_iter()
                            .map(|i| i.map(StateItem::Message))
                            .collect()
                    })
                }
            };
            Python::attach(|py| match out {
                Ok(items) => {
                    let items: Vec<Option<StateItem>> = items;
                    items
                        .into_iter()
                        .map(|item| item.map(|item| item.to_py(py, &env)).transpose())
                        .collect::<PyResult<Vec<Option<Py<PyAny>>>>>()
                }
                Err(error) => Err(state_error(py, &env, &error)),
            })
        })
    }

    /// Inserts or overwrites `key`.
    ///
    /// A JSON `null` is rejected (transient), naming `remove(key)` to delete; a
    /// kind mismatch is likewise transient.
    fn set<'p>(
        &self,
        py: Python<'p>,
        key: String,
        item: &Bound<'p, PyAny>,
    ) -> PyResult<Bound<'p, PyAny>> {
        let ctx = self.env.op_context(py)?;
        let prepared = prepare_item(
            py,
            &self.env,
            self.state.kind(),
            item,
            "; use remove(key) to remove the entry",
        )?;
        let state = Arc::clone(&self.state);
        let env = self.env.clone();
        future_into_py(py, async move {
            let out = match (&*state, prepared) {
                (MapStateVariant::Json(handle), StateItem::Json(value)) => {
                    handle.set(key, value).with_context(ctx).await
                }
                (MapStateVariant::Message(handle), StateItem::Message(message)) => {
                    handle.set(key, message).with_context(ctx).await
                }
                (MapStateVariant::Json(_), StateItem::Message(_))
                | (MapStateVariant::Message(_), StateItem::Json(_)) => {
                    return Python::attach(|py| {
                        Err(transient_error(py, &env, "item/collection kind mismatch"))
                    });
                }
            };
            Python::attach(|py| out.map_err(|error| state_error(py, &env, &error)))
        })
    }

    /// Removes `key`.
    fn remove<'p>(&self, py: Python<'p>, key: String) -> PyResult<Bound<'p, PyAny>> {
        let ctx = self.env.op_context(py)?;
        let state = Arc::clone(&self.state);
        let env = self.env.clone();
        future_into_py(py, async move {
            let out = match &*state {
                MapStateVariant::Json(handle) => handle.remove(key).with_context(ctx).await,
                MapStateVariant::Message(handle) => handle.remove(key).with_context(ctx).await,
            };
            Python::attach(|py| out.map_err(|error| state_error(py, &env, &error)))
        })
    }

    /// Removes every entry.
    fn clear<'p>(&self, py: Python<'p>) -> PyResult<Bound<'p, PyAny>> {
        let ctx = self.env.op_context(py)?;
        let state = Arc::clone(&self.state);
        let env = self.env.clone();
        future_into_py(py, async move {
            let out = match &*state {
                MapStateVariant::Json(handle) => handle.clear().with_context(ctx).await,
                MapStateVariant::Message(handle) => handle.clear().with_context(ctx).await,
            };
            Python::attach(|py| out.map_err(|error| state_error(py, &env, &error)))
        })
    }

    /// Opens a demand-driven cursor over the live entries in key order.
    ///
    /// Synchronous — no I/O. The extracted carrier is active while core builds
    /// its stream span. Entries are yielded as `(key, value)` pairs.
    fn scan(&self, py: Python, direction: &str) -> PyResult<NativeStateScan> {
        let dir = parse_direction(py, &self.env, direction)?;
        let _guard = self.env.op_context(py)?.attach();
        let scan = match &*self.state {
            MapStateVariant::Json(handle) => ScanState::MapJson {
                cursor: handle.scan(dir),
                retained: VecDeque::new(),
            },
            MapStateVariant::Message(handle) => ScanState::MapMessage {
                cursor: handle.scan(dir),
                retained: VecDeque::new(),
            },
        };
        Ok(NativeStateScan {
            inner: Arc::new(Mutex::new(scan)),
            env: self.env.clone(),
        })
    }

    /// Durably commits the buffered operations mid-handler.
    fn commit<'p>(&self, py: Python<'p>) -> PyResult<Bound<'p, PyAny>> {
        let ctx = self.env.op_context(py)?;
        let state = Arc::clone(&self.state);
        let env = self.env.clone();
        future_into_py(py, async move {
            let out = match &*state {
                MapStateVariant::Json(handle) => handle.commit().with_context(ctx).await,
                MapStateVariant::Message(handle) => handle.commit().with_context(ctx).await,
            };
            Python::attach(|py| out.map_err(|error| state_error(py, &env, &error)))
        })
    }

    /// Discards the buffered uncommitted operations (infallible).
    fn rollback<'p>(&self, py: Python<'p>) -> PyResult<Bound<'p, PyAny>> {
        let ctx = self.env.op_context(py)?;
        let state = Arc::clone(&self.state);
        future_into_py(py, async move {
            match &*state {
                MapStateVariant::Json(handle) => handle.rollback().with_context(ctx).await,
                MapStateVariant::Message(handle) => handle.rollback().with_context(ctx).await,
            }
            Ok(())
        })
    }

    /// Traverses the Python handles this state holds for GC.
    fn __traverse__(&self, visit: PyVisit) -> Result<(), PyTraverseError> {
        self.env.traverse(visit).map(|_| ())
    }
}

/// Erased deque state handle, vended per event.
#[pyclass]
pub struct NativeDequeState {
    /// The wrapped erased deque handle.
    pub(crate) state: Arc<DequeStateVariant>,
    /// The shared per-handle environment.
    pub(crate) env: StateEnv,
}

#[pymethods]
impl NativeDequeState {
    /// The number of live elements.
    fn len<'p>(&self, py: Python<'p>) -> PyResult<Bound<'p, PyAny>> {
        let ctx = self.env.op_context(py)?;
        let state = Arc::clone(&self.state);
        let env = self.env.clone();
        future_into_py(py, async move {
            let out = match &*state {
                DequeStateVariant::Json(handle) => handle.len().with_context(ctx).await,
                DequeStateVariant::Message(handle) => handle.len().with_context(ctx).await,
            };
            Python::attach(|py| match out {
                Ok(len) => u32::try_from(len).map_err(|_| {
                    transient_error(
                        py,
                        &env,
                        &format!("deque length {len} exceeds the u32 range"),
                    )
                }),
                Err(error) => Err(state_error(py, &env, &error)),
            })
        })
    }

    /// Whether the deque holds no live elements.
    fn is_empty<'p>(&self, py: Python<'p>) -> PyResult<Bound<'p, PyAny>> {
        let ctx = self.env.op_context(py)?;
        let state = Arc::clone(&self.state);
        let env = self.env.clone();
        future_into_py(py, async move {
            let out = match &*state {
                DequeStateVariant::Json(handle) => handle.is_empty().with_context(ctx).await,
                DequeStateVariant::Message(handle) => handle.is_empty().with_context(ctx).await,
            };
            Python::attach(|py| out.map_err(|error| state_error(py, &env, &error)))
        })
    }

    /// Reads the element at front-relative position `index`, or `None` past the
    /// end.
    fn get<'p>(&self, py: Python<'p>, index: u32) -> PyResult<Bound<'p, PyAny>> {
        let ctx = self.env.op_context(py)?;
        let index = index as usize;
        let state = Arc::clone(&self.state);
        let env = self.env.clone();
        future_into_py(py, async move {
            let out = match &*state {
                DequeStateVariant::Json(handle) => handle
                    .get(index)
                    .with_context(ctx)
                    .await
                    .map(|item| item.map(StateItem::Json)),
                DequeStateVariant::Message(handle) => handle
                    .get(index)
                    .with_context(ctx)
                    .await
                    .map(|item| item.map(StateItem::Message)),
            };
            Python::attach(|py| match out {
                Ok(item) => item.map(|item| item.to_py(py, &env)).transpose(),
                Err(error) => Err(state_error(py, &env, &error)),
            })
        })
    }

    /// Appends an element at the back.
    ///
    /// A JSON `null` is rejected (transient); a kind mismatch is likewise
    /// transient.
    fn push_back<'p>(&self, py: Python<'p>, item: &Bound<'p, PyAny>) -> PyResult<Bound<'p, PyAny>> {
        let ctx = self.env.op_context(py)?;
        let prepared = prepare_item(py, &self.env, self.state.kind(), item, " in a deque")?;
        let state = Arc::clone(&self.state);
        let env = self.env.clone();
        future_into_py(py, async move {
            let out = match (&*state, prepared) {
                (DequeStateVariant::Json(handle), StateItem::Json(value)) => {
                    handle.push_back(value).with_context(ctx).await
                }
                (DequeStateVariant::Message(handle), StateItem::Message(message)) => {
                    handle.push_back(message).with_context(ctx).await
                }
                (DequeStateVariant::Json(_), StateItem::Message(_))
                | (DequeStateVariant::Message(_), StateItem::Json(_)) => {
                    return Python::attach(|py| {
                        Err(transient_error(py, &env, "item/collection kind mismatch"))
                    });
                }
            };
            Python::attach(|py| out.map_err(|error| state_error(py, &env, &error)))
        })
    }

    /// Prepends an element at the front.
    ///
    /// A JSON `null` is rejected (transient); a kind mismatch is likewise
    /// transient.
    fn push_front<'p>(
        &self,
        py: Python<'p>,
        item: &Bound<'p, PyAny>,
    ) -> PyResult<Bound<'p, PyAny>> {
        let ctx = self.env.op_context(py)?;
        let prepared = prepare_item(py, &self.env, self.state.kind(), item, " in a deque")?;
        let state = Arc::clone(&self.state);
        let env = self.env.clone();
        future_into_py(py, async move {
            let out = match (&*state, prepared) {
                (DequeStateVariant::Json(handle), StateItem::Json(value)) => {
                    handle.push_front(value).with_context(ctx).await
                }
                (DequeStateVariant::Message(handle), StateItem::Message(message)) => {
                    handle.push_front(message).with_context(ctx).await
                }
                (DequeStateVariant::Json(_), StateItem::Message(_))
                | (DequeStateVariant::Message(_), StateItem::Json(_)) => {
                    return Python::attach(|py| {
                        Err(transient_error(py, &env, "item/collection kind mismatch"))
                    });
                }
            };
            Python::attach(|py| out.map_err(|error| state_error(py, &env, &error)))
        })
    }

    /// Removes and returns the front element, or `None` when empty.
    fn pop_front<'p>(&self, py: Python<'p>) -> PyResult<Bound<'p, PyAny>> {
        let ctx = self.env.op_context(py)?;
        let state = Arc::clone(&self.state);
        let env = self.env.clone();
        future_into_py(py, async move {
            let out = match &*state {
                DequeStateVariant::Json(handle) => handle
                    .pop_front()
                    .with_context(ctx)
                    .await
                    .map(|item| item.map(StateItem::Json)),
                DequeStateVariant::Message(handle) => handle
                    .pop_front()
                    .with_context(ctx)
                    .await
                    .map(|item| item.map(StateItem::Message)),
            };
            Python::attach(|py| match out {
                Ok(item) => item.map(|item| item.to_py(py, &env)).transpose(),
                Err(error) => Err(state_error(py, &env, &error)),
            })
        })
    }

    /// Removes and returns the back element, or `None` when empty.
    fn pop_back<'p>(&self, py: Python<'p>) -> PyResult<Bound<'p, PyAny>> {
        let ctx = self.env.op_context(py)?;
        let state = Arc::clone(&self.state);
        let env = self.env.clone();
        future_into_py(py, async move {
            let out = match &*state {
                DequeStateVariant::Json(handle) => handle
                    .pop_back()
                    .with_context(ctx)
                    .await
                    .map(|item| item.map(StateItem::Json)),
                DequeStateVariant::Message(handle) => handle
                    .pop_back()
                    .with_context(ctx)
                    .await
                    .map(|item| item.map(StateItem::Message)),
            };
            Python::attach(|py| match out {
                Ok(item) => item.map(|item| item.to_py(py, &env)).transpose(),
                Err(error) => Err(state_error(py, &env, &error)),
            })
        })
    }

    /// Removes every element.
    fn clear<'p>(&self, py: Python<'p>) -> PyResult<Bound<'p, PyAny>> {
        let ctx = self.env.op_context(py)?;
        let state = Arc::clone(&self.state);
        let env = self.env.clone();
        future_into_py(py, async move {
            let out = match &*state {
                DequeStateVariant::Json(handle) => handle.clear().with_context(ctx).await,
                DequeStateVariant::Message(handle) => handle.clear().with_context(ctx).await,
            };
            Python::attach(|py| out.map_err(|error| state_error(py, &env, &error)))
        })
    }

    /// Opens a demand-driven cursor over the live elements in index order.
    ///
    /// Synchronous — no I/O. The extracted carrier is active while core builds
    /// its stream span.
    fn scan(&self, py: Python, direction: &str) -> PyResult<NativeStateScan> {
        let dir = parse_direction(py, &self.env, direction)?;
        let _guard = self.env.op_context(py)?.attach();
        let scan = match &*self.state {
            DequeStateVariant::Json(handle) => ScanState::DequeJson {
                cursor: handle.scan(dir),
                retained: VecDeque::new(),
            },
            DequeStateVariant::Message(handle) => ScanState::DequeMessage {
                cursor: handle.scan(dir),
                retained: VecDeque::new(),
            },
        };
        Ok(NativeStateScan {
            inner: Arc::new(Mutex::new(scan)),
            env: self.env.clone(),
        })
    }

    /// Durably commits the buffered operations mid-handler.
    fn commit<'p>(&self, py: Python<'p>) -> PyResult<Bound<'p, PyAny>> {
        let ctx = self.env.op_context(py)?;
        let state = Arc::clone(&self.state);
        let env = self.env.clone();
        future_into_py(py, async move {
            let out = match &*state {
                DequeStateVariant::Json(handle) => handle.commit().with_context(ctx).await,
                DequeStateVariant::Message(handle) => handle.commit().with_context(ctx).await,
            };
            Python::attach(|py| out.map_err(|error| state_error(py, &env, &error)))
        })
    }

    /// Discards the buffered uncommitted operations (infallible).
    fn rollback<'p>(&self, py: Python<'p>) -> PyResult<Bound<'p, PyAny>> {
        let ctx = self.env.op_context(py)?;
        let state = Arc::clone(&self.state);
        future_into_py(py, async move {
            match &*state {
                DequeStateVariant::Json(handle) => handle.rollback().with_context(ctx).await,
                DequeStateVariant::Message(handle) => handle.rollback().with_context(ctx).await,
            }
            Ok(())
        })
    }

    /// Traverses the Python handles this state holds for GC.
    fn __traverse__(&self, visit: PyVisit) -> Result<(), PyTraverseError> {
        self.env.traverse(visit).map(|_| ())
    }
}

/// The four scan flavours, each holding its cursor arm plus the retained tail
/// of a pulled ready chunk not yet handed to Python.
enum ScanState {
    /// A deque JSON scan yielding values.
    DequeJson {
        /// The erased cursor.
        cursor: BoxStateCursor<Value>,
        /// Pulled-but-unyielded items.
        retained: VecDeque<Value>,
    },
    /// A map JSON scan yielding `(key, value)` entries.
    MapJson {
        /// The erased cursor.
        cursor: BoxStateCursor<(String, Value)>,
        /// Pulled-but-unyielded items.
        retained: VecDeque<(String, Value)>,
    },
    /// A deque message scan yielding messages.
    DequeMessage {
        /// The erased cursor.
        cursor: BoxStateCursor<ConsumerMessage<Value>>,
        /// Pulled-but-unyielded items.
        retained: VecDeque<ConsumerMessage<Value>>,
    },
    /// A map message scan yielding `(key, message)` entries.
    MapMessage {
        /// The erased cursor.
        cursor: BoxStateCursor<(String, ConsumerMessage<Value>)>,
        /// Pulled-but-unyielded items.
        retained: VecDeque<(String, ConsumerMessage<Value>)>,
    },
}

/// Builds the Python object for the FRONT retained item without removing it.
///
/// Peek-then-pop: the caller only pops after this succeeds, so a conversion
/// failure leaves the item retained for a deterministic re-attempt rather than
/// silently dropping it. Returns `None` when nothing is retained.
///
/// Holds no `await`; the GIL is acquired synchronously to run only trusted,
/// non-reentrant Python (the frozen `Message` constructor and `pythonize` of
/// JSON primitives), so it cannot re-enter `__anext__`/`aclose`.
fn scan_build_front(scan: &ScanState, env: &StateEnv) -> PyResult<Option<Py<PyAny>>> {
    match scan {
        ScanState::DequeJson { retained, .. } => match retained.front() {
            None => Ok(None),
            Some(value) => Python::attach(|py| Ok(Some(pythonize(py, value)?.unbind()))),
        },
        ScanState::MapJson { retained, .. } => match retained.front() {
            None => Ok(None),
            Some((key, value)) => Python::attach(|py| {
                let value = pythonize(py, value)?;
                let entry = PyTuple::new(py, [PyString::new(py, key).into_any(), value])?;
                Ok(Some(entry.into_any().unbind()))
            }),
        },
        ScanState::DequeMessage { retained, .. } => match retained.front() {
            None => Ok(None),
            Some(message) => Python::attach(|py| Ok(Some(build_message(py, env, message)?))),
        },
        ScanState::MapMessage { retained, .. } => match retained.front() {
            None => Ok(None),
            Some((key, message)) => Python::attach(|py| {
                let value = build_message(py, env, message)?.into_bound(py);
                let entry = PyTuple::new(py, [PyString::new(py, key).into_any(), value])?;
                Ok(Some(entry.into_any().unbind()))
            }),
        },
    }
}

/// Removes the front retained item (dropping it so it can be GC'd).
fn scan_pop_front(scan: &mut ScanState) {
    match scan {
        ScanState::DequeJson { retained, .. } => {
            retained.pop_front();
        }
        ScanState::MapJson { retained, .. } => {
            retained.pop_front();
        }
        ScanState::DequeMessage { retained, .. } => {
            retained.pop_front();
        }
        ScanState::MapMessage { retained, .. } => {
            retained.pop_front();
        }
    }
}

/// Drops every retained item (on explicit close).
fn scan_clear_retained(scan: &mut ScanState) {
    match scan {
        ScanState::DequeJson { retained, .. } => retained.clear(),
        ScanState::MapJson { retained, .. } => retained.clear(),
        ScanState::DequeMessage { retained, .. } => retained.clear(),
        ScanState::MapMessage { retained, .. } => retained.clear(),
    }
}

/// Demand-driven async iterator over a map or deque collection.
///
/// Pulling is lazy: each `__anext__` restores the carrier without a binding
/// span, awaits core's next ready chunk (up to 256 items), and hands them to
/// Python one at a time. Chunking, exhaustion, deferred-error ordering,
/// cancellation safety, and close-idempotence are core-owned; this layer owns
/// only the retained-vector flattening and its serialization behind one mutex.
#[pyclass]
pub struct NativeStateScan {
    /// The cursor arm plus its retained tail, serialized so concurrent
    /// `__anext__`/`aclose` calls acquire in order.
    inner: Arc<Mutex<ScanState>>,
    /// The shared per-handle environment.
    env: StateEnv,
}

#[pymethods]
impl NativeStateScan {
    /// Returns the iterator itself.
    fn __aiter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    /// Yields the next scanned item, or raises `StopAsyncIteration` at
    /// exhaustion.
    fn __anext__<'p>(&self, py: Python<'p>) -> PyResult<Bound<'p, PyAny>> {
        let ctx = self.env.op_context(py)?;
        let inner = Arc::clone(&self.inner);
        let env = self.env.clone();
        future_into_py(py, async move {
            let mut guard = inner.lock().await;
            // Drain the retained tail first (peek-then-pop).
            if let Some(object) = scan_build_front(&guard, &env)? {
                scan_pop_front(&mut guard);
                return Ok(object);
            }
            // Retained empty: pull one ready chunk with the carrier active.
            let pulled = match &mut *guard {
                ScanState::DequeJson { cursor, retained } => cursor
                    .next_ready_chunk(SCAN_READY_CHUNK_SIZE)
                    .with_context(ctx)
                    .await
                    .map(|chunk| chunk.map(|items| retained.extend(items))),
                ScanState::MapJson { cursor, retained } => cursor
                    .next_ready_chunk(SCAN_READY_CHUNK_SIZE)
                    .with_context(ctx)
                    .await
                    .map(|chunk| chunk.map(|items| retained.extend(items))),
                ScanState::DequeMessage { cursor, retained } => cursor
                    .next_ready_chunk(SCAN_READY_CHUNK_SIZE)
                    .with_context(ctx)
                    .await
                    .map(|chunk| chunk.map(|items| retained.extend(items))),
                ScanState::MapMessage { cursor, retained } => cursor
                    .next_ready_chunk(SCAN_READY_CHUNK_SIZE)
                    .with_context(ctx)
                    .await
                    .map(|chunk| chunk.map(|items| retained.extend(items))),
            };
            match pulled {
                Err(error) => Python::attach(|py| Err(state_error(py, &env, &error))),
                Ok(None) => Err(PyStopAsyncIteration::new_err(())),
                Ok(Some(())) => {
                    // Core guarantees `Some` is non-empty; the `None` arm is
                    // defensive and never taken.
                    match scan_build_front(&guard, &env)? {
                        Some(object) => {
                            scan_pop_front(&mut guard);
                            Ok(object)
                        }
                        None => Err(PyStopAsyncIteration::new_err(())),
                    }
                }
            }
        })
    }

    /// Closes the cursor, dropping the retained tail and releasing the stream.
    /// Idempotent; a subsequent `__anext__` raises a terminated error.
    fn aclose<'p>(&self, py: Python<'p>) -> PyResult<Bound<'p, PyAny>> {
        let inner = Arc::clone(&self.inner);
        future_into_py(py, async move {
            let mut guard = inner.lock().await;
            scan_clear_retained(&mut guard);
            match &*guard {
                ScanState::DequeJson { cursor, .. } => cursor.close().await,
                ScanState::MapJson { cursor, .. } => cursor.close().await,
                ScanState::DequeMessage { cursor, .. } => cursor.close().await,
                ScanState::MapMessage { cursor, .. } => cursor.close().await,
            }
            Ok(())
        })
    }

    /// Traverses the Python handles this scan holds for GC.
    fn __traverse__(&self, visit: PyVisit) -> Result<(), PyTraverseError> {
        self.env.traverse(visit).map(|_| ())
    }
}
