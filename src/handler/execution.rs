use super::{
    ClassifyError, ConsumerMessage, Context, DateTime, Error, ErrorCategory, EventContext, Future,
    HashMap, IntoPyDict, Keyed, MessageCore, MessageExecutionContext, Mutex, Py, PyAny,
    PyAnyMethods, PyErr, PyResult, Python, TimerExecutionContext, Trigger, Utc, error,
    into_future_with_locals, pythonize,
};

pub(super) trait PythonRecord {
    fn into_python(self, py: Python<'_>, class: &Py<PyAny>) -> PyResult<Py<PyAny>>;
}

impl PythonRecord for ConsumerMessage<serde_json::Value> {
    fn into_python(self, py: Python<'_>, class: &Py<PyAny>) -> PyResult<Py<PyAny>> {
        let payload = pythonize(py, self.payload())?;
        let core = Py::new(py, MessageCore::new(self.clone()))?;
        class.call1(
            py,
            (
                self.topic().as_ref(),
                self.partition(),
                self.offset(),
                *self.timestamp(),
                self.key().as_ref(),
                payload,
                core,
            ),
        )
    }
}

impl PythonRecord for ConsumerMessage<()> {
    fn into_python(self, py: Python<'_>, class: &Py<PyAny>) -> PyResult<Py<PyAny>> {
        class.call1(
            py,
            (
                self.topic().as_ref(),
                self.partition(),
                self.offset(),
                *self.timestamp(),
                self.key().as_ref(),
            ),
        )
    }
}

/// Logs Python exceptions with full traceback information.
///
/// # Arguments
///
/// * `result` - `PyResult` containing a potential Python error to log
///
/// # Returns
///
/// A `PyResult` indicating whether logging succeeded
///
/// # Errors
///
/// Returns `PyErr` if accessing traceback information fails
pub(super) fn log_exception(kind: &str, result: &PyResult<Py<PyAny>>) -> PyResult<()> {
    let Err(error) = result else {
        return Ok(());
    };

    Python::attach(|py| {
        let traceback = py.import("traceback")?;
        let exc_info = (error.get_type(py), error.value(py), error.traceback(py));

        let traceback: Vec<String> = traceback
            .getattr("format_exception")?
            .call1(exc_info)?
            .extract()?;

        let traceback = traceback.join("");

        error!(%traceback, "{kind} handling failed: {error:#}");
        Ok(())
    })
}

/// Cancels a Python task by signaling its shutdown event
///
/// # Arguments
///
/// * `event_set_method` - Python Event.set method
/// * `shutdown_event` - Event to signal
///
/// # Errors
///
/// Returns `PyErr` if setting the event fails
pub(super) fn cancel_task(event_set_method: &Py<PyAny>, shutdown_event: Py<PyAny>) -> PyResult<()> {
    Python::attach(|py| {
        event_set_method.call1(py, (shutdown_event,))?;
        Ok(())
    })
}

/// Prepares and executes a Python message handler
///
/// # Arguments
///
/// * `context` - Message context
/// * `message` - Kafka message
/// * `serialized_context` - OpenTelemetry context
/// * `message_class` - Python Message class
/// * `event_class` - Python Event class
/// * `handle_method` - Python handler method
/// * `locals` - Python event loop task locals
///
/// # Returns
///
/// Tuple of (shutdown event, handler future)
///
/// # Errors
///
/// Returns `PyErr` on Python object creation/method call failures
pub(super) fn execute<C, P>(
    context: C,
    message: ConsumerMessage<P>,
    serialized_context: HashMap<String, String>,
    execution_context: MessageExecutionContext<'_>,
) -> PyResult<(
    Py<PyAny>,
    impl Future<Output = PyResult<Py<PyAny>>> + Send + Sized,
)>
where
    C: EventContext<Payload = serde_json::Value>,
    ConsumerMessage<P>: PythonRecord,
    P: Send + Sync + 'static,
{
    Python::attach(move |py| {
        // Create Python message objects using cached OpenTelemetry functions
        let message_context = Context {
            inner: context.boxed(),
            get_current: execution_context.otel_get_current.clone_ref(py),
            inject: execution_context.otel_inject.clone_ref(py),
            propagator: execution_context.propagator,
            message_class: execution_context.message_class.clone_ref(py),
            state_handles: Mutex::new(HashMap::new()),
        };
        let message = message.into_python(py, execution_context.record_class)?;

        // Convert serialized_context to a Python dict
        let otel_context = serialized_context.into_py_dict(py)?;

        // Create asyncio.Event for shutdown signaling
        let shutdown_event = execution_context.event_class.call0(py)?;

        // Create and convert handler coroutine to future
        let coroutine = execution_context
            .method
            .call1(
                py,
                (message_context, message, otel_context, &shutdown_event),
            )?
            .into_bound(py);

        let complete_future = into_future_with_locals(execution_context.locals, coroutine)?;
        Ok((shutdown_event, complete_future))
    })
}

/// Executes a timer event by calling the Python handler
///
/// # Arguments
///
/// * `context` - Timer processing context
/// * `trigger` - Timer trigger to process
/// * `serialized_context` - OpenTelemetry context serialized as a `HashMap`
/// * `timer_class` - Python Timer class
/// * `event_class` - Python Event class for cancellation
/// * `timer_method` - Python timer handler method
/// * `locals` - Task locals for asyncio integration
///
/// # Returns
///
/// A tuple containing the shutdown event and the completion future
pub(super) fn execute_timer<C>(
    context: C,
    trigger: Trigger,
    serialized_context: HashMap<String, String>,
    timer_context: TimerExecutionContext<'_>,
) -> PyResult<(
    Py<PyAny>,
    impl Future<Output = PyResult<Py<PyAny>>> + Send + Sized,
)>
where
    C: EventContext<Payload = serde_json::Value>,
{
    Python::attach(move |py| {
        // Create Python timer object using cached OpenTelemetry functions
        let context_obj = Context {
            inner: context.boxed(),
            get_current: timer_context.otel_get_current.clone_ref(py),
            inject: timer_context.otel_inject.clone_ref(py),
            propagator: timer_context.propagator,
            message_class: timer_context.message_class.clone_ref(py),
            state_handles: Mutex::new(HashMap::new()),
        };

        let timer = timer_context.timer_class.call1(
            py,
            (trigger.key.as_ref(), {
                let datetime_utc: DateTime<Utc> = trigger.time.into();
                datetime_utc
            }),
        )?;

        // Convert serialized_context to a Python dict
        let otel_context = serialized_context.into_py_dict(py)?;

        // Create asyncio.Event for shutdown signaling
        let shutdown_event = timer_context.event_class.call0(py)?;

        // Create and convert handler coroutine to future
        let coroutine = timer_context
            .timer_method
            .call1(py, (context_obj, timer, otel_context, &shutdown_event))?
            .into_bound(py);

        let complete_future = into_future_with_locals(timer_context.locals, coroutine)?;
        Ok((shutdown_event, complete_future))
    })
}

/// Python errors from message handling
#[derive(Debug, Error)]
pub enum WrappedPythonError {
    /// Underlying Python exception
    #[error(transparent)]
    Python(#[from] PyErr),

    /// The handler result has no JSON representation.
    #[error("handler result is not representable as JSON: {0}")]
    ResultConversion(String),
}

impl ClassifyError for WrappedPythonError {
    /// Determines error retry behavior based on Python error attributes
    ///
    /// Returns:
    /// - `ErrorCategory::Permanent` for errors with `is_permanent=True`
    /// - `ErrorCategory::Transient` otherwise
    fn classify_error(&self) -> ErrorCategory {
        match self {
            WrappedPythonError::Python(error) => {
                Python::attach(|py| match is_permanent_error(py, error) {
                    Ok(true) => ErrorCategory::Permanent,
                    _ => ErrorCategory::Transient,
                })
            }
            WrappedPythonError::ResultConversion(_) => ErrorCategory::Permanent,
        }
    }
}

/// Checks if a Python error is marked as permanent
///
/// # Arguments
///
/// * `py` - Python interpreter token
/// * `error` - Error to check
///
/// # Returns
///
/// Whether error has `is_permanent=True`
fn is_permanent_error(py: Python, error: &PyErr) -> PyResult<bool> {
    error.value(py).getattr("is_permanent")?.extract()
}
