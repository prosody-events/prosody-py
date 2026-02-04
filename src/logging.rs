//! Non-blocking Python logging bridge for tracing.
//!
//! This module provides a tracing layer that forwards Rust log messages to
//! Python's logging system without blocking. It uses a bounded channel to
//! queue log events and a dedicated background thread to drain them to Python.
//!
//! This avoids deadlocks that can occur when Rust async tasks running on tokio
//! threads try to acquire Python's GIL to log while the Python main thread is
//! waiting for those tasks to complete.

use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::{IntoPyDict, PyDict};
use std::fmt::Debug;
use std::fmt::Write as _;
use std::sync::mpsc::{self, Receiver, SyncSender, TrySendError};
use std::thread;
use tracing::field::{Field, Visit};
use tracing::{Event, Level, Subscriber};
use tracing_subscriber::Layer;
use tracing_subscriber::layer::Context;
use tracing_subscriber::registry::LookupSpan;

/// Capacity of the log message channel.
/// Messages are dropped if the channel is full to prevent backpressure.
const CHANNEL_CAPACITY: usize = 1024;

/// A log event ready to be forwarded to Python, containing all metadata
/// needed to create an idiomatic Python `LogRecord`.
struct LogEvent {
    /// Python logging level (ERROR=40, WARNING=30, INFO=20, DEBUG=10, TRACE=5)
    level: u8,
    /// Logger name (Rust module path like `prosody::consumer::middleware`)
    name: String,
    /// Source file path from Rust (if available)
    pathname: Option<String>,
    /// Source line number (if available)
    lineno: Option<u32>,
    /// Function/span name
    func_name: String,
    /// The formatted log message
    msg: String,
    /// Additional structured fields from tracing as key-value pairs
    extra: Vec<(String, String)>,
}

/// A tracing layer that forwards logs to Python's logging system without
/// blocking.
///
/// Log messages are queued in a bounded channel and processed by a background
/// thread that owns the channel receiver. The background thread safely acquires
/// the GIL to forward logs to Python because it's not part of the async call
/// chain.
pub struct PythonLoggingLayer {
    sender: SyncSender<LogEvent>,
}

impl PythonLoggingLayer {
    /// Creates a new Python logging layer.
    ///
    /// Spawns a background thread that drains log messages to Python's logging
    /// system. The thread runs until the layer is dropped.
    ///
    /// # Arguments
    ///
    /// * `py` - Python GIL token, used to cache Python logging functions
    ///
    /// # Errors
    ///
    /// Returns an error if Python's logging module cannot be imported.
    pub fn new(py: Python) -> PyResult<Self> {
        let (sender, receiver) = mpsc::sync_channel::<LogEvent>(CHANNEL_CAPACITY);

        // Cache Python's logging module and getLogger function
        let get_logger: Py<PyAny> = py.import("logging")?.getattr("getLogger")?.unbind();

        thread::Builder::new()
            .name("python-log-bridge".to_owned())
            .spawn(move || worker_loop(receiver, get_logger))
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

        Ok(Self { sender })
    }
}

/// Maximum number of log events to process per GIL acquisition.
/// Amortizes the cost of acquiring the GIL across multiple log events.
const BATCH_SIZE: usize = 64;

/// Background worker that drains log events to Python.
///
/// This runs on a dedicated thread that can safely block on GIL acquisition
/// because it's not part of any async task chain.
#[allow(clippy::needless_pass_by_value)] // Thread owns these until sender drops
fn worker_loop(receiver: Receiver<LogEvent>, get_logger: Py<PyAny>) {
    // Block waiting for the first event
    while let Ok(first_event) = receiver.recv() {
        // Acquire GIL once and process a batch of events
        Python::attach(|py| {
            // Process the first event
            if let Err(err) = forward_to_python(py, &get_logger, &first_event) {
                // Can't log to Python (might cause recursion), drop silently.
                let _ = err;
            }

            // Try to drain more events while we have the GIL (up to BATCH_SIZE)
            for _ in 1..BATCH_SIZE {
                match receiver.try_recv() {
                    Ok(event) => {
                        if let Err(err) = forward_to_python(py, &get_logger, &event) {
                            let _ = err;
                        }
                    }
                    Err(_) => break, // No more events waiting
                }
            }
        });
    }
}

/// Forwards a log event to Python's logging system using idiomatic `LogRecord`.
///
/// Creates a proper Python `LogRecord` with source location metadata (pathname,
/// lineno, `funcName`) so Python formatters can display Rust source locations.
fn forward_to_python(py: Python, get_logger: &Py<PyAny>, event: &LogEvent) -> PyResult<()> {
    let logger = get_logger.call1(py, (&event.name,))?;

    // Check if this level is enabled before doing more work
    let is_enabled: bool = logger
        .call_method1(py, "isEnabledFor", (event.level,))?
        .extract(py)?;

    if !is_enabled {
        return Ok(());
    }

    // Build extra dict with structured fields from tracing
    let extra = PyDict::new(py);
    for (key, value) in &event.extra {
        extra.set_item(key, value)?;
    }

    // Create a proper LogRecord using makeRecord with all Rust source metadata
    // makeRecord signature: makeRecord(name, level, fn, lno, msg, args, exc_info,
    // func=None, extra=None, sinfo=None)
    //
    // For pathname and lineno, we pass the actual values if available, otherwise
    // we pass empty string / 0 as Python's logging module requires these positional
    // args but will display them correctly when using standard formatters.
    let pathname = event.pathname.as_deref().unwrap_or("");
    let lineno = event.lineno.unwrap_or(0);

    let kwargs = [
        (
            "func",
            event.func_name.as_str().into_pyobject(py)?.into_any(),
        ),
        ("extra", extra.into_any()),
        ("sinfo", py.None().into_bound(py).into_any()),
    ]
    .into_py_dict(py)?;

    let record = logger.call_method(
        py,
        "makeRecord",
        (
            &event.name, // name
            event.level, // level
            pathname,    // fn (filename/pathname)
            lineno,      // lno (line number)
            &event.msg,  // msg
            (),          // args (empty tuple)
            py.None(),   // exc_info
        ),
        Some(&kwargs),
    )?;

    // Handle the record through the logger
    logger.call_method1(py, "handle", (record,))?;

    Ok(())
}

/// Map tracing level to Python logging level.
const fn level_to_python(level: Level) -> u8 {
    match level {
        Level::ERROR => 40, // logging.ERROR
        Level::WARN => 30,  // logging.WARNING
        Level::INFO => 20,  // logging.INFO
        Level::DEBUG => 10, // logging.DEBUG
        Level::TRACE => 5,  // Below DEBUG (custom level)
    }
}

/// Visitor that extracts log message and fields from tracing events.
struct MessageVisitor {
    message: String,
    fields: Vec<(String, String)>,
}

impl MessageVisitor {
    fn new() -> Self {
        Self {
            message: String::new(),
            fields: Vec::new(),
        }
    }
}

impl Visit for MessageVisitor {
    fn record_f64(&mut self, field: &Field, value: f64) {
        self.fields
            .push((field.name().to_owned(), value.to_string()));
    }

    fn record_i64(&mut self, field: &Field, value: i64) {
        self.fields
            .push((field.name().to_owned(), value.to_string()));
    }

    fn record_u64(&mut self, field: &Field, value: u64) {
        self.fields
            .push((field.name().to_owned(), value.to_string()));
    }

    fn record_bool(&mut self, field: &Field, value: bool) {
        self.fields
            .push((field.name().to_owned(), value.to_string()));
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        if field.name() == "message" {
            value.clone_into(&mut self.message);
        } else {
            self.fields
                .push((field.name().to_owned(), value.to_owned()));
        }
    }

    fn record_debug(&mut self, field: &Field, value: &dyn Debug) {
        if field.name() == "message" {
            self.message = format!("{value:?}");
        } else {
            self.fields
                .push((field.name().to_owned(), format!("{value:?}")));
        }
    }
}

impl<S> Layer<S> for PythonLoggingLayer
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
        let metadata = event.metadata();
        if !metadata.is_event() {
            return;
        }

        // Extract message and fields from event
        let mut visitor = MessageVisitor::new();
        event.record(&mut visitor);

        // Format message with additional fields appended (key=value style)
        // This matches common structured logging patterns
        let mut msg = visitor.message;
        for (key, value) in &visitor.fields {
            if msg.is_empty() {
                msg = format!("{key}={value}");
            } else {
                let _ = write!(msg, " {key}={value}");
            }
        }

        let log_event = LogEvent {
            level: level_to_python(*metadata.level()),
            name: metadata.target().to_owned(),
            pathname: metadata.file().map(ToOwned::to_owned),
            lineno: metadata.line(),
            func_name: metadata.name().to_owned(),
            msg,
            extra: visitor.fields,
        };

        // Non-blocking send - drops message if channel is full.
        // This ensures we never block the async runtime.
        #[allow(clippy::print_stderr)]
        if let Err(TrySendError::Full(dropped)) = self.sender.try_send(log_event) {
            // Channel full - Python logging can't keep up with log volume.
            // Print to stderr so the message isn't lost entirely.
            eprintln!(
                "[prosody] log buffer full, dropping: {} - {}",
                dropped.name, dropped.msg
            );
        }
        // TrySendError::Disconnected means worker thread died; silently drop
    }
}
