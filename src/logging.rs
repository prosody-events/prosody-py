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
use std::fmt::Debug;
use std::fmt::Write as _;
use std::sync::mpsc::{self, Receiver, SyncSender, TrySendError};
use std::thread::{self, JoinHandle};
use tracing::field::{Field, Visit};
use tracing::{Event, Level, Subscriber};
use tracing_subscriber::Layer;
use tracing_subscriber::layer::Context;
use tracing_subscriber::registry::LookupSpan;

/// Capacity of the log message channel.
/// Messages are dropped if the channel is full to prevent backpressure.
const CHANNEL_CAPACITY: usize = 1024;

/// A log event ready to be forwarded to Python.
struct LogEvent {
    level: Level,
    target: String,
    message: String,
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
    // Handle kept to ensure thread lives as long as the layer
    _worker: JoinHandle<()>,
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

        // Cache Python's logging.getLogger function for the worker thread
        let get_logger: Py<PyAny> = py.import("logging")?.getattr("getLogger")?.unbind();

        let worker = thread::Builder::new()
            .name("python-log-bridge".to_owned())
            .spawn(move || worker_loop(receiver, get_logger))
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

        Ok(Self {
            sender,
            _worker: worker,
        })
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

/// Forwards a log event to Python's logging system.
fn forward_to_python(py: Python, get_logger: &Py<PyAny>, event: &LogEvent) -> PyResult<()> {
    let logger = get_logger.call1(py, (&event.target,))?;

    // Map tracing levels to Python logging levels
    let level: u32 = match event.level {
        Level::ERROR => 40, // logging.ERROR
        Level::WARN => 30,  // logging.WARNING
        Level::INFO => 20,  // logging.INFO
        Level::DEBUG => 10, // logging.DEBUG
        Level::TRACE => 5,  // Below DEBUG (NOTSET is 0)
    };

    // Check if this level is enabled before formatting
    let is_enabled: bool = logger
        .call_method1(py, "isEnabledFor", (level,))?
        .extract(py)?;

    if is_enabled {
        logger.call_method1(py, "log", (level, &event.message))?;
    }

    Ok(())
}

/// Visitor that extracts log message content from tracing event fields.
struct MessageVisitor {
    message: String,
}

impl MessageVisitor {
    fn new() -> Self {
        Self {
            message: String::new(),
        }
    }
}

impl Visit for MessageVisitor {
    fn record_debug(&mut self, field: &Field, value: &dyn Debug) {
        if field.name() == "message" {
            self.message = format!("{value:?}");
        } else if self.message.is_empty() {
            self.message = format!("{}: {:?}", field.name(), value);
        } else {
            let _ = write!(self.message, " {}={:?}", field.name(), value);
        }
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        if field.name() == "message" {
            value.clone_into(&mut self.message);
        } else if self.message.is_empty() {
            self.message = format!("{}: {}", field.name(), value);
        } else {
            let _ = write!(self.message, " {}={}", field.name(), value);
        }
    }
}

impl<S> Layer<S> for PythonLoggingLayer
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
        let metadata = event.metadata();

        // Extract message from event fields
        let mut visitor = MessageVisitor::new();
        event.record(&mut visitor);

        let log_event = LogEvent {
            level: *metadata.level(),
            target: metadata.target().to_owned(),
            message: visitor.message,
        };

        // Non-blocking send - drops message if channel is full.
        // This ensures we never block the async runtime.
        #[allow(clippy::print_stderr)]
        if let Err(TrySendError::Full(dropped)) = self.sender.try_send(log_event) {
            // Channel full, print to stderr rather than losing the log entirely
            eprintln!(
                "[{}] {}: {}",
                dropped.level, dropped.target, dropped.message
            );
        }
        // TrySendError::Disconnected means worker thread died; silently drop
    }
}
