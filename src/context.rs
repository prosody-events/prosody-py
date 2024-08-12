//! Defines structures for representing Kafka messages in a Python-compatible
//! format.
//!
//! This module provides the `Context` struct to hold message context
//! information for Kafka messages.

use prosody::consumer::message::MessageContext;
use pyo3::pyclass;

/// Encapsulates context information for a Kafka message.
///
/// This struct wraps a `MessageContext` from the `prosody` crate,
/// making it accessible in a Python environment.
#[pyclass]
pub struct Context(pub MessageContext);
