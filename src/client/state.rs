//! Defines the consumer states for the Prosody client.
//!
//! This module provides an enum to represent the different states of the
//! consumer and implements the `Display` trait for string representation of
//! these states.

use std::fmt;
use std::fmt::{Display, Formatter};

use prosody::consumer::ProsodyConsumer;

use crate::client::mode::ModeConfiguration;
use crate::handler::PythonHandler;

/// Represents the current state of the consumer.
#[derive(Default)]
pub enum ConsumerState {
    /// The consumer is not yet configured.
    #[default]
    Unconfigured,
    /// The consumer is configured but not running.
    Configured(ModeConfiguration),
    /// The consumer is actively running.
    Running {
        /// The active Prosody consumer instance.
        consumer: ProsodyConsumer,
        /// The configuration used for this consumer.
        config: ModeConfiguration,
        /// The Python handler for processing messages.
        handler: PythonHandler,
    },
}

impl Display for ConsumerState {
    /// Formats the `ConsumerState` as a human-readable string.
    ///
    /// # Arguments
    ///
    /// * `f` - A mutable reference to the `Formatter`.
    ///
    /// # Returns
    ///
    /// A `fmt::Result` indicating whether the operation was successful.
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        // Convert the state to a string representation
        let state = match self {
            ConsumerState::Unconfigured => "unconfigured",
            ConsumerState::Configured(_) => "configured",
            ConsumerState::Running { .. } => "running",
        };

        // Write the state string to the formatter
        f.write_str(state)
    }
}
