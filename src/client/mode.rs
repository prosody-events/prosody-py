//! Defines operational modes and configurations for the Prosody client.
//!
//! This module provides enums and implementations for different operational
//! modes (`Pipeline` and `LowLatency`) and their associated configurations.

use std::fmt;
use std::fmt::{Display, Formatter};
use std::str::FromStr;

use prosody::consumer::failure::retry::RetryConfiguration;
use prosody::consumer::failure::topic::FailureTopicConfiguration;
use prosody::consumer::ConsumerConfiguration;
use pyo3::exceptions::PyValueError;
use pyo3::PyErr;

use crate::client::{LOW_LATENCY_MODE, PIPELINE_MODE};

/// Operational modes for the Prosody client.
#[derive(Copy, Clone, Debug, Default)]
pub enum Mode {
    /// Pipeline mode for standard processing.
    #[default]
    Pipeline,
    /// Low-latency mode for faster processing with potential trade-offs.
    LowLatency,
}

impl Display for Mode {
    /// Formats the `Mode` as a string.
    ///
    /// # Arguments
    ///
    /// * `f` - A mutable reference to the `Formatter`.
    ///
    /// # Returns
    ///
    /// A `fmt::Result` indicating whether the operation was successful.
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match self {
            Mode::Pipeline => f.write_str(PIPELINE_MODE),
            Mode::LowLatency => f.write_str(LOW_LATENCY_MODE),
        }
    }
}

impl FromStr for Mode {
    type Err = PyErr;

    /// Parses a string slice into a `Mode`.
    ///
    /// # Arguments
    ///
    /// * `s` - The string slice to parse.
    ///
    /// # Returns
    ///
    /// A `Result` containing the parsed `Mode` or a `PyErr` if parsing fails.
    ///
    /// # Errors
    ///
    /// Returns a `PyValueError` if the input string is not a valid mode.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            PIPELINE_MODE => Ok(Mode::Pipeline),
            LOW_LATENCY_MODE => Ok(Mode::LowLatency),
            unknown => Err(PyValueError::new_err(format!("unknown mode: '{unknown}'"))),
        }
    }
}

/// Configuration for different operational modes of the Prosody client.
#[derive(Debug)]
pub enum ModeConfiguration {
    /// Configuration for Pipeline mode.
    Pipeline {
        /// The consumer configuration.
        consumer: ConsumerConfiguration,
        /// The retry configuration.
        retry: RetryConfiguration,
    },
    /// Configuration for Low-Latency mode.
    LowLatency {
        /// The consumer configuration.
        consumer: ConsumerConfiguration,
        /// The retry configuration.
        retry: RetryConfiguration,
        /// The failure topic configuration.
        failure_topic: FailureTopicConfiguration,
    },
}

impl ModeConfiguration {
    /// Returns the mode of the configuration.
    ///
    /// # Returns
    ///
    /// The `Mode` corresponding to this configuration.
    pub fn mode(&self) -> Mode {
        match self {
            ModeConfiguration::Pipeline { .. } => Mode::Pipeline,
            ModeConfiguration::LowLatency { .. } => Mode::LowLatency,
        }
    }

    /// Returns a reference to the consumer configuration.
    ///
    /// # Returns
    ///
    /// A reference to the `ConsumerConfiguration` for this mode.
    pub fn consumer(&self) -> &ConsumerConfiguration {
        match self {
            ModeConfiguration::Pipeline { consumer, .. }
            | ModeConfiguration::LowLatency { consumer, .. } => consumer,
        }
    }
}
