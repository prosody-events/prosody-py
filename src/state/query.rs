//! Query options that open a keyed-state cursor.
//!
//! The Python layer checks the caller's options and resolves each edge pair
//! into one edge, so an edge is either inclusive or exclusive, never both.
//! This module applies the resolved options to core's fluent query. Core
//! owns every query semantic.

use super::{StateEnv, parse_direction};
use prosody::consumer::event_context::StateCursor;
use prosody::state::erased::{ErasedDequeRead, ErasedKeyRead};
use pyo3::{FromPyObject, PyResult, Python};
use std::num::NonZeroUsize;

/// Options for a map or set query.
///
/// Extracted from the attributes of the Python `_KeyQuery` value. An edge is a
/// `(key, inclusive)` pair in query order.
#[derive(FromPyObject)]
pub(crate) struct KeyQuery {
    direction: String,
    prefix: Option<String>,
    start: Option<(String, bool)>,
    end: Option<(String, bool)>,
    limit: Option<NonZeroUsize>,
}

/// Options for a deque query.
///
/// Extracted from the attributes of the Python `_PositionQuery` value.
/// Positions count from the front. An edge is a `(position, inclusive)` pair
/// in query order. `range` is an ascending half-open span; its end is `None`
/// when the span has no upper bound.
#[derive(FromPyObject)]
pub(crate) struct PositionQuery {
    direction: String,
    start: Option<(usize, bool)>,
    end: Option<(usize, bool)>,
    range: Option<(usize, Option<usize>)>,
    limit: Option<NonZeroUsize>,
}

impl KeyQuery {
    /// Applies these options to `read` and opens its cursor without a read.
    ///
    /// The direction applies first because core reads `from`, `after`, `to`,
    /// and `before` in the order set before the call.
    ///
    /// # Errors
    ///
    /// Returns a transient error for an unknown direction token.
    pub(crate) fn stream<Item>(
        self,
        py: Python,
        env: &StateEnv,
        read: ErasedKeyRead<Item>,
    ) -> PyResult<StateCursor<Item>> {
        let mut read = read.direction(parse_direction(py, env, &self.direction)?);
        if let Some(prefix) = &self.prefix {
            read = read.prefix(prefix);
        }
        read = match &self.start {
            Some((key, true)) => read.from(key),
            Some((key, false)) => read.after(key),
            None => read,
        };
        read = match &self.end {
            Some((key, true)) => read.to(key),
            Some((key, false)) => read.before(key),
            None => read,
        };
        if let Some(limit) = self.limit {
            read = read.limit(limit);
        }
        Ok(read.stream())
    }
}

impl PositionQuery {
    /// Applies these options to `read` and opens its cursor without a read.
    ///
    /// # Errors
    ///
    /// Returns a transient error for an unknown direction token.
    pub(crate) fn stream<Item>(
        self,
        py: Python,
        env: &StateEnv,
        read: ErasedDequeRead<Item>,
    ) -> PyResult<StateCursor<Item>> {
        let mut read = read.direction(parse_direction(py, env, &self.direction)?);
        read = match self.start {
            Some((position, true)) => read.from(position),
            Some((position, false)) => read.after(position),
            None => read,
        };
        read = match self.end {
            Some((position, true)) => read.to(position),
            Some((position, false)) => read.before(position),
            None => read,
        };
        read = match self.range {
            Some((start, Some(end))) => read.range(start..end),
            Some((start, None)) => read.range(start..),
            None => read,
        };
        if let Some(limit) = self.limit {
            read = read.limit(limit);
        }
        Ok(read.stream())
    }
}
