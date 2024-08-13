//! Utility functions for formatting data structures into string
//! representations.

/// Formats a slice of string-like values into a Python-style list
/// representation.
///
/// This function takes a slice of values that can be referenced as strings and
/// formats them into a string that resembles a Python list literal. Each item
/// is wrapped in single quotes and separated by commas.
///
/// # Arguments
///
/// * `value` - A slice of values that can be referenced as strings.
///
/// # Returns
///
/// A `String` containing the formatted list representation.
///
/// # Examples
///
/// ```
/// use crate::format::format_list;
///
/// let values = vec!["apple", "banana", "cherry"];
/// assert_eq!(format_list(&values), "['apple', 'banana', 'cherry']");
/// ```
pub fn format_list<T>(value: &[T]) -> String
where
    T: AsRef<str>,
{
    // Map each item to a quoted string, join them with commas,
    // and wrap the result in square brackets
    let items = value
        .iter()
        .map(|s| format!("'{}'", s.as_ref()))
        .collect::<Vec<_>>()
        .join(", ");

    format!("[{items}]")
}
