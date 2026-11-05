//! Stream identity: an aggregate id plus its category (aggregate type).

use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

use crate::error::StreamIdError;
use crate::ids::AggregateId;

/// A stream category, validated to match `[A-Za-z0-9_.-]+`.
///
/// Validation runs once at construction, so every `Category` in the program
/// is known to be well formed.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct Category(String);

impl Category {
    /// Validates and wraps a category name.
    ///
    /// # Errors
    /// Returns [`StreamIdError::InvalidCategory`] if the name is empty or
    /// contains characters outside `[A-Za-z0-9_.-]`.
    pub fn new(name: impl Into<String>) -> Result<Self, StreamIdError> {
        let name = name.into();
        let valid = !name.is_empty()
            && name
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '.' | '-'));
        if valid {
            Ok(Self(name))
        } else {
            Err(StreamIdError::InvalidCategory(name))
        }
    }

    /// Returns the category name.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for Category {
    type Error = StreamIdError;

    fn try_from(name: String) -> Result<Self, Self::Error> {
        Self::new(name)
    }
}

impl From<Category> for String {
    fn from(category: Category) -> Self {
        category.0
    }
}

impl fmt::Display for Category {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// Identity of an event stream: `aggregate_id` plus `category`.
///
/// Renders as `"{aggregate_id}:{category}"`. The `:` delimiter is why it is
/// excluded from categories.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct StreamId {
    aggregate_id: AggregateId,
    category: Category,
}

impl StreamId {
    /// Builds a stream id from its parts.
    #[must_use]
    pub const fn new(aggregate_id: AggregateId, category: Category) -> Self {
        Self {
            aggregate_id,
            category,
        }
    }

    /// The aggregate this stream belongs to.
    #[must_use]
    pub const fn aggregate_id(&self) -> AggregateId {
        self.aggregate_id
    }

    /// The stream category.
    #[must_use]
    pub const fn category(&self) -> &Category {
        &self.category
    }
}

impl fmt::Display for StreamId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.aggregate_id, self.category)
    }
}

impl FromStr for StreamId {
    type Err = StreamIdError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (id, category) = s
            .split_once(':')
            .ok_or_else(|| StreamIdError::Malformed(s.to_owned()))?;
        Ok(Self::new(id.parse()?, Category::new(category)?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn category_rejects_empty_and_delimiters() {
        assert!(Category::new("").is_err());
        assert!(Category::new("Order:v2").is_err());
        assert!(Category::new("Order.v2-beta_1").is_ok());
    }

    #[test]
    fn stream_id_round_trips_through_text() {
        let id = StreamId::new(AggregateId::new(), Category::new("Order").expect("valid"));
        let parsed: StreamId = id.to_string().parse().expect("parse");
        assert_eq!(parsed, id);
    }

    #[test]
    fn stream_id_rejects_missing_separator_and_bad_uuid() {
        assert!(matches!(
            "no-separator".parse::<StreamId>(),
            Err(StreamIdError::Malformed(_))
        ));
        assert!(matches!(
            "not-a-uuid:Order".parse::<StreamId>(),
            Err(StreamIdError::Uuid(_))
        ));
    }
}

#[cfg(test)]
mod more_tests {
    use super::*;

    #[test]
    fn category_rejects_whitespace_and_non_ascii() {
        assert!(Category::new("Or der").is_err());
        assert!(Category::new("Ordér").is_err());
        assert!(Category::new("a/b").is_err());
    }

    #[test]
    fn category_error_carries_the_input() {
        match Category::new("bad:name") {
            Err(StreamIdError::InvalidCategory(name)) => assert_eq!(name, "bad:name"),
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn category_deserialization_validates() {
        assert!(serde_json::from_str::<Category>("\"Order\"").is_ok());
        assert!(serde_json::from_str::<Category>("\"\"").is_err());
    }

    #[test]
    fn stream_id_rejects_bad_category_after_separator() {
        let text = format!("{}:bad cat", AggregateId::new());
        assert!(matches!(
            text.parse::<StreamId>(),
            Err(StreamIdError::InvalidCategory(_))
        ));
    }

    #[test]
    fn stream_id_renders_id_then_category() {
        let agg = AggregateId::new();
        let id = StreamId::new(agg, Category::new("Order").expect("valid"));
        assert_eq!(id.to_string(), format!("{agg}:Order"));
        assert_eq!(id.aggregate_id(), agg);
        assert_eq!(id.category().as_str(), "Order");
    }
}
