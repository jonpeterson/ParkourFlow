//! The payload of `Record` streams: one row of structured data.

use std::sync::Arc;

/// One row of a `Record` stream, such as a CSV line.
///
/// Column names are shared by every record of a stream (`Arc`), so a source
/// allocates them once rather than per row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Record {
    /// Column names, in order. Empty if the stream has none, e.g. a CSV file
    /// read without a header row.
    pub columns: Arc<[String]>,
    pub values: Vec<String>,
}

impl Record {
    pub fn new(columns: Arc<[String]>, values: Vec<String>) -> Self {
        Self { columns, values }
    }

    /// The value in the column named `column`, if there is one.
    pub fn get(&self, column: &str) -> Option<&str> {
        let i = self.columns.iter().position(|c| c == column)?;
        self.values.get(i).map(String::as_str)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn get_by_column() {
        let columns: Arc<[String]> = vec!["a".into(), "b".into()].into();
        let record = Record::new(columns, vec!["1".into(), "2".into()]);
        assert_eq!(record.get("b"), Some("2"));
        assert_eq!(record.get("c"), None);
    }
}
