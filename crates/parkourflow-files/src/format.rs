//! Delimited text (CSV) rows, read and written with the `csv` crate.
//!
//! The CSV nodes use only [`RowReader`] and [`RowWriter`], so the CSV
//! library stays behind this module.
//!
//! - Fields may be quoted with `"`. A quoted field can hold the delimiter,
//!   line breaks, and `""` for a literal quote.
//! - Rows end with `\n` or `\r\n` when read, and with `\n` when written.
//!   Blank lines are skipped.
//! - Rows may have different numbers of fields.
//! - Text must be UTF-8.

use std::fmt;
use std::io::{self, Read, Write};

/// A malformed row, or an I/O error, with the line it happened on if known.
#[derive(Debug)]
pub struct ReadError {
    /// 1-based line number.
    pub line: Option<u64>,
    pub message: String,
}

impl fmt::Display for ReadError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(line) = self.line {
            write!(f, "line {line}: ")?;
        }
        f.write_str(&self.message)
    }
}

impl std::error::Error for ReadError {}

impl From<csv::Error> for ReadError {
    fn from(e: csv::Error) -> Self {
        let line = e.position().map(|p| p.line());
        let message = match e.kind() {
            csv::ErrorKind::Utf8 { .. } => "not valid UTF-8 text".into(),
            csv::ErrorKind::Io(e) => e.to_string(),
            _ => e.to_string(),
        };
        Self { line, message }
    }
}

/// The delimiter as the single byte the `csv` crate wants.
fn delimiter_byte(delimiter: char) -> io::Result<u8> {
    u8::try_from(delimiter)
        .ok()
        .filter(u8::is_ascii)
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("delimiter {delimiter:?} is not a single ASCII character"),
            )
        })
}

/// Reads rows of fields from delimited text.
pub struct RowReader<R> {
    reader: csv::Reader<R>,
    record: csv::StringRecord,
}

impl<R: Read> RowReader<R> {
    pub fn new(input: R, delimiter: char) -> io::Result<Self> {
        let reader = csv::ReaderBuilder::new()
            .delimiter(delimiter_byte(delimiter)?)
            // Header handling is the node's job: every row is data here.
            .has_headers(false)
            .flexible(true)
            .from_reader(input);
        Ok(Self {
            reader,
            record: csv::StringRecord::new(),
        })
    }

    /// Reads the next row, or `None` at the end of the input.
    pub fn read_row(&mut self) -> Result<Option<Vec<String>>, ReadError> {
        if !self.reader.read_record(&mut self.record)? {
            return Ok(None);
        }
        Ok(Some(self.record.iter().map(String::from).collect()))
    }
}

/// Writes rows of fields as delimited text, quoting fields that need it.
pub struct RowWriter<W: Write> {
    writer: csv::Writer<W>,
}

impl<W: Write> RowWriter<W> {
    pub fn new(output: W, delimiter: char) -> io::Result<Self> {
        let writer = csv::WriterBuilder::new()
            .delimiter(delimiter_byte(delimiter)?)
            .flexible(true)
            .terminator(csv::Terminator::Any(b'\n'))
            .from_writer(output);
        Ok(Self { writer })
    }

    pub fn write_row<S: AsRef<str>>(&mut self, fields: &[S]) -> io::Result<()> {
        self.writer
            .write_record(fields.iter().map(|f| f.as_ref()))
            .map_err(io::Error::other)
    }

    pub fn flush(&mut self) -> io::Result<()> {
        self.writer.flush()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn read_all(text: &[u8], delimiter: char) -> Vec<Vec<String>> {
        let mut reader = RowReader::new(text, delimiter).unwrap();
        std::iter::from_fn(|| reader.read_row().unwrap()).collect()
    }

    #[test]
    fn reads_plain_and_quoted_fields() {
        let rows = read_all(
            b"a,b,c\r\n1,\"two, too\",\"say \"\"hi\"\"\"\n\n\"multi\nline\",,x",
            ',',
        );
        assert_eq!(
            rows,
            [
                vec!["a", "b", "c"],
                vec!["1", "two, too", "say \"hi\""],
                vec!["multi\nline", "", "x"],
            ]
        );
    }

    #[test]
    fn other_delimiters() {
        assert_eq!(read_all(b"a\tb;c\n", '\t'), [vec!["a", "b;c"]]);
        assert_eq!(read_all(b"a|b\n", '|'), [vec!["a", "b"]]);
        assert!(RowReader::new(&b""[..], '→').is_err());
    }

    #[test]
    fn invalid_text_reports_its_line() {
        let mut reader = RowReader::new(&b"a\nb\xff\n"[..], ',').unwrap();
        reader.read_row().unwrap();
        let err = reader.read_row().unwrap_err();
        assert_eq!(err.to_string(), "line 2: not valid UTF-8 text");
    }

    #[test]
    fn written_rows_read_back() {
        let rows = vec![
            vec!["plain", "with,comma", "with \"quote\""],
            vec!["multi\nline", "", "x"],
            vec![""],
        ];
        let mut out = Vec::new();
        let mut writer = RowWriter::new(&mut out, ',').unwrap();
        for row in &rows {
            writer.write_row(row).unwrap();
        }
        writer.flush().unwrap();
        drop(writer);
        assert_eq!(
            String::from_utf8(out.clone()).unwrap(),
            "plain,\"with,comma\",\"with \"\"quote\"\"\"\n\"multi\nline\",,x\n\"\"\n"
        );
        assert_eq!(read_all(&out, ','), rows);
    }
}
