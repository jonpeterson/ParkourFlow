//! Delimited text (CSV) rows, as RFC 4180 describes them.
//!
//! The CSV nodes use only [`RowReader`] and [`RowWriter`], so this module can
//! be swapped for a CSV library without touching them.
//!
//! - Fields may be quoted with `"`. A quoted field can hold the delimiter,
//!   line breaks, and `""` for a literal quote.
//! - Rows end with `\n` or `\r\n`. Blank lines are skipped.
//! - Rows may have different numbers of fields.

use std::fmt;
use std::io::{self, BufRead, Write};

/// A malformed row, or an I/O error, with the line it happened on.
#[derive(Debug)]
pub struct ReadError {
    /// 1-based line number.
    pub line: u64,
    pub kind: ReadErrorKind,
}

#[derive(Debug)]
pub enum ReadErrorKind {
    Io(io::Error),
    /// A quoted field was still open at the end of the input.
    UnterminatedQuote,
}

impl fmt::Display for ReadError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "line {}: ", self.line)?;
        match &self.kind {
            ReadErrorKind::Io(e) => write!(f, "{e}"),
            ReadErrorKind::UnterminatedQuote => f.write_str("quoted field is never closed"),
        }
    }
}

impl std::error::Error for ReadError {}

/// Reads rows of fields from delimited text.
pub struct RowReader<R> {
    input: R,
    delimiter: char,
    /// Lines read so far.
    lines: u64,
    buf: String,
}

impl<R: BufRead> RowReader<R> {
    pub fn new(input: R, delimiter: char) -> Self {
        Self {
            input,
            delimiter,
            lines: 0,
            buf: String::new(),
        }
    }

    /// Reads the next row, or `None` at the end of the input.
    pub fn read_row(&mut self) -> Result<Option<Vec<String>>, ReadError> {
        let mut fields = Vec::new();
        let mut field = String::new();
        let mut in_quotes = false;
        // Whether the row has anything but a line break, so `""` alone is a
        // row with one empty field rather than a blank line.
        let mut blank = true;
        let start = self.lines + 1;
        loop {
            self.buf.clear();
            let read = self.input.read_line(&mut self.buf).map_err(|e| ReadError {
                line: self.lines + 1,
                kind: ReadErrorKind::Io(e),
            })?;
            if read == 0 {
                if in_quotes {
                    return Err(ReadError {
                        line: start,
                        kind: ReadErrorKind::UnterminatedQuote,
                    });
                }
                // The last row had no line break.
                if blank {
                    return Ok(None);
                }
                fields.push(field);
                return Ok(Some(fields));
            }
            self.lines += 1;

            let mut chars = self.buf.chars().peekable();
            while let Some(c) = chars.next() {
                if !matches!(c, '\r' | '\n') {
                    blank = false;
                }
                if in_quotes {
                    if c == '"' {
                        if chars.peek() == Some(&'"') {
                            chars.next();
                            field.push('"');
                        } else {
                            in_quotes = false;
                        }
                    } else {
                        field.push(c);
                    }
                } else if c == self.delimiter {
                    fields.push(std::mem::take(&mut field));
                } else if c == '"' && field.is_empty() {
                    in_quotes = true;
                } else if c == '\n' || (c == '\r' && chars.peek() == Some(&'\n')) {
                    break;
                } else {
                    field.push(c);
                }
            }
            if in_quotes {
                // The line break belongs to the quoted field; keep reading.
                continue;
            }
            if blank {
                continue;
            }
            fields.push(field);
            return Ok(Some(fields));
        }
    }
}

/// Writes rows of fields as delimited text, quoting fields that need it.
pub struct RowWriter<W> {
    output: W,
    delimiter: char,
}

impl<W: Write> RowWriter<W> {
    pub fn new(output: W, delimiter: char) -> Self {
        Self { output, delimiter }
    }

    pub fn write_row<S: AsRef<str>>(&mut self, fields: &[S]) -> io::Result<()> {
        let mut line = String::new();
        for (i, field) in fields.iter().enumerate() {
            if i > 0 {
                line.push(self.delimiter);
            }
            let field = field.as_ref();
            let needs_quotes = field
                .chars()
                .any(|c| c == self.delimiter || matches!(c, '"' | '\n' | '\r'));
            // A lone empty field would read back as a blank line.
            if needs_quotes || (fields.len() == 1 && field.is_empty()) {
                line.push('"');
                line.push_str(&field.replace('"', "\"\""));
                line.push('"');
            } else {
                line.push_str(field);
            }
        }
        line.push('\n');
        self.output.write_all(line.as_bytes())
    }

    pub fn flush(&mut self) -> io::Result<()> {
        self.output.flush()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn read_all(text: &str, delimiter: char) -> Vec<Vec<String>> {
        let mut reader = RowReader::new(text.as_bytes(), delimiter);
        std::iter::from_fn(|| reader.read_row().unwrap()).collect()
    }

    #[test]
    fn reads_plain_and_quoted_fields() {
        let rows = read_all(
            "a,b,c\r\n1,\"two, too\",\"say \"\"hi\"\"\"\n\n\"multi\nline\",,x",
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
        assert_eq!(read_all("a\tb;c\n", '\t'), [vec!["a", "b;c"]]);
        assert_eq!(read_all("a|b\n", '|'), [vec!["a", "b"]]);
    }

    #[test]
    fn unterminated_quote_reports_its_line() {
        let mut reader = RowReader::new("a\n\"open\nstill open\n".as_bytes(), ',');
        reader.read_row().unwrap();
        let err = reader.read_row().unwrap_err();
        assert_eq!(err.to_string(), "line 2: quoted field is never closed");
    }

    #[test]
    fn written_rows_read_back() {
        let rows = vec![
            vec!["plain", "with,comma", "with \"quote\""],
            vec!["multi\nline", "", "x"],
            vec![""],
        ];
        let mut out = Vec::new();
        let mut writer = RowWriter::new(&mut out, ',');
        for row in &rows {
            writer.write_row(row).unwrap();
        }
        let text = String::from_utf8(out).unwrap();
        assert_eq!(
            text,
            "plain,\"with,comma\",\"with \"\"quote\"\"\"\n\"multi\nline\",,x\n\"\"\n"
        );
        assert_eq!(read_all(&text, ','), rows);
    }
}
