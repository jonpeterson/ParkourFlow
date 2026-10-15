//! CSV Source and CSV Writer.

use std::fs::File;
use std::io::{BufReader, BufWriter};
use std::path::PathBuf;
use std::sync::Arc;

use parkourflow_core::{Node, NodeContext, NodeRunner, Record, StreamItem};
use serde::Deserialize;

use crate::format::{RowReader, RowWriter};

pub const SOURCE_TYPE: &str = "file.csv_source";
pub const WRITER_TYPE: &str = "file.csv_writer";

/// The `delimiter` property's options, as characters.
fn delimiter(option: &str) -> Result<char, String> {
    match option {
        "tab" => Ok('\t'),
        "," | ";" | "|" => Ok(option.chars().next().unwrap_or(',')),
        other => Err(format!("unknown delimiter `{other}`")),
    }
}

fn config<T: for<'de> Deserialize<'de>>(node: &Node) -> Result<T, String> {
    node.config()
        .map_err(|e| format!("Invalid configuration: {e}"))
}

#[derive(Debug, Deserialize)]
struct CsvSourceConfig {
    path: PathBuf,
    delimiter: String,
    has_header: bool,
}

/// Reads a CSV file and emits one [`Record`] per row on `records`. Pausable:
/// it waits for credit, so nothing is dropped.
pub struct CsvSource {
    path: PathBuf,
    reader: RowReader<BufReader<File>>,
    has_header: bool,
}

impl CsvSource {
    pub fn build(node: &Node) -> Result<Box<dyn NodeRunner>, String> {
        let config: CsvSourceConfig = config(node)?;
        let file = File::open(&config.path)
            .map_err(|e| format!("Can't open {}: {e}", config.path.display()))?;
        Ok(Box::new(Self {
            reader: RowReader::new(BufReader::new(file), delimiter(&config.delimiter)?),
            path: config.path,
            has_header: config.has_header,
        }))
    }
}

impl NodeRunner for CsvSource {
    fn run(mut self: Box<Self>, mut ctx: NodeContext) -> Result<(), String> {
        let output = ctx.take_output("records")?;
        let read_error = |e| format!("{}: {e}", self.path.display());

        let columns: Arc<[String]> = if self.has_header {
            self.reader
                .read_row()
                .map_err(read_error)?
                .unwrap_or_default()
                .into()
        } else {
            Arc::new([])
        };
        let mut sequence = 0;
        while !ctx.is_cancelled() {
            let Some(values) = self.reader.read_row().map_err(read_error)? else {
                break;
            };
            let record = Record::new(columns.clone(), values);
            if output.send(StreamItem::new(sequence, record)).is_err() {
                // Stopped, or nobody downstream is listening any more.
                break;
            }
            sequence += 1;
        }
        Ok(())
    }
}

#[derive(Debug, Deserialize)]
struct CsvWriterConfig {
    path: PathBuf,
    delimiter: String,
    write_header: bool,
}

/// Writes the [`Record`]s arriving on `records` to a CSV file, with the
/// first record's column names as the header row.
pub struct CsvWriter {
    path: PathBuf,
    writer: RowWriter<BufWriter<File>>,
    write_header: bool,
}

impl CsvWriter {
    /// Creates (or truncates) the output file.
    pub fn build(node: &Node) -> Result<Box<dyn NodeRunner>, String> {
        let config: CsvWriterConfig = config(node)?;
        let file = File::create(&config.path)
            .map_err(|e| format!("Can't create {}: {e}", config.path.display()))?;
        Ok(Box::new(Self {
            writer: RowWriter::new(BufWriter::new(file), delimiter(&config.delimiter)?),
            path: config.path,
            write_header: config.write_header,
        }))
    }
}

impl NodeRunner for CsvWriter {
    fn run(mut self: Box<Self>, mut ctx: NodeContext) -> Result<(), String> {
        let input = ctx.take_input("records")?;
        let path = self.path.display().to_string();
        let write_error = |e| format!("{path}: {e}");

        let mut header_pending = self.write_header;
        // Ends when the stream ends or the node is stopped; either way, keep
        // what was written.
        while let Some(item) = input.recv() {
            let record = item
                .payload::<Record>()
                .ok_or_else(|| format!("Item {} is not a Record", item.sequence))?;
            if header_pending {
                header_pending = false;
                if !record.columns.is_empty() {
                    self.writer
                        .write_row(&record.columns)
                        .map_err(write_error)?;
                }
            }
            self.writer.write_row(&record.values).map_err(write_error)?;
        }
        self.writer.flush().map_err(write_error)
    }
}
