use std::{
    env,
    io::{self, IsTerminal, Write},
    process::{Child, Command, Stdio},
};

use tabled::{builder::Builder, settings::Style};
use tokio::sync::mpsc;

use crate::{
    cli::OutputFormat,
    error::{AppError, Result},
};

#[derive(Debug, Default)]
pub struct ResultSet {
    pub has_row_description: bool,
    pub columns: Vec<String>,
    pub rows: Vec<Vec<Option<String>>>,
    pub total_rows: usize,
    pub affected_rows: u64,
    pub fields_truncated: bool,
}

impl ResultSet {
    pub fn with_columns(columns: Vec<String>) -> Self {
        Self {
            has_row_description: true,
            columns,
            ..Self::default()
        }
    }

    /// Counts the row and keeps it unless `row_limit` (0 = unlimited) has been
    /// reached, truncating each field to `max_field_width` characters.
    pub fn retain_human_row(
        &mut self,
        values: &[Option<&str>],
        row_limit: usize,
        max_field_width: usize,
    ) {
        self.total_rows += 1;
        if dropped_rows(self.total_rows, row_limit) {
            return;
        }
        let mut row = Vec::with_capacity(values.len());
        for value in values {
            row.push(value.map(|value| {
                let (value, truncated) = truncate_field(value, max_field_width);
                self.fields_truncated |= truncated;
                value
            }));
        }
        self.rows.push(row);
    }
}

pub struct RenderedOutput {
    pub data: String,
    pub diagnostic: Option<String>,
}

pub enum StreamOutput {
    Data(String),
    Diagnostic(String),
}

/// Starts a blocking writer thread that copies streamed data to stdout, or
/// into the pager when `page` is set, and diagnostics to stderr. Dropping the
/// sender ends the thread; await the handle to collect any write error.
pub fn stream_writer(
    page: bool,
) -> (
    mpsc::Sender<StreamOutput>,
    tokio::task::JoinHandle<Result<()>>,
) {
    let (sender, receiver) = mpsc::channel(8);
    let task = tokio::task::spawn_blocking(move || {
        if page {
            write_stream_to_pager(
                receiver,
                &pager_command(),
                screen_lines(),
                write_stdout,
                write_diagnostic,
            )
        } else {
            write_stream(receiver, write_stdout, write_diagnostic)
        }
    });
    (sender, task)
}

/// Streams into a pager, which is started only once the output reaches
/// `page_after_lines`. Shorter output, such as a row count or a few rows, is
/// written through `write_data` and `write_diagnostic` in its original order
/// at the end instead, so a pager never opens on something that fits on the
/// screen. Quitting the pager early closes the pipe and surfaces as
/// [`AppError::PagerClosed`].
///
/// Once a pager is running, diagnostics are held until it exits: a full-screen
/// pager owns the terminal, and writing to stderr underneath it would paint
/// over its display.
fn write_stream_to_pager(
    mut receiver: mpsc::Receiver<StreamOutput>,
    pager: &str,
    page_after_lines: usize,
    mut write_data: impl FnMut(&str) -> Result<()>,
    mut write_diagnostic: impl FnMut(&str) -> Result<()>,
) -> Result<()> {
    let mut child: Option<Child> = None;
    let mut pending: Vec<StreamOutput> = Vec::new();
    let mut pending_lines = 0;
    let mut diagnostics = Vec::new();
    let mut handle = |output: StreamOutput| -> Result<()> {
        match output {
            StreamOutput::Data(data) => {
                if let Some(child) = &mut child {
                    return write_to_pager(child, &data);
                }
                pending_lines += data.matches('\n').count();
                pending.push(StreamOutput::Data(data));
                if pending_lines >= page_after_lines {
                    let started = child.insert(spawn_pager(pager)?);
                    for output in pending.drain(..) {
                        match output {
                            StreamOutput::Data(data) => write_to_pager(started, &data)?,
                            StreamOutput::Diagnostic(diagnostic) => diagnostics.push(diagnostic),
                        }
                    }
                }
            }
            StreamOutput::Diagnostic(diagnostic) => {
                if child.is_some() {
                    diagnostics.push(diagnostic);
                } else {
                    pending.push(StreamOutput::Diagnostic(diagnostic));
                }
            }
        }
        Ok(())
    };
    let written = std::iter::from_fn(|| receiver.blocking_recv()).try_for_each(&mut handle);

    // Without a pager, everything held so far is written out, including when
    // the pager failed to start, so a bad pager setting does not lose output.
    let exited = match child {
        Some(child) => wait_pager(child),
        None => pending.iter().try_for_each(|output| match output {
            StreamOutput::Data(data) => write_data(data),
            StreamOutput::Diagnostic(diagnostic) => write_diagnostic(diagnostic),
        }),
    };
    for diagnostic in &diagnostics {
        write_diagnostic(diagnostic)?;
    }
    written?;
    exited
}

fn write_to_pager(pager: &mut Child, data: &str) -> Result<()> {
    let stdin = pager.stdin.as_mut().expect("pager stdin is piped");
    stdin.write_all(data.as_bytes()).map_err(|error| {
        if error.kind() == io::ErrorKind::BrokenPipe {
            AppError::PagerClosed
        } else {
            AppError::Io(error)
        }
    })
}

fn write_diagnostic(diagnostic: &str) -> Result<()> {
    // PostgreSQL notices are logged to stderr by the connection task, so never
    // retain this lock while waiting for events.
    let stderr = io::stderr();
    let mut stderr = stderr.lock();
    stderr.write_all(diagnostic.as_bytes())?;
    stderr.write_all(b"\n")?;
    stderr.flush()?;
    Ok(())
}

fn write_stream(
    mut receiver: mpsc::Receiver<StreamOutput>,
    mut write_data: impl FnMut(&str) -> Result<()>,
    mut write_diagnostic: impl FnMut(&str) -> Result<()>,
) -> Result<()> {
    while let Some(output) = receiver.blocking_recv() {
        match output {
            StreamOutput::Data(data) => write_data(&data)?,
            StreamOutput::Diagnostic(diagnostic) => write_diagnostic(&diagnostic)?,
        }
    }
    Ok(())
}

/// How a result set is laid out.
///
/// This collapses `--format` and `--expanded`, which overlap: `-x` on any format
/// and `--format=vertical` both mean one field per line. Deciding once here is
/// what lets rendering be a total match on the layout.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Layout {
    Table,
    Vertical,
    Delimited(char),
}

impl Layout {
    pub fn new(format: OutputFormat, expanded: bool) -> Self {
        if expanded {
            return Self::Vertical;
        }
        match format {
            OutputFormat::Table => Self::Table,
            OutputFormat::Vertical => Self::Vertical,
            OutputFormat::Csv => Self::Delimited(','),
            OutputFormat::Tsv => Self::Delimited('\t'),
        }
    }

    /// Delimited output is meant for another program to read, so its row count
    /// travels beside the data rather than inside it.
    pub fn is_machine_readable(self) -> bool {
        matches!(self, Self::Delimited(_))
    }
}

#[derive(Clone, Copy)]
pub struct RenderOptions {
    pub layout: Layout,
    /// Rows retained per result set; 0 means unlimited.
    pub row_limit: usize,
    /// Escape control characters, for output going to a terminal rather than to
    /// a file or another program.
    pub escape_controls: bool,
}

/// Renders `result` the way interactive table output does, for callers that
/// assemble their own composite output.
pub fn render_table_output(result: &ResultSet, row_limit: usize) -> String {
    render_query(
        result,
        RenderOptions {
            layout: Layout::Table,
            row_limit,
            escape_controls: false,
        },
    )
    .data
}

pub fn render_query(result: &ResultSet, options: RenderOptions) -> RenderedOutput {
    let data = render_data(result, options);
    let diagnostic = diagnostic(result, options.row_limit);
    if options.layout.is_machine_readable() {
        return RenderedOutput {
            data,
            diagnostic: Some(diagnostic),
        };
    }
    let mut data = data;
    data.push_str(&diagnostic);
    data.push('\n');
    RenderedOutput {
        data,
        diagnostic: None,
    }
}

fn render_data(result: &ResultSet, options: RenderOptions) -> String {
    match options.layout {
        Layout::Table => render_table(result),
        Layout::Vertical => render_vertical(result),
        Layout::Delimited(delimiter) => {
            render_delimited(result, delimiter, options.escape_controls)
        }
    }
}

/// The trailing row count, plus a note for each way the result was cut short.
pub fn diagnostic(result: &ResultSet, row_limit: usize) -> String {
    let mut diagnostic = if result.has_row_description {
        format!(
            "({} row{})",
            result.total_rows,
            if result.total_rows == 1 { "" } else { "s" }
        )
    } else {
        format!("{} row(s) affected", result.affected_rows)
    };
    if dropped_rows(result.total_rows, row_limit) {
        diagnostic.push_str(" [rows limited]");
    }
    if result.fields_truncated {
        diagnostic.push_str(" [fields truncated]");
    }
    diagnostic
}

/// Whether `--row-limit` held rows back. A limit of zero means unlimited.
pub fn dropped_rows(total_rows: usize, row_limit: usize) -> bool {
    row_limit != 0 && total_rows > row_limit
}

fn render_table(result: &ResultSet) -> String {
    if result.columns.is_empty() {
        return if result.has_row_description {
            "--\n".into()
        } else {
            String::new()
        };
    }
    let mut builder = Builder::with_capacity(result.rows.len() + 1, result.columns.len());
    builder.push_record(result.columns.iter().map(|value| safe_terminal_text(value)));
    for row in &result.rows {
        builder.push_record(
            row.iter()
                .map(|value| safe_terminal_text(display_value(value))),
        );
    }
    let mut table = builder.build();
    table.with(Style::psql());
    format!("{table}\n")
}

fn render_vertical(result: &ResultSet) -> String {
    let mut output = String::new();
    for (index, row) in result.rows.iter().enumerate() {
        output.push_str(&format!("-[ RECORD {} ]-\n", index + 1));
        for (column, value) in result.columns.iter().zip(row) {
            output.push_str(&safe_terminal_text(column));
            output.push_str(" | ");
            output.push_str(&safe_terminal_text(display_value(value)));
            output.push('\n');
        }
    }
    output
}

pub fn render_delimited_header(
    columns: &[String],
    delimiter: char,
    escape_controls: bool,
) -> String {
    if columns.is_empty() {
        return "\n".into();
    }
    let separator = delimiter.to_string();
    let mut output = columns
        .iter()
        .map(|value| escape_delimited_field(Some(value.as_str()), delimiter, escape_controls))
        .collect::<Vec<_>>()
        .join(&separator);
    output.push('\n');
    output
}

pub fn render_delimited_row(
    row: &[Option<String>],
    delimiter: char,
    escape_controls: bool,
) -> String {
    if row.is_empty() {
        return "\n".into();
    }
    let separator = delimiter.to_string();
    let mut output = row
        .iter()
        .map(|value| escape_delimited_field(value.as_deref(), delimiter, escape_controls))
        .collect::<Vec<_>>()
        .join(&separator);
    output.push('\n');
    output
}

fn render_delimited(result: &ResultSet, delimiter: char, escape_controls: bool) -> String {
    if result.columns.is_empty() {
        return if result.has_row_description {
            // The empty first record is the zero-field header.
            "\n".repeat(result.rows.len() + 1)
        } else {
            String::new()
        };
    }
    let mut output = render_delimited_header(&result.columns, delimiter, escape_controls);
    for row in &result.rows {
        output.push_str(&render_delimited_row(row, delimiter, escape_controls));
    }
    output
}

fn escape_delimited_field(value: Option<&str>, delimiter: char, escape_controls: bool) -> String {
    let Some(value) = value else {
        // PostgreSQL CSV convention: an unquoted empty field is NULL.
        return String::new();
    };
    let safe;
    let value = if escape_controls {
        safe = safe_terminal_text(value);
        &safe
    } else {
        value
    };
    if value.is_empty() || value.contains(delimiter) || value.contains(['"', '\n', '\r']) {
        format!("\"{}\"", value.replace('"', "\"\""))
    } else {
        value.to_owned()
    }
}

pub(crate) fn is_unsafe_terminal_character(character: char) -> bool {
    character.is_control()
        || matches!(
            character,
            '\u{061c}' | '\u{200e}'..='\u{200f}' | '\u{202a}'..='\u{202e}' | '\u{2066}'..='\u{2069}'
        )
}

pub fn safe_editor_text(value: &str) -> String {
    let mut output = String::with_capacity(value.len());
    for character in value.chars() {
        if character != '\n' && character != '\t' && is_unsafe_terminal_character(character) {
            output.extend(std::iter::repeat_n('?', character.len_utf8()));
        } else {
            output.push(character);
        }
    }
    output
}

pub fn safe_terminal_text(value: &str) -> String {
    let mut output = String::with_capacity(value.len());
    for character in value.chars() {
        match character {
            '\n' => output.push_str("\\n"),
            '\r' => output.push_str("\\r"),
            '\t' => output.push_str("\\t"),
            character if character.is_control() => {
                use std::fmt::Write as _;
                let _ = write!(output, "\\x{:02x}", character as u32);
            }
            character if is_unsafe_terminal_character(character) => {
                use std::fmt::Write as _;
                let _ = write!(output, "\\u{{{:x}}}", character as u32);
            }
            character => output.push(character),
        }
    }
    output
}

pub(crate) fn quote_identifier(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}

pub(crate) fn truncate_field(value: &str, max_width: usize) -> (String, bool) {
    if max_width == 0 {
        return (value.to_owned(), false);
    }
    match value.char_indices().nth(max_width) {
        Some((end, _)) => (format!("{}…", &value[..end]), true),
        None => (value.to_owned(), false),
    }
}

fn display_value(value: &Option<String>) -> &str {
    value.as_deref().unwrap_or("<null>")
}

pub fn write(output: &str, pager_enabled: bool) -> Result<()> {
    if pager_enabled && io::stdout().is_terminal() && should_page(output) {
        return page(output);
    }
    write_stdout(output)
}

pub fn write_stdout(output: &str) -> Result<()> {
    let stdout = io::stdout();
    let mut stdout = stdout.lock();
    write_stdout_to(&mut stdout, output.as_bytes())?;
    stdout.flush().map_err(stdout_error)?;
    Ok(())
}

fn write_stdout_to(writer: &mut impl Write, output: &[u8]) -> Result<()> {
    writer.write_all(output).map_err(stdout_error)
}

fn stdout_error(error: io::Error) -> AppError {
    if error.kind() == io::ErrorKind::BrokenPipe {
        AppError::StdoutClosed
    } else {
        AppError::Io(error)
    }
}

fn should_page(output: &str) -> bool {
    output.lines().count() >= screen_lines()
}

/// How many lines of output fit on the terminal before paging is worthwhile.
/// A terminal that reports no height is treated as a standard 24-line one.
fn screen_lines() -> usize {
    crossterm::terminal::size()
        .ok()
        .map(|(_, height)| height as usize)
        .filter(|height| *height > 0)
        .unwrap_or(24)
        .saturating_sub(2)
}

fn page(output: &str) -> Result<()> {
    page_with_command(output, &pager_command())
}

/// `PGLINE_PAGER` overrides `PAGER`, as `PSQL_PAGER` does for psql, so a
/// CSV-aware pager can be used here without changing it for every other tool.
fn pager_command() -> String {
    env::var("PGLINE_PAGER")
        .or_else(|_| env::var("PAGER"))
        .unwrap_or_else(|_| {
            if cfg!(windows) {
                "more".into()
            } else {
                "less -SRFX".into()
            }
        })
}

fn spawn_pager(pager: &str) -> Result<Child> {
    let mut parts = shlex::split(pager)
        .ok_or(AppError::InvalidPager)?
        .into_iter();
    let program = parts.next().ok_or(AppError::InvalidPager)?;
    Ok(Command::new(program)
        .args(parts)
        .env("LESS", env::var("LESS").unwrap_or_else(|_| "-SRFX".into()))
        .stdin(Stdio::piped())
        .spawn()?)
}

fn page_with_command(output: &str, pager: &str) -> Result<()> {
    let mut child = spawn_pager(pager)?;
    if let Some(stdin) = child.stdin.as_mut()
        && let Err(error) = stdin.write_all(output.as_bytes())
        && error.kind() != io::ErrorKind::BrokenPipe
    {
        return Err(error.into());
    }
    wait_pager(child)
}

/// Closes the pager's stdin, which tells it the output is complete, and waits
/// for it to exit.
fn wait_pager(mut pager: Child) -> Result<()> {
    drop(pager.stdin.take());
    let status = pager.wait()?;
    if status.success() {
        Ok(())
    } else {
        Err(AppError::PagerExit(status))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct BrokenPipeWriter;

    impl Write for BrokenPipeWriter {
        fn write(&mut self, _buffer: &[u8]) -> io::Result<usize> {
            Err(io::Error::from(io::ErrorKind::BrokenPipe))
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    fn options(layout: Layout) -> RenderOptions {
        RenderOptions {
            layout,
            row_limit: 0,
            escape_controls: false,
        }
    }

    fn result() -> ResultSet {
        ResultSet {
            has_row_description: true,
            columns: vec!["id".into(), "name".into()],
            rows: vec![
                vec![Some("1".into()), Some("Ada, \"A\"".into())],
                vec![Some("2".into()), None],
            ],
            total_rows: 2,
            affected_rows: 2,
            fields_truncated: false,
        }
    }

    #[test]
    fn renders_psql_table() {
        let text = render_table_output(&result(), 0);
        assert!(text.contains(" id | name "));
        assert!(text.contains("<null>"));
        assert!(text.ends_with("(2 rows)\n"));
    }

    #[test]
    fn renders_vertical_records() {
        let text = render_query(&result(), options(Layout::Vertical)).data;
        assert!(text.contains("-[ RECORD 1 ]-"));
        assert!(text.contains("name | Ada, \"A\""));
    }

    #[test]
    fn csv_quotes_special_values_and_uses_unquoted_null() {
        let text = render_query(&result(), options(Layout::Delimited(',')));
        assert_eq!(text.data, "id,name\n1,\"Ada, \"\"A\"\"\"\n2,\n");
        assert_eq!(text.diagnostic.as_deref(), Some("(2 rows)"));
    }

    #[test]
    fn delimited_output_distinguishes_null_from_empty_strings() {
        let result = ResultSet {
            has_row_description: true,
            columns: vec!["null_value".into(), "empty_value".into()],
            rows: vec![vec![None, Some(String::new())]],
            total_rows: 1,
            ..ResultSet::default()
        };
        assert_eq!(
            render_query(&result, options(Layout::Delimited(','))).data,
            "null_value,empty_value\n,\"\"\n"
        );
        assert_eq!(
            render_query(&result, options(Layout::Delimited('\t'))).data,
            "null_value\tempty_value\n\t\"\"\n"
        );
    }

    #[test]
    fn renders_zero_column_row_sets_as_queries() {
        let empty = ResultSet {
            has_row_description: true,
            ..ResultSet::default()
        };
        let rendered = render_query(&empty, options(Layout::Table));
        assert_eq!(rendered.data, "--\n(0 rows)\n");

        let rows = ResultSet {
            has_row_description: true,
            rows: vec![vec![], vec![], vec![]],
            total_rows: 3,
            ..ResultSet::default()
        };
        let rendered = render_query(&rows, options(Layout::Delimited(',')));
        assert_eq!(rendered.data, "\n\n\n\n");
        assert_eq!(rendered.diagnostic.as_deref(), Some("(3 rows)"));
    }

    #[test]
    fn retained_rows_respect_the_row_limit_and_field_width() {
        let mut result = ResultSet::with_columns(vec!["value".into()]);
        result.retain_human_row(&[Some("abcdef")], 2, 3);
        result.retain_human_row(&[None], 2, 3);
        result.retain_human_row(&[Some("dropped")], 2, 3);

        assert_eq!(result.total_rows, 3);
        assert_eq!(result.rows, [vec![Some("abc…".into())], vec![None]]);
        assert!(result.fields_truncated);
        assert!(
            render_table_output(&result, 2)
                .ends_with("(3 rows) [rows limited] [fields truncated]\n")
        );
    }

    #[test]
    fn quotes_identifiers_and_doubles_embedded_quotes() {
        assert_eq!(quote_identifier("a\"b"), "\"a\"\"b\"");
    }

    #[test]
    fn field_truncation_has_a_distinct_diagnostic() {
        let mut result = result();
        result.fields_truncated = true;
        let rendered = render_query(&result, options(Layout::Delimited(',')));
        assert_eq!(
            rendered.diagnostic.as_deref(),
            Some("(2 rows) [fields truncated]")
        );
    }

    #[test]
    fn distinguishes_stdout_and_stderr_broken_pipes() {
        let (sender, receiver) = mpsc::channel(1);
        sender
            .blocking_send(StreamOutput::Data("data".into()))
            .unwrap();
        drop(sender);
        let mut stdout = BrokenPipeWriter;
        assert!(matches!(
            write_stream(
                receiver,
                |data| write_stdout_to(&mut stdout, data.as_bytes()),
                |_| Ok(()),
            )
            .unwrap_err(),
            AppError::StdoutClosed
        ));

        let (sender, receiver) = mpsc::channel(1);
        sender
            .blocking_send(StreamOutput::Diagnostic("diagnostic".into()))
            .unwrap();
        drop(sender);
        let mut stderr = BrokenPipeWriter;
        assert!(matches!(
            write_stream(
                receiver,
                |_| Ok(()),
                |diagnostic| {
                    stderr.write_all(diagnostic.as_bytes())?;
                    Ok(())
                },
            )
            .unwrap_err(),
            AppError::Io(source) if source.kind() == io::ErrorKind::BrokenPipe
        ));
    }

    #[cfg(unix)]
    #[test]
    fn streamed_output_reports_how_the_pager_ended() {
        let stream = |data: String| {
            let (sender, receiver) = mpsc::channel(1);
            sender.blocking_send(StreamOutput::Data(data)).unwrap();
            drop(sender);
            receiver
        };

        write_stream_to_pager(
            stream("data".into()),
            "sh -c 'cat >/dev/null'",
            0,
            write_stdout,
            write_diagnostic,
        )
        .unwrap();

        let error = write_stream_to_pager(
            stream("data".into()),
            "sh -c 'cat >/dev/null; exit 7'",
            0,
            write_stdout,
            write_diagnostic,
        )
        .unwrap_err();
        assert!(matches!(error, AppError::PagerExit(status) if !status.success()));

        // More than a pipe buffer's worth, so the write blocks until the pager
        // exits without reading and the pipe breaks.
        let error = write_stream_to_pager(
            stream("x".repeat(1 << 20)),
            "sh -c 'exit 0'",
            0,
            write_stdout,
            write_diagnostic,
        )
        .unwrap_err();
        assert!(matches!(error, AppError::PagerClosed));
    }

    #[cfg(unix)]
    #[test]
    fn output_that_fits_on_screen_is_written_directly_in_order() {
        // A CSV batch: rows as data, each row count as a diagnostic, then a
        // table-format statement whose row count travels inside the data.
        let (sender, receiver) = mpsc::channel(5);
        for output in [
            StreamOutput::Data("a\n1\n".into()),
            StreamOutput::Diagnostic("(1 row)".into()),
            StreamOutput::Data("b\n2\n".into()),
            StreamOutput::Diagnostic("(1 row)".into()),
            StreamOutput::Data("1 row(s) affected\n".into()),
        ] {
            sender.blocking_send(output).unwrap();
        }
        drop(sender);

        let seen = std::cell::RefCell::new(Vec::new());
        // A pager that would fail if it were ever started.
        write_stream_to_pager(
            receiver,
            "sh -c 'exit 7'",
            10,
            |data| {
                seen.borrow_mut().push(format!("out:{data}"));
                Ok(())
            },
            |diagnostic| {
                seen.borrow_mut().push(format!("err:{diagnostic}"));
                Ok(())
            },
        )
        .unwrap();
        assert_eq!(
            seen.into_inner(),
            [
                "out:a\n1\n",
                "err:(1 row)",
                "out:b\n2\n",
                "err:(1 row)",
                "out:1 row(s) affected\n",
            ]
        );
    }

    #[cfg(unix)]
    #[test]
    fn the_pager_opens_once_output_reaches_a_screen() {
        let (sender, receiver) = mpsc::channel(3);
        for _ in 0..3 {
            sender
                .blocking_send(StreamOutput::Data("row\n".into()))
                .unwrap();
        }
        drop(sender);
        let error = write_stream_to_pager(
            receiver,
            "sh -c 'cat >/dev/null; exit 7'",
            3,
            write_stdout,
            write_diagnostic,
        )
        .unwrap_err();
        assert!(matches!(error, AppError::PagerExit(_)));
    }

    #[cfg(unix)]
    #[test]
    fn diagnostics_wait_until_the_pager_has_exited() {
        let marker = env::temp_dir().join(format!("pgline-pager-exited-{}", std::process::id()));
        let _ = std::fs::remove_file(&marker);
        let (sender, receiver) = mpsc::channel(2);
        sender
            .blocking_send(StreamOutput::Data("data".into()))
            .unwrap();
        sender
            .blocking_send(StreamOutput::Diagnostic("(1 row)".into()))
            .unwrap();
        drop(sender);

        // The pager leaves a marker as it exits, so a diagnostic written before
        // then would find no marker.
        let pager = format!("sh -c 'cat >/dev/null; touch {}'", marker.display());
        let mut seen = Vec::new();
        write_stream_to_pager(receiver, &pager, 0, write_stdout, |diagnostic| {
            assert!(
                marker.exists(),
                "diagnostic written while the pager was running"
            );
            seen.push(diagnostic.to_owned());
            Ok(())
        })
        .unwrap();
        assert_eq!(seen, ["(1 row)"]);
        let _ = std::fs::remove_file(&marker);
    }

    #[cfg(unix)]
    #[test]
    fn pager_nonzero_exit_is_reported() {
        let error = page_with_command("output", "sh -c 'cat >/dev/null; exit 7'").unwrap_err();
        assert!(matches!(error, AppError::PagerExit(status) if !status.success()));
    }

    #[test]
    fn terminal_output_visibly_escapes_unicode_bidi_controls() {
        let raw = "left\u{202e}override\u{2067}isolate\u{2069}";
        let safe = safe_terminal_text(raw);
        assert_eq!(safe, "left\\u{202e}override\\u{2067}isolate\\u{2069}");
        assert!(!safe.contains(['\u{202e}', '\u{2067}', '\u{2069}']));

        let editor_safe = safe_editor_text(raw);
        assert_eq!(editor_safe.len(), raw.len());
        assert!(!editor_safe.contains(['\u{202e}', '\u{2067}', '\u{2069}']));
    }

    #[test]
    fn terminal_output_visibly_escapes_controls() {
        let result = ResultSet {
            has_row_description: true,
            columns: vec!["danger\x1b]52;c;clipboard\x07".into()],
            rows: vec![vec![Some("line\r\n\x1b[2J\tbell\x07".into())]],
            total_rows: 1,
            affected_rows: 1,
            fields_truncated: false,
        };
        let table = render_table_output(&result, 0);
        assert!(!table.contains('\x1b'));
        assert!(!table.contains('\x07'));
        assert!(table.contains("\\x1b"));
        assert!(table.contains("\\r\\n"));
        assert!(table.contains("\\t"));

        let terminal_csv = render_query(
            &result,
            RenderOptions {
                escape_controls: true,
                ..options(Layout::Delimited(','))
            },
        );
        assert!(!terminal_csv.data.contains('\x1b'));
        assert!(terminal_csv.data.contains("\\x1b"));

        let redirected_csv = render_query(&result, options(Layout::Delimited(',')));
        assert!(redirected_csv.data.contains('\x1b'));
        assert_eq!(redirected_csv.diagnostic.as_deref(), Some("(1 row)"));
    }
}
