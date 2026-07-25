use std::{
    env,
    io::{self, IsTerminal, Write},
    process::{Command, Stdio},
};

use tabled::{builder::Builder, settings::Style};
use tokio::sync::mpsc;

pub const MAX_HUMAN_RESULT_BYTES: usize = 16 * 1024 * 1024;
pub const MAX_HUMAN_RESULT_CELLS: usize = 100_000;
pub const MAX_INTERACTIVE_BATCH_BYTES: usize = 32 * 1024 * 1024;

use crate::{
    cli::OutputFormat,
    error::{AppError, Result},
};

/// How much result data may be held in memory for human-readable output.
///
/// A budget may be shared by several [`ResultSet`]s — `\d pattern` renders one
/// table per category from a single query and caps their combined size — so the
/// running totals live here rather than on the result sets themselves. Once a
/// budget is exhausted it stays exhausted: later rows are counted but dropped,
/// and every result set that loses a row records that in `retention_limited`.
#[derive(Debug)]
pub struct RetentionBudget {
    retained_bytes: usize,
    retained_cells: usize,
    max_bytes: usize,
    max_cells: usize,
    exhausted: bool,
}

impl RetentionBudget {
    pub fn for_human_result() -> Self {
        Self::with_limits(MAX_HUMAN_RESULT_BYTES, MAX_HUMAN_RESULT_CELLS)
    }

    pub fn with_limits(max_bytes: usize, max_cells: usize) -> Self {
        Self {
            retained_bytes: 0,
            retained_cells: 0,
            max_bytes,
            max_cells,
            exhausted: false,
        }
    }

    pub fn is_exhausted(&self) -> bool {
        self.exhausted
    }

    /// Charges `bytes` and `cells` against the budget, reporting whether they
    /// fit. Anything that does not fit exhausts the budget permanently.
    pub fn take(&mut self, bytes: usize, cells: usize) -> bool {
        if self.exhausted
            || self.retained_bytes.saturating_add(bytes) > self.max_bytes
            || self.retained_cells.saturating_add(cells) > self.max_cells
        {
            self.exhausted = true;
            return false;
        }
        self.retained_bytes += bytes;
        self.retained_cells += cells;
        true
    }
}

/// A string that stops growing at a byte limit, reserving room for `marker` so
/// that [`finish`](Self::finish) can always report the truncation.
///
/// Once an append is refused the buffer stays limited and later appends are
/// no-ops, so callers can push a whole sequence of sections and test
/// [`is_limited`](Self::is_limited) once at the end rather than threading a
/// "still fits" flag through every step.
pub struct BoundedBuffer {
    text: String,
    limit: usize,
    marker: &'static str,
    limited: bool,
}

impl BoundedBuffer {
    pub fn new(limit: usize, marker: &'static str) -> Self {
        Self {
            text: String::new(),
            limit,
            marker,
            limited: false,
        }
    }

    pub fn is_limited(&self) -> bool {
        self.limited
    }

    /// Appends `section` whole or not at all, reporting whether it fit. Callers
    /// rendering independent sections want all-or-nothing so that output never
    /// ends mid-table.
    pub fn push(&mut self, section: &str) -> bool {
        if self.limited {
            return false;
        }
        if self.text.len().saturating_add(section.len()) > self.capacity() {
            self.limited = true;
            return false;
        }
        self.text.push_str(section);
        true
    }

    /// Appends as much of `text` as fits, cutting on a character boundary.
    ///
    /// Callers with a single rendered table prefer a partial table to none, so
    /// this trims rather than refusing. Text that fits within the limit is kept
    /// whole and leaves the buffer unlimited — only a genuine cut is reported.
    pub fn push_truncated(&mut self, text: &str) {
        if self.limited {
            return;
        }
        if self.text.len().saturating_add(text.len()) <= self.limit {
            self.text.push_str(text);
            return;
        }
        let mut room = self.capacity().saturating_sub(self.text.len());
        while !text.is_char_boundary(room) {
            room -= 1;
        }
        self.text.push_str(&text[..room]);
        self.limited = true;
    }

    pub fn finish(mut self) -> String {
        if self.limited {
            self.text.push_str(self.marker);
        }
        self.text
    }

    /// The room available to data, keeping the marker in reserve.
    fn capacity(&self) -> usize {
        self.limit.saturating_sub(self.marker.len())
    }
}

#[derive(Debug, Default)]
pub struct ResultSet {
    pub has_row_description: bool,
    pub columns: Vec<String>,
    pub rows: Vec<Vec<Option<String>>>,
    pub total_rows: usize,
    pub affected_rows: u64,
    pub fields_truncated: bool,
    pub retention_limited: bool,
}

impl ResultSet {
    /// Starts a result set for `columns`, charging the header to `budget`.
    pub fn with_columns(columns: Vec<String>, budget: &mut RetentionBudget) -> Self {
        let header_bytes = columns.iter().map(String::len).sum();
        let header_cells = columns.len();
        let mut result = Self {
            has_row_description: true,
            columns,
            ..Self::default()
        };
        result.retention_limited = !budget.take(header_bytes, header_cells);
        result
    }

    pub fn retain_human_row(
        &mut self,
        values: &[Option<&str>],
        row_limit: usize,
        max_field_width: usize,
        budget: &mut RetentionBudget,
    ) {
        self.total_rows += 1;
        if row_limit != 0 && self.total_rows > row_limit {
            return;
        }
        if budget.is_exhausted() {
            self.retention_limited = true;
            return;
        }

        // Empty rows still allocate and vertical output gives each one a
        // heading, so they must consume the cell budget.
        let row_cells = values.len().max(1);
        // A row whose total width overflows `usize` cannot fit any budget, so
        // treat the overflow itself as exhausting it.
        let fits = match values.iter().try_fold(0usize, |total, value| {
            total.checked_add(value.map_or(0, |value| retained_field_len(value, max_field_width)))
        }) {
            Some(row_bytes) => budget.take(row_bytes, row_cells),
            None => budget.take(usize::MAX, row_cells),
        };
        if !fits {
            self.retention_limited = true;
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

pub struct StreamWriter {
    task: tokio::task::JoinHandle<Result<()>>,
}

impl StreamWriter {
    pub async fn finish(self) -> Result<()> {
        self.task.await?
    }
}

pub fn stream_writer() -> (mpsc::Sender<StreamOutput>, StreamWriter) {
    let (sender, receiver) = mpsc::channel(8);
    let task = tokio::task::spawn_blocking(move || {
        write_stream(
            receiver,
            |data| {
                let stdout = io::stdout();
                let mut stdout = stdout.lock();
                write_stdout_to(&mut stdout, data.as_bytes())?;
                stdout.flush().map_err(stdout_error)
            },
            |diagnostic| {
                // PostgreSQL notices are logged to stderr by the connection
                // task, so never retain this lock while waiting for events.
                let stderr = io::stderr();
                let mut stderr = stderr.lock();
                stderr.write_all(diagnostic.as_bytes())?;
                stderr.write_all(b"\n")?;
                stderr.flush()?;
                Ok(())
            },
        )
    });
    (sender, StreamWriter { task })
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

    /// The field separator, for callers streaming rows one at a time instead of
    /// rendering a whole result set.
    pub fn delimiter(self) -> char {
        match self {
            Self::Delimited(delimiter) => delimiter,
            _ => ',',
        }
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
    if result.retention_limited {
        diagnostic.push_str(" [output limited]");
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

pub(crate) fn retained_field_len(value: &str, max_width: usize) -> usize {
    if max_width == 0 {
        return value.len();
    }
    value
        .char_indices()
        .nth(max_width)
        .map_or(value.len(), |(end, _)| end + '…'.len_utf8())
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
    let height = crossterm::terminal::size()
        .map(|(_, height)| height as usize)
        .unwrap_or(24);
    output.lines().count() >= height.saturating_sub(2)
}

fn page(output: &str) -> Result<()> {
    let pager = env::var("PAGER").unwrap_or_else(|_| {
        if cfg!(windows) {
            "more".into()
        } else {
            "less -SRFX".into()
        }
    });
    page_with_command(output, &pager)
}

fn page_with_command(output: &str, pager: &str) -> Result<()> {
    let mut parts = shlex::split(pager)
        .ok_or(AppError::InvalidPager)?
        .into_iter();
    let program = parts.next().ok_or(AppError::InvalidPager)?;
    let mut child = Command::new(program)
        .args(parts)
        .env("LESS", env::var("LESS").unwrap_or_else(|_| "-SRFX".into()))
        .stdin(Stdio::piped())
        .spawn()?;
    if let Some(mut stdin) = child.stdin.take()
        && let Err(error) = stdin.write_all(output.as_bytes())
        && error.kind() != io::ErrorKind::BrokenPipe
    {
        return Err(error.into());
    }
    let status = child.wait()?;
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
            ..ResultSet::default()
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
    fn human_row_retention_enforces_budgets_without_a_row_limit() {
        let mut budget = RetentionBudget::with_limits(8, 2);
        let mut result = ResultSet::with_columns(vec!["value".into()], &mut budget);
        result.retain_human_row(&[Some("one")], 0, 0, &mut budget);
        result.retain_human_row(&[Some("two")], 0, 0, &mut budget);
        result.retain_human_row(&[Some("three")], 0, 0, &mut budget);

        assert_eq!(result.total_rows, 3);
        assert_eq!(result.rows, [vec![Some("one".into())]]);
        assert!(result.retention_limited);
        assert!(render_table_output(&result, 0).contains("[output limited]"));
    }

    #[test]
    fn refused_sections_are_dropped_whole_and_stay_refused() {
        let mut buffer = BoundedBuffer::new(4, "");
        assert!(!buffer.push("oversized"));
        assert!(!buffer.push("ok"), "the buffer stays limited once refused");

        assert!(buffer.is_limited());
        assert!(buffer.finish().is_empty(), "no partial section was written");
    }

    #[test]
    fn a_refused_push_reserves_room_for_the_marker() {
        let marker = "[cut]";
        let mut buffer = BoundedBuffer::new(marker.len() + 5, marker);
        assert!(buffer.push("12345"));
        assert!(!buffer.push("6"));

        assert_eq!(buffer.finish(), format!("12345{marker}"));
    }

    #[test]
    fn truncating_pushes_keep_text_that_fits_whole() {
        let mut buffer = BoundedBuffer::new(5, "[cut]");
        buffer.push_truncated("12345");

        assert!(
            !buffer.is_limited(),
            "text within the limit is not a truncation"
        );
        assert_eq!(buffer.finish(), "12345");
    }

    #[test]
    fn truncating_pushes_cut_on_character_boundaries() {
        // "ααα" is six bytes and the limit is four, leaving three bytes for data
        // once the one-byte marker is reserved — mid-way through a character.
        let mut buffer = BoundedBuffer::new(4, "!");
        buffer.push_truncated("ααα");

        assert!(buffer.is_limited());
        assert_eq!(buffer.finish(), "α!");
    }

    #[test]
    fn several_result_sets_can_share_one_retention_budget() {
        let mut budget = RetentionBudget::with_limits(10, 10);
        let mut columns = ResultSet::with_columns(Vec::new(), &mut budget);
        let mut constraints = ResultSet::with_columns(Vec::new(), &mut budget);
        let mut indexes = ResultSet::with_columns(Vec::new(), &mut budget);

        columns.retain_human_row(&[Some("1234567")], 0, 0, &mut budget);
        constraints.retain_human_row(&[Some("abc")], 0, 0, &mut budget);
        indexes.retain_human_row(&[Some("x")], 0, 0, &mut budget);

        assert_eq!(columns.rows.len(), 1);
        assert_eq!(constraints.rows.len(), 1);
        assert!(
            indexes.rows.is_empty(),
            "the shared budget was already full"
        );
        assert!(indexes.retention_limited);
        assert!(budget.is_exhausted());
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
            ..ResultSet::default()
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
