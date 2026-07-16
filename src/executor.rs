use std::{future::Future, io::IsTerminal, sync::Arc, time::Duration};

use futures_util::{StreamExt, pin_mut};
use tokio::sync::mpsc;
use tokio_postgres::{Client, SimpleColumn, SimpleQueryMessage, SimpleQueryRow, SimpleQueryStream};

use crate::{
    cli::OutputFormat,
    connection::CancellationTls,
    error::{AppError, Result},
    output::{self, ResultSet},
};

#[derive(Clone, Copy)]
pub struct ExecutionOptions {
    pub format: OutputFormat,
    pub expanded: bool,
    pub row_limit: usize,
    pub max_field_width: usize,
}

pub struct Execution {
    pub output: String,
    pub diagnostics: Vec<String>,
    pub completed_statements: usize,
    pub error: Option<tokio_postgres::Error>,
}

pub async fn execute(
    client: &Client,
    tls: &CancellationTls,
    sql: &str,
    options: ExecutionOptions,
    output_sink: Option<&mpsc::Sender<output::StreamOutput>>,
) -> Result<Execution> {
    let cancel_token = client.cancel_token();
    let stream = open_query_stream(client, sql, output_sink, &cancel_token, tls).await?;
    pin_mut!(stream);
    let mut state = ExecutionState::new(options, output_sink, &cancel_token, tls);

    loop {
        let message = tokio::select! {
            message = stream.next() => message,
            () = output_sink_closed(output_sink), if output_sink.is_some() => {
                state.handle_closed_output_sink().await?;
                return Err(AppError::OutputSinkClosed);
            }
            interrupt = tokio::signal::ctrl_c() => {
                interrupt?;
                state.handle_interrupt().await?;
                continue;
            }
        };
        let Some(message) = message else { break };
        match message {
            Ok(message) => state.handle_message(message).await?,
            Err(error) => {
                state.query_error = Some(error);
                break;
            }
        }
    }
    Ok(state.finish())
}

async fn open_query_stream<'a>(
    client: &'a Client,
    sql: &'a str,
    output_sink: Option<&mpsc::Sender<output::StreamOutput>>,
    cancel_token: &tokio_postgres::CancelToken,
    tls: &CancellationTls,
) -> Result<SimpleQueryStream> {
    tokio::select! {
        stream = client.simple_query_raw(sql) => Ok(stream?),
        () = output_sink_closed(output_sink), if output_sink.is_some() => {
            cancel_query(cancel_token, tls).await?;
            Err(AppError::OutputSinkClosed)
        }
    }
}

async fn output_sink_closed(output_sink: Option<&mpsc::Sender<output::StreamOutput>>) {
    match output_sink {
        Some(output_sink) => output_sink.closed().await,
        None => std::future::pending().await,
    }
}

struct ExecutionState<'a> {
    options: ExecutionOptions,
    output_sink: Option<&'a mpsc::Sender<output::StreamOutput>>,
    cancel_token: &'a tokio_postgres::CancelToken,
    tls: &'a CancellationTls,
    current_result: ResultSet,
    rendered: String,
    diagnostics: Vec<String>,
    completed_statements: usize,
    query_error: Option<tokio_postgres::Error>,
    cancelled: bool,
    stream_machine: bool,
    delimiter: char,
    terminal: bool,
    batch_output_limited: bool,
}

impl<'a> ExecutionState<'a> {
    fn new(
        options: ExecutionOptions,
        output_sink: Option<&'a mpsc::Sender<output::StreamOutput>>,
        cancel_token: &'a tokio_postgres::CancelToken,
        tls: &'a CancellationTls,
    ) -> Self {
        let stream_machine = output_sink.is_some()
            && !options.expanded
            && matches!(options.format, OutputFormat::Csv | OutputFormat::Tsv);
        let delimiter = match options.format {
            OutputFormat::Tsv => '\t',
            _ => ',',
        };
        Self {
            options,
            output_sink,
            cancel_token,
            tls,
            current_result: ResultSet::default(),
            rendered: String::new(),
            diagnostics: Vec::new(),
            completed_statements: 0,
            query_error: None,
            cancelled: false,
            stream_machine,
            delimiter,
            terminal: std::io::stdout().is_terminal(),
            batch_output_limited: false,
        }
    }

    async fn handle_message(&mut self, message: SimpleQueryMessage) -> Result<()> {
        match message {
            SimpleQueryMessage::RowDescription(columns) => self.begin_result(columns).await,
            SimpleQueryMessage::Row(row) => self.retain_or_stream_row(row).await,
            SimpleQueryMessage::CommandComplete(affected_rows) => {
                self.complete_statement(affected_rows).await
            }
            _ => Ok(()),
        }
    }

    async fn begin_result(&mut self, columns: Arc<[SimpleColumn]>) -> Result<()> {
        self.current_result = ResultSet {
            has_row_description: true,
            columns: columns
                .iter()
                .map(|column| column.name().to_owned())
                .collect(),
            ..ResultSet::default()
        };
        self.current_result.initialize_retention();
        if self.stream_machine {
            self.send(output::StreamOutput::Data(output::render_delimited_header(
                &self.current_result.columns,
                self.delimiter,
                self.terminal,
            )))
            .await?;
        }
        Ok(())
    }

    async fn retain_or_stream_row(&mut self, row: SimpleQueryRow) -> Result<()> {
        if !self.stream_machine {
            let values: Vec<Option<&str>> = (0..row.len()).map(|index| row.get(index)).collect();
            self.current_result.retain_human_row(
                &values,
                self.options.row_limit,
                self.options.max_field_width,
                output::MAX_HUMAN_RESULT_BYTES,
                output::MAX_HUMAN_RESULT_CELLS,
            );
            return Ok(());
        }

        self.current_result.total_rows += 1;
        if self.options.row_limit != 0 && self.current_result.total_rows > self.options.row_limit {
            return Ok(());
        }
        let values = (0..row.len())
            .map(|index| {
                row.get(index).map(|value| {
                    let (value, truncated) =
                        output::truncate_field(value, self.options.max_field_width);
                    self.current_result.fields_truncated |= truncated;
                    value
                })
            })
            .collect::<Vec<_>>();
        self.send(output::StreamOutput::Data(output::render_delimited_row(
            &values,
            self.delimiter,
            self.terminal,
        )))
        .await
    }

    async fn complete_statement(&mut self, affected_rows: u64) -> Result<()> {
        self.completed_statements += 1;
        self.current_result.affected_rows = affected_rows;
        let truncated =
            self.options.row_limit != 0 && self.current_result.total_rows > self.options.row_limit;
        let mut rendered = output::render_query(
            &self.current_result,
            self.options.format,
            self.options.expanded,
            truncated,
            self.terminal,
        );
        if self.stream_machine {
            rendered.data.clear();
        }
        self.emit_completed_statement(rendered).await?;
        self.current_result = ResultSet::default();
        Ok(())
    }

    async fn emit_completed_statement(&mut self, rendered: output::RenderedOutput) -> Result<()> {
        if self.output_sink.is_some() {
            if !rendered.data.is_empty() {
                self.send(output::StreamOutput::Data(rendered.data)).await?;
            }
            if let Some(diagnostic) = rendered.diagnostic {
                self.send(output::StreamOutput::Diagnostic(diagnostic))
                    .await?;
            }
        } else {
            append_bounded_batch_output(
                &mut self.rendered,
                &rendered.data,
                &mut self.diagnostics,
                &mut self.batch_output_limited,
                output::MAX_INTERACTIVE_BATCH_BYTES,
            );
            if let Some(diagnostic) = rendered.diagnostic {
                self.diagnostics.push(diagnostic);
            }
        }
        Ok(())
    }

    async fn send(&mut self, message: output::StreamOutput) -> Result<()> {
        send_output(
            self.output_sink.expect("stream output has an output sink"),
            message,
            self.cancel_token,
            self.tls,
            &mut self.cancelled,
        )
        .await
    }

    async fn handle_closed_output_sink(&mut self) -> Result<()> {
        if !self.cancelled {
            cancel_query(self.cancel_token, self.tls).await?;
        }
        Ok(())
    }

    async fn handle_interrupt(&mut self) -> Result<()> {
        if self.cancelled {
            return Err(AppError::Cancellation(
                "interrupted while waiting for the cancelled query to stop".into(),
            ));
        }
        self.cancelled = true;
        cancel_query(self.cancel_token, self.tls).await
    }

    fn finish(self) -> Execution {
        Execution {
            output: self.rendered,
            diagnostics: self.diagnostics,
            completed_statements: self.completed_statements,
            error: self.query_error,
        }
    }
}

fn append_bounded_batch_output(
    rendered: &mut String,
    output: &str,
    diagnostics: &mut Vec<String>,
    limited: &mut bool,
    limit: usize,
) {
    if *limited {
        return;
    }
    if rendered.len().saturating_add(output.len()) <= limit {
        rendered.push_str(output);
    } else {
        diagnostics.push("interactive batch output limited; additional results omitted".into());
        *limited = true;
    }
}

async fn send_output(
    sender: &mpsc::Sender<output::StreamOutput>,
    output: output::StreamOutput,
    cancel_token: &tokio_postgres::CancelToken,
    tls: &CancellationTls,
    cancelled: &mut bool,
) -> Result<()> {
    loop {
        tokio::select! {
            permit = sender.reserve() => {
                let permit = match permit {
                    Ok(permit) => permit,
                    Err(_) => {
                        if !*cancelled {
                            cancel_query(cancel_token, tls).await?;
                        }
                        return Err(AppError::OutputSinkClosed);
                    }
                };
                permit.send(output);
                // Give the writer a chance to drain the bounded channel before
                // polling more server messages from a large statement batch.
                tokio::task::yield_now().await;
                return Ok(());
            }
            interrupt = tokio::signal::ctrl_c() => {
                interrupt?;
                if *cancelled {
                    return Err(AppError::Cancellation(
                        "interrupted while waiting for output to be written".into(),
                    ));
                }
                *cancelled = true;
                cancel_query(cancel_token, tls).await?;
            }
        }
    }
}

pub enum CancellableQueryOutcome<T> {
    Completed(T),
    Cancelled {
        reason: String,
        backend_cancelled: bool,
    },
}

pub async fn await_cancellable_query<T, Query, Interrupt>(
    query: Query,
    interrupt: Interrupt,
    timeout: Option<Duration>,
    drain_timeout: Duration,
    cancel_token: &tokio_postgres::CancelToken,
    tls: &CancellationTls,
    operation: &str,
) -> Result<CancellableQueryOutcome<T>>
where
    Query: Future<Output = Result<T>>,
    Interrupt: Future<Output = std::io::Result<()>>,
{
    tokio::pin!(query);
    tokio::pin!(interrupt);
    let timeout = async {
        match timeout {
            Some(timeout) => tokio::time::sleep(timeout).await,
            None => std::future::pending().await,
        }
    };
    tokio::pin!(timeout);
    let reason = tokio::select! {
        result = &mut query => return result.map(CancellableQueryOutcome::Completed),
        result = &mut interrupt => {
            result?;
            format!("{operation} cancelled")
        }
        () = &mut timeout => format!("{operation} timed out"),
    };

    cancel_query(cancel_token, tls).await?;
    let drained = tokio::select! {
        result = &mut query => result,
        result = tokio::signal::ctrl_c() => {
            result?;
            return Err(AppError::Cancellation(format!(
                "interrupted while waiting for {operation} to stop"
            )));
        }
        () = tokio::time::sleep(drain_timeout) => {
            return Err(AppError::Cancellation(format!(
                "{operation} did not stop within {} seconds after cancellation",
                drain_timeout.as_secs()
            )));
        }
    };
    match drained {
        Ok(_) => Ok(CancellableQueryOutcome::Cancelled {
            reason,
            backend_cancelled: false,
        }),
        Err(AppError::Postgres(error)) if is_query_cancelled(&error) => {
            Ok(CancellableQueryOutcome::Cancelled {
                reason,
                backend_cancelled: true,
            })
        }
        Err(error) => Err(error),
    }
}

fn is_query_cancelled(error: &tokio_postgres::Error) -> bool {
    error
        .as_db_error()
        .is_some_and(|error| *error.code() == tokio_postgres::error::SqlState::QUERY_CANCELED)
}

pub async fn cancel_query(
    cancel_token: &tokio_postgres::CancelToken,
    tls: &CancellationTls,
) -> Result<()> {
    let cancel = async {
        match tls {
            CancellationTls::Disabled => cancel_token.cancel_query(tokio_postgres::NoTls).await,
            CancellationTls::Rustls(tls) => cancel_token.cancel_query(tls.clone()).await,
        }
    };
    tokio::select! {
        result = tokio::time::timeout(Duration::from_secs(5), cancel) => match result {
            Ok(Ok(())) => Ok(()),
            Ok(Err(error)) => Err(AppError::Cancellation(error.to_string())),
            Err(_) => Err(AppError::Cancellation("timed out after 5 seconds".into())),
        },
        interrupt = tokio::signal::ctrl_c() => {
            interrupt?;
            Err(AppError::Cancellation("interrupted while sending cancellation request".into()))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn options(format: OutputFormat, max_field_width: usize) -> ExecutionOptions {
        ExecutionOptions {
            format,
            expanded: false,
            row_limit: 100,
            max_field_width,
        }
    }

    #[test]
    fn interactive_batch_limit_omits_a_suffix() {
        let mut rendered = String::new();
        let mut diagnostics = Vec::new();
        let mut limited = false;
        append_bounded_batch_output(
            &mut rendered,
            "oversized",
            &mut diagnostics,
            &mut limited,
            4,
        );
        append_bounded_batch_output(&mut rendered, "later", &mut diagnostics, &mut limited, 4);
        assert!(rendered.is_empty());
        assert_eq!(diagnostics.len(), 1);
    }

    #[test]
    fn field_truncation_respects_unicode_boundaries() {
        assert_eq!(output::truncate_field("abcdef", 3), ("abc…".into(), true));
        assert_eq!(output::truncate_field("café", 4), ("café".into(), false));
        assert_eq!(output::truncate_field("café", 3), ("caf…".into(), true));
        assert_eq!(
            output::truncate_field("abcdef", 0),
            ("abcdef".into(), false)
        );
        assert_eq!(output::retained_field_len("😀😀", 1), 7);
        assert_eq!(output::retained_field_len("😀😀", 0), 8);
    }

    #[tokio::test]
    #[ignore = "requires PGLINE_TEST_URL"]
    async fn executes_basic_table_queries() {
        let database = crate::test_support::connect().await;

        let execution = execute(
            &database.client,
            &database.tls,
            "select 1 as answer, null as missing;",
            options(OutputFormat::Table, 500),
            None,
        )
        .await
        .unwrap();
        assert!(execution.output.contains("answer"));
        assert!(execution.output.contains("<null>"));
        assert!(execution.output.contains("(1 row)"));

        let execution = execute(
            &database.client,
            &database.tls,
            "SELECT FROM generate_series(1, 3)",
            options(OutputFormat::Table, 500),
            None,
        )
        .await
        .unwrap();
        assert!(execution.output.contains("(3 rows)"));
        assert!(!execution.output.contains("affected"));
    }

    #[tokio::test]
    #[ignore = "requires PGLINE_TEST_URL"]
    async fn enforces_human_output_limits() {
        let database = crate::test_support::connect().await;
        let execution = execute(
            &database.client,
            &database.tls,
            &format!(
                "SELECT FROM generate_series(1, {})",
                output::MAX_HUMAN_RESULT_CELLS + 1
            ),
            ExecutionOptions {
                row_limit: 0,
                ..options(OutputFormat::Vertical, 500)
            },
            None,
        )
        .await
        .unwrap();
        assert!(execution.output.contains(&format!(
            "({} rows) [output limited]",
            output::MAX_HUMAN_RESULT_CELLS + 1
        )));
        assert_eq!(
            execution.output.matches("-[ RECORD ").count(),
            output::MAX_HUMAN_RESULT_CELLS
        );
    }

    #[tokio::test]
    #[ignore = "requires PGLINE_TEST_URL"]
    async fn timed_out_startup_query_is_cancelled_and_drained() {
        let database = crate::test_support::connect().await;
        let query = async {
            database
                .client
                .query_one("SELECT pg_sleep(30)", &[])
                .await?;
            Ok(())
        };
        let outcome = await_cancellable_query(
            query,
            std::future::pending(),
            Some(Duration::from_millis(25)),
            Duration::from_secs(2),
            &database.client.cancel_token(),
            &database.tls,
            "test startup query",
        )
        .await
        .unwrap();
        assert!(matches!(
            outcome,
            CancellableQueryOutcome::Cancelled {
                backend_cancelled: true,
                ..
            }
        ));
        assert_eq!(
            database
                .client
                .query_one("SELECT 1", &[])
                .await
                .unwrap()
                .get::<_, i32>(0),
            1
        );
    }

    #[tokio::test]
    #[ignore = "requires PGLINE_TEST_URL"]
    async fn preserves_completed_statements_before_an_error() {
        let database = crate::test_support::connect().await;
        let execution = execute(
            &database.client,
            &database.tls,
            "SELECT 1 AS kept; SELECT missing_column;",
            options(OutputFormat::Table, 500),
            None,
        )
        .await
        .unwrap();
        assert_eq!(execution.completed_statements, 1);
        assert!(execution.error.is_some());
        assert!(execution.output.contains("kept"));
        assert!(execution.output.contains("1"));
    }

    #[tokio::test]
    #[ignore = "requires PGLINE_TEST_URL"]
    async fn applies_configured_field_truncation() {
        let database = crate::test_support::connect().await;
        let execution = execute(
            &database.client,
            &database.tls,
            "SELECT repeat('x', 600) AS value",
            options(OutputFormat::Csv, 500),
            None,
        )
        .await
        .unwrap();
        assert!(execution.output.contains('…'));
        assert!(
            execution
                .diagnostics
                .iter()
                .any(|diagnostic| diagnostic.contains("fields truncated"))
        );

        let execution = execute(
            &database.client,
            &database.tls,
            "SELECT repeat('x', 600) AS value",
            options(OutputFormat::Csv, 0),
            None,
        )
        .await
        .unwrap();
        assert!(!execution.output.contains('…'));
        assert_eq!(execution.output.matches('x').count(), 600);
    }

    #[tokio::test]
    #[ignore = "requires PGLINE_TEST_URL"]
    async fn enforces_wide_and_oversized_result_limits() {
        let database = crate::test_support::connect().await;
        let wide_columns = (0..101)
            .map(|index| format!("g AS c{index}"))
            .collect::<Vec<_>>()
            .join(", ");
        let execution = execute(
            &database.client,
            &database.tls,
            &format!("SELECT {wide_columns} FROM generate_series(1, 1000) AS rows(g)"),
            ExecutionOptions {
                row_limit: 0,
                ..options(OutputFormat::Table, 500)
            },
            None,
        )
        .await
        .unwrap();
        assert!(execution.output.contains("(1000 rows) [output limited]"));

        let execution = execute(
            &database.client,
            &database.tls,
            &format!(
                "SELECT value FROM (VALUES (repeat('x', {})), ('SECOND')) AS rows(value)",
                output::MAX_HUMAN_RESULT_BYTES + 1
            ),
            ExecutionOptions {
                row_limit: 0,
                ..options(OutputFormat::Table, 0)
            },
            None,
        )
        .await
        .unwrap();
        assert!(!execution.output.contains("SECOND"));
        assert!(execution.output.contains("(2 rows) [output limited]"));
    }

    #[tokio::test]
    #[ignore = "requires PGLINE_TEST_URL"]
    async fn streams_csv_rows() {
        let database = crate::test_support::connect().await;
        let (sender, mut receiver) = mpsc::channel(8);
        let execution = execute(
            &database.client,
            &database.tls,
            "SELECT value FROM generate_series(1, 2) AS values(value)",
            options(OutputFormat::Csv, 0),
            Some(&sender),
        )
        .await
        .unwrap();
        assert!(execution.output.is_empty());
        let streamed = std::iter::from_fn(|| receiver.try_recv().ok())
            .filter_map(|output| match output {
                output::StreamOutput::Data(data) => Some(data),
                output::StreamOutput::Diagnostic(_) => None,
            })
            .collect::<String>();
        assert_eq!(streamed, "value\n1\n2\n");
    }

    #[tokio::test]
    #[ignore = "requires PGLINE_TEST_URL"]
    async fn emits_completed_statements_incrementally() {
        let database = crate::test_support::connect().await;
        let (sender, mut receiver) = mpsc::channel(8);
        let execution = execute(
            &database.client,
            &database.tls,
            "SELECT 1 AS first; SELECT pg_sleep(0.1); SELECT missing_column",
            options(OutputFormat::Table, 500),
            Some(&sender),
        );
        pin_mut!(execution);
        let first = tokio::time::timeout(Duration::from_secs(1), async {
            tokio::select! {
                biased;
                first = receiver.recv() => first.expect("output channel closed early"),
                _ = &mut execution => panic!(
                    "query completed before its first statement was emitted"
                ),
            }
        })
        .await
        .expect("first statement was not emitted incrementally");
        assert!(matches!(first, output::StreamOutput::Data(data) if data.contains("first")));
        let execution = execution.await.unwrap();
        assert_eq!(execution.completed_statements, 2);
        assert!(execution.error.is_some());
        assert!(execution.output.is_empty());
    }

    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "requires PGLINE_TEST_URL"]
    async fn sink_closure_cancels_queries_and_preserves_connection_reuse() {
        let database = crate::test_support::connect().await;
        let observer = crate::test_support::connect().await;
        let pid: i32 = database
            .client
            .query_one("SELECT pg_backend_pid()", &[])
            .await
            .unwrap()
            .get(0);
        let (sender, receiver) = mpsc::channel(8);
        let execution = execute(
            &database.client,
            &database.tls,
            "SELECT pg_sleep(30) /* pgline_sink_cancel_test */",
            options(OutputFormat::Table, 500),
            Some(&sender),
        );
        tokio::time::timeout(Duration::from_secs(3), async {
            let close_active_sink = async {
                loop {
                    let active: bool = observer
                        .client
                        .query_one(
                            "SELECT EXISTS (SELECT 1 FROM pg_stat_activity \
                             WHERE pid = $1 AND state = 'active' \
                               AND query LIKE '%pgline_sink_cancel_test%')",
                            &[&pid],
                        )
                        .await
                        .unwrap()
                        .get(0);
                    if active {
                        drop(receiver);
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            };
            let (execution, ()) = tokio::join!(execution, close_active_sink);
            let error = match execution {
                Ok(_) => panic!("query succeeded after the output receiver closed"),
                Err(error) => error,
            };
            assert!(matches!(error, AppError::OutputSinkClosed));
            assert_eq!(
                database
                    .client
                    .query_one("SELECT 1", &[])
                    .await
                    .unwrap()
                    .get::<_, i32>(0),
                1
            );
        })
        .await
        .expect("active query was not cancelled and made reusable promptly");

        let (blocked_sender, blocked_receiver) = mpsc::channel(1);
        blocked_sender
            .send(output::StreamOutput::Data("channel prefill".into()))
            .await
            .unwrap();
        let mut cancelled = false;
        let cancel_token = database.client.cancel_token();
        tokio::time::timeout(Duration::from_secs(3), async {
            let sleeping = database
                .client
                .batch_execute("SELECT pg_sleep(30) /* pgline_reserve_cancel_test */");
            let blocked_send = send_output(
                &blocked_sender,
                output::StreamOutput::Data("blocked output".into()),
                &cancel_token,
                &database.tls,
                &mut cancelled,
            );
            let close_full_sink = async {
                loop {
                    let active: bool = observer
                        .client
                        .query_one(
                            "SELECT EXISTS (SELECT 1 FROM pg_stat_activity \
                             WHERE pid = $1 AND state = 'active' \
                               AND query LIKE '%pgline_reserve_cancel_test%')",
                            &[&pid],
                        )
                        .await
                        .unwrap()
                        .get(0);
                    if active {
                        drop(blocked_receiver);
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            };
            let (sleeping, blocked_send, ()) =
                tokio::join!(sleeping, blocked_send, close_full_sink);
            assert!(sleeping.is_err(), "sleeping query was not cancelled");
            assert!(matches!(blocked_send, Err(AppError::OutputSinkClosed)));
            assert_eq!(
                database
                    .client
                    .query_one("SELECT 1", &[])
                    .await
                    .unwrap()
                    .get::<_, i32>(0),
                1
            );
        })
        .await
        .expect("full output sink closure did not cancel the active query");
    }
}
