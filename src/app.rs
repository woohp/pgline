use std::{
    fs,
    io::{self, IsTerminal},
    sync::{Arc, atomic::Ordering},
    time::{Duration, Instant},
};

use reedline::Signal;

use crate::{
    cli::Cli,
    commands::{self, CatalogCommand, SpecialCommand},
    connection::{self, Database},
    copy_preflight::unsupported_copy_error,
    error::{AppError, Result},
    executor::{self, CancellableQueryOutcome},
    metadata::{Metadata, MetadataStore},
    output::{self, Layout},
    repl::{self, SqlPrompt},
    transaction::{self, TransactionStatus},
};

/// What the REPL should do once a backslash command has run.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum CommandOutcome {
    Continue,
    /// The connection changed, so the editor has to be rebuilt against the new
    /// database's completions.
    RebuildEditor,
    /// `\e` produced a query. The REPL returns it to the prompt; one-shot runs
    /// print it.
    ReplaceBuffer(String),
    Exit,
}

/// How results are delivered and what a failed query means.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    /// At the REPL. Recoverable server errors are reported and the prompt
    /// returns, and human output is buffered so it can be paged.
    Repl,
    /// Running -c, -f or piped stdin. Any error ends the process, and output is
    /// streamed straight out.
    OneShot,
}

/// Limits applied at the REPL unless overridden, so an accidental `SELECT *`
/// at the prompt stays survivable. Scripts get complete output.
const REPL_ROW_LIMIT: usize = 1000;
const REPL_MAX_FIELD_WIDTH: usize = 500;
const METADATA_LOAD_TIMEOUT: Duration = Duration::from_secs(5);
const CATALOG_DRAIN_TIMEOUT: Duration = Duration::from_secs(5);

pub struct App {
    cli: Cli,
    database: Database,
    // Session settings that backslash commands can toggle after startup.
    expanded: bool,
    timing: bool,
    pager: bool,
    transaction: TransactionStatus,
    last_query: Option<String>,
    metadata: MetadataStore,
}

impl App {
    pub fn new(cli: Cli, database: Database) -> Self {
        Self {
            expanded: cli.expanded,
            timing: cli.timing,
            pager: !cli.no_pager,
            cli,
            database,
            transaction: TransactionStatus::Idle,
            last_query: None,
            metadata: MetadataStore::default(),
        }
    }

    pub async fn run(mut self) -> Result<()> {
        if let Some(sql) = self.cli.execute.clone() {
            if let Some(command) = commands::parse(&sql) {
                // One-shot runs exit after the command either way.
                if let CommandOutcome::ReplaceBuffer(query) =
                    self.handle_command(command?, Mode::OneShot).await?
                {
                    output::write_stdout(&query)?;
                }
                return Ok(());
            }
            return self.run_query(&sql, Mode::OneShot).await;
        }
        if let Some(path) = self.cli.file.clone() {
            let sql = tokio::task::spawn_blocking(move || fs::read_to_string(path)).await??;
            return self.run_query(&sql, Mode::OneShot).await;
        }
        if !io::stdin().is_terminal() {
            let sql = tokio::task::spawn_blocking(|| io::read_to_string(io::stdin())).await??;
            return self.run_query(&sql, Mode::OneShot).await;
        }
        self.run_interactive().await
    }

    fn create_editor(&self) -> Result<reedline::Reedline> {
        repl::create_editor(
            &self.cli,
            self.metadata.clone(),
            Arc::clone(&self.database.standard_conforming_strings),
        )
    }

    async fn run_interactive(&mut self) -> Result<()> {
        output::write_stdout(&format!(
            "Connected to {} as {}. Type \\? for help.\n",
            output::safe_terminal_text(&self.database.info.database),
            output::safe_terminal_text(&self.database.info.user)
        ))?;
        let metadata = load_startup_metadata(&self.database).await?;
        warn_if_metadata_truncated(&metadata);
        self.metadata.replace(metadata);
        let mut editor = self.create_editor()?;
        // Set by a Ctrl-D inside a transaction; a second consecutive Ctrl-D
        // then exits anyway.
        let mut exit_armed = false;

        loop {
            let info = &self.database.info;
            let prompt = SqlPrompt::new(&info.user, &info.host, &info.database, self.transaction);
            let signal = tokio::task::block_in_place(|| editor.read_line(&prompt))?;
            if let Signal::CtrlD = signal {
                if self.transaction == TransactionStatus::Idle || exit_armed {
                    break;
                }
                exit_armed = true;
                eprintln!("A transaction is active. Press Ctrl-D again to exit, or ROLLBACK;");
                continue;
            }
            exit_armed = false;
            match signal {
                Signal::Success(input) => {
                    let Some(command) = commands::parse(&input) else {
                        if !input.trim().is_empty() {
                            self.run_query(&input, Mode::Repl).await?;
                        }
                        continue;
                    };
                    let is_catalog = matches!(&command, Ok(SpecialCommand::Catalog(_)));
                    let outcome = match command {
                        Ok(command) => self.handle_command(command, Mode::Repl).await,
                        Err(error) => Err(error),
                    };
                    match outcome {
                        Ok(CommandOutcome::Exit) => break,
                        Ok(CommandOutcome::RebuildEditor) => {
                            editor = self.create_editor()?;
                        }
                        Ok(CommandOutcome::ReplaceBuffer(query)) => {
                            repl::replace_buffer(&mut editor, query);
                        }
                        Ok(CommandOutcome::Continue) => {}
                        Err(AppError::InvalidCommand(message)) => {
                            eprintln!("{}", output::safe_terminal_text(&message));
                        }
                        Err(error) => {
                            let Some(db_error) = error.as_recoverable_db_error() else {
                                return Err(error);
                            };
                            if is_catalog {
                                self.note_catalog_failure();
                            }
                            eprintln!(
                                "PostgreSQL error: {}",
                                output::safe_terminal_text(&db_error.to_string())
                            );
                        }
                    }
                }
                Signal::CtrlC => {
                    output::write_stdout("^C\n")?;
                }
                _ => {}
            }
        }
        Ok(())
    }

    async fn handle_command(
        &mut self,
        command: SpecialCommand,
        mode: Mode,
    ) -> Result<CommandOutcome> {
        match command {
            SpecialCommand::Help => output::write_stdout(commands::HELP)?,
            SpecialCommand::Quit => {
                if self.transaction != TransactionStatus::Idle {
                    eprintln!(
                        "A transaction is active; run ROLLBACK; before quitting (Ctrl-D twice to force)."
                    );
                } else {
                    return Ok(CommandOutcome::Exit);
                }
            }
            SpecialCommand::Edit(seed) => {
                let initial = seed.as_deref().or(self.last_query.as_deref()).unwrap_or("");
                if let Some(query) = tokio::task::block_in_place(|| commands::edit_query(initial))?
                {
                    return Ok(CommandOutcome::ReplaceBuffer(query));
                }
            }
            SpecialCommand::Expanded(value) => {
                self.expanded = value.unwrap_or(!self.expanded);
                output::write_stdout(&format!("Expanded output is {}.\n", on_off(self.expanded)))?;
            }
            SpecialCommand::Timing(value) => {
                self.timing = value.unwrap_or(!self.timing);
                output::write_stdout(&format!("Timing is {}.\n", on_off(self.timing)))?;
            }
            SpecialCommand::Pager(value) => {
                self.pager = value.unwrap_or(!self.pager);
                output::write_stdout(&format!("Pager is {}.\n", on_off(self.pager)))?;
            }
            SpecialCommand::Refresh => self.refresh_metadata().await?,
            SpecialCommand::Connect(database) => return self.reconnect(&database).await,
            SpecialCommand::Catalog(command) => self.run_catalog_command(&command, mode).await?,
        }
        Ok(CommandOutcome::Continue)
    }

    async fn run_catalog_command(&mut self, command: &CatalogCommand, mode: Mode) -> Result<()> {
        let catalog = commands::catalog::run(
            &self.database.client,
            command,
            commands::catalog::CatalogLimits {
                row_limit: effective_row_limit(mode, self.cli.row_limit),
                max_field_width: effective_max_field_width(
                    mode,
                    Layout::Table,
                    self.cli.max_field_width,
                ),
            },
        );
        let outcome = executor::await_cancellable_query(
            catalog,
            tokio::signal::ctrl_c(),
            None,
            CATALOG_DRAIN_TIMEOUT,
            &self.database.canceller(),
            "catalog query",
        )
        .await?;
        match outcome {
            CancellableQueryOutcome::Completed(rendered) => {
                tokio::task::block_in_place(|| output::write(&rendered, self.pager))?;
            }
            CancellableQueryOutcome::Cancelled {
                backend_cancelled, ..
            } => {
                let was_active = self.transaction == TransactionStatus::Active;
                if backend_cancelled {
                    self.note_catalog_failure();
                }
                if was_active && backend_cancelled {
                    eprintln!("Catalog query cancelled; transaction is now failed.");
                } else {
                    eprintln!("Catalog query cancelled.");
                }
            }
        }
        Ok(())
    }

    /// A catalog-flavored query failed or was cancelled on the server, which
    /// fails an active transaction.
    fn note_catalog_failure(&mut self) {
        self.transaction = transaction::after_catalog_operation(self.transaction);
    }

    async fn refresh_metadata(&mut self) -> Result<()> {
        match await_metadata_load(&self.database).await {
            Ok(CancellableQueryOutcome::Completed(metadata)) => {
                warn_if_metadata_truncated(&metadata);
                self.metadata.replace(metadata);
                output::write_stdout("Completion metadata refreshed.\n")?;
            }
            Ok(CancellableQueryOutcome::Cancelled {
                reason,
                backend_cancelled,
            }) => {
                if backend_cancelled {
                    self.note_catalog_failure();
                }
                eprintln!(
                    "{}; previous completion metadata retained.",
                    output::safe_terminal_text(&reason)
                );
            }
            Err(error) => {
                let Some(db_error) = error.as_recoverable_db_error() else {
                    return Err(error);
                };
                self.note_catalog_failure();
                eprintln!(
                    "Completion metadata refresh failed: {}; previous metadata retained.",
                    output::safe_terminal_text(&db_error.to_string())
                );
            }
        }
        Ok(())
    }

    async fn reconnect(&mut self, database: &str) -> Result<CommandOutcome> {
        if self.transaction != TransactionStatus::Idle {
            eprintln!("A transaction is active; run ROLLBACK; before changing connections.");
            return Ok(CommandOutcome::Continue);
        }

        let new_database = match connection::connect_to_database(&self.database, database).await {
            Ok(database) => database,
            Err(error) => {
                eprintln!(
                    "Connection failed: {}; previous connection retained.",
                    output::safe_terminal_text(&error.to_string())
                );
                return Ok(CommandOutcome::Continue);
            }
        };
        let metadata = match await_metadata_load(&new_database).await {
            Ok(CancellableQueryOutcome::Completed(metadata)) => metadata,
            Ok(CancellableQueryOutcome::Cancelled { reason, .. }) => {
                eprintln!(
                    "Connection setup failed: {}; previous connection retained.",
                    output::safe_terminal_text(&reason)
                );
                return Ok(CommandOutcome::Continue);
            }
            Err(error) => {
                eprintln!(
                    "Connection setup failed: {}; previous connection retained.",
                    output::safe_terminal_text(&error.to_string())
                );
                return Ok(CommandOutcome::Continue);
            }
        };
        warn_if_metadata_truncated(&metadata);
        self.metadata.replace(metadata);
        self.database = new_database;
        output::write_stdout(&format!(
            "Connected to {} as {}.\n",
            output::safe_terminal_text(&self.database.info.database),
            output::safe_terminal_text(&self.database.info.user)
        ))?;
        Ok(CommandOutcome::RebuildEditor)
    }

    fn layout(&self) -> Layout {
        Layout::new(self.cli.format, self.expanded)
    }

    async fn run_query(&mut self, sql: &str, mode: Mode) -> Result<()> {
        self.last_query = Some(sql.to_owned());
        let standard_conforming_strings = self
            .database
            .standard_conforming_strings
            .load(Ordering::Relaxed);
        if let Some(error) = unsupported_copy_error(sql, standard_conforming_strings) {
            // Nothing was sent to the server, so the session is intact.
            return report_or_fail(mode, error, true);
        }
        let query_started = Instant::now();
        let execution = match self.execute_sql(sql, mode).await {
            Ok(execution) => execution,
            Err(error) => {
                self.transaction =
                    transaction::after_error(self.transaction, sql, 0, standard_conforming_strings);
                let recoverable = error.as_recoverable_db_error().is_some();
                return report_or_fail(mode, error, recoverable);
            }
        };

        self.transaction = if execution.error.is_some() {
            transaction::after_error(
                self.transaction,
                sql,
                execution.completed_statements,
                standard_conforming_strings,
            )
        } else {
            transaction::after_success(self.transaction, sql, standard_conforming_strings)
        };
        self.present_execution(&execution, query_started.elapsed())?;

        match execution.error {
            Some(error) => {
                let error = AppError::Postgres(error);
                let recoverable = error.as_recoverable_db_error().is_some();
                report_or_fail(mode, error, recoverable)
            }
            None => Ok(()),
        }
    }

    async fn execute_sql(&self, sql: &str, mode: Mode) -> Result<executor::Execution> {
        let layout = self.layout();
        // The REPL buffers human output so it can be paged; everything else
        // streams to stdout as statements complete.
        let (output_sink, writer) = if mode == Mode::Repl && !layout.is_machine_readable() {
            (None, None)
        } else {
            let (sink, writer) = output::stream_writer();
            (Some(sink), Some(writer))
        };
        let execution = executor::execute(
            &self.database.client,
            &self.database.canceller(),
            sql,
            executor::ExecutionOptions {
                format: self.cli.format,
                expanded: self.expanded,
                row_limit: effective_row_limit(mode, self.cli.row_limit),
                max_field_width: effective_max_field_width(mode, layout, self.cli.max_field_width),
            },
            output_sink.as_ref(),
        )
        .await;
        // Closing the sink lets the writer thread drain and exit.
        drop(output_sink);
        if let Some(writer) = writer {
            writer.await??;
        }
        execution
    }

    fn present_execution(&self, execution: &executor::Execution, elapsed: Duration) -> Result<()> {
        if !execution.output.is_empty() {
            tokio::task::block_in_place(|| output::write(&execution.output, self.pager))?;
        }
        for diagnostic in &execution.diagnostics {
            eprintln!("{diagnostic}");
        }
        if !self.timing {
            return Ok(());
        }
        if self.layout().is_machine_readable() {
            eprintln!("Time: {}", format_duration(elapsed));
        } else {
            output::write_stdout(&format!("Time: {}\n", format_duration(elapsed)))?;
        }
        Ok(())
    }
}

/// At the REPL a recoverable failure is printed and the prompt returns; a
/// one-shot run, or an unrecoverable failure, ends the process instead.
fn report_or_fail(mode: Mode, error: AppError, recoverable: bool) -> Result<()> {
    if mode == Mode::Repl && recoverable {
        eprintln!("{}", output::safe_terminal_text(&error.to_string()));
        return Ok(());
    }
    Err(error)
}

async fn load_startup_metadata(database: &Database) -> Result<Metadata> {
    match await_metadata_load(database).await {
        Ok(CancellableQueryOutcome::Completed(metadata)) => Ok(metadata),
        Ok(CancellableQueryOutcome::Cancelled { reason, .. }) => {
            eprintln!(
                "warning: metadata completion unavailable: {}",
                output::safe_terminal_text(&reason)
            );
            Ok(Metadata::default())
        }
        Err(error) => {
            let Some(db_error) = error.as_recoverable_db_error() else {
                return Err(error);
            };
            eprintln!(
                "warning: metadata completion unavailable: {}",
                output::safe_terminal_text(&db_error.to_string())
            );
            Ok(Metadata::default())
        }
    }
}

async fn await_metadata_load(database: &Database) -> Result<CancellableQueryOutcome<Metadata>> {
    executor::await_cancellable_query(
        Metadata::load(&database.client),
        tokio::signal::ctrl_c(),
        Some(METADATA_LOAD_TIMEOUT),
        METADATA_LOAD_TIMEOUT,
        &database.canceller(),
        "metadata loading",
    )
    .await
}

fn warn_if_metadata_truncated(metadata: &Metadata) {
    if metadata.truncated {
        eprintln!("warning: completion metadata was truncated; some suggestions are unavailable");
    }
}

fn effective_row_limit(mode: Mode, configured: Option<usize>) -> usize {
    configured.unwrap_or(match mode {
        Mode::Repl => REPL_ROW_LIMIT,
        Mode::OneShot => 0,
    })
}

/// Only human output at the REPL truncates fields by default. Scripts and
/// machine-readable output keep whole fields unless a width was asked for.
fn effective_max_field_width(mode: Mode, layout: Layout, configured: Option<usize>) -> usize {
    configured.unwrap_or(if mode == Mode::Repl && !layout.is_machine_readable() {
        REPL_MAX_FIELD_WIDTH
    } else {
        0
    })
}

fn on_off(value: bool) -> &'static str {
    if value { "on" } else { "off" }
}

fn format_duration(duration: Duration) -> String {
    if duration.as_secs() > 0 {
        format!("{:.3} s", duration.as_secs_f64())
    } else {
        format!("{:.3} ms", duration.as_secs_f64() * 1000.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        cli::OutputFormat,
        test_support::{
            assert_connection_usable, backend_pid, connect, connect_with_cli,
            wait_until_query_active,
        },
    };

    #[tokio::test]
    #[ignore = "requires PGLINE_TEST_URL"]
    async fn reconnect_switches_database_and_replaces_metadata() {
        let (cli, database) = connect_with_cli(&[]).await;
        let Some(target_database) = database
            .client
            .query_opt(
                "SELECT datname FROM pg_catalog.pg_database \
                 WHERE datallowconn AND NOT datistemplate \
                   AND datname <> current_database() \
                   AND pg_catalog.has_database_privilege(datname, 'CONNECT') \
                 ORDER BY datname LIMIT 1",
                &[],
            )
            .await
            .unwrap()
            .map(|row| row.get::<_, String>(0))
        else {
            return;
        };
        let mut app = App::new(cli, database);
        app.metadata.replace(Metadata {
            relations: vec!["stale_relation".into()],
            ..Metadata::default()
        });

        let outcome = app.reconnect(&target_database).await.unwrap();

        assert_eq!(outcome, CommandOutcome::RebuildEditor);
        assert_eq!(app.database.info.database, target_database);
        app.metadata.with_current(|metadata| {
            assert!(!metadata.relations.contains(&"stale_relation".into()));
        });
        assert_eq!(
            app.database
                .client
                .query_one("SELECT current_database()", &[])
                .await
                .unwrap()
                .get::<_, String>(0),
            target_database
        );
    }

    #[tokio::test]
    #[ignore = "requires PGLINE_TEST_URL"]
    async fn failed_reconnect_retains_the_current_connection() {
        let (cli, database) = connect_with_cli(&[]).await;
        let original_database = database.info.database.clone();
        let mut app = App::new(cli, database);

        let outcome = app
            .reconnect("pgline_database_that_does_not_exist")
            .await
            .unwrap();

        assert_eq!(outcome, CommandOutcome::Continue);
        assert_eq!(app.database.info.database, original_database);
        assert_connection_usable(&app.database.client).await;
    }

    #[tokio::test]
    #[ignore = "requires PGLINE_TEST_URL"]
    async fn refresh_replaces_completion_metadata() {
        let (cli, database) = connect_with_cli(&[]).await;
        database
            .client
            .batch_execute(
                "BEGIN; CREATE TEMP TABLE pgline_refresh_test \
                 (first_column integer, second_column text)",
            )
            .await
            .unwrap();
        let mut app = App::new(cli, database);

        assert_eq!(
            app.handle_command(SpecialCommand::Refresh, Mode::Repl)
                .await
                .unwrap(),
            CommandOutcome::Continue
        );

        app.metadata.with_current(|metadata| {
            assert!(metadata.relations.contains(&"pgline_refresh_test".into()));
            assert_eq!(
                metadata.relation_columns["pgline_refresh_test"],
                ["first_column", "second_column"]
            );
        });
        app.database.client.batch_execute("ROLLBACK").await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "requires PGLINE_TEST_URL"]
    async fn cancelled_catalog_query_finishes_before_connection_reuse() {
        let database = connect().await;
        let observer = connect().await;
        let pid = backend_pid(&database.client).await;
        let query = async {
            database.client.query("SELECT pg_sleep(30)", &[]).await?;
            Ok::<(), AppError>(())
        };
        let interrupt = async {
            tokio::time::timeout(
                Duration::from_secs(2),
                wait_until_query_active(&observer.client, pid, "pg_sleep(30)"),
            )
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "query did not become active"))
        };

        let outcome = executor::await_cancellable_query(
            query,
            interrupt,
            None,
            CATALOG_DRAIN_TIMEOUT,
            &database.canceller(),
            "catalog query",
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
        tokio::time::timeout(
            Duration::from_secs(2),
            assert_connection_usable(&database.client),
        )
        .await
        .expect("connection remained busy after catalog cancellation");
    }

    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "requires PGLINE_TEST_URL"]
    async fn rejected_copy_protocol_forms_leave_connection_usable() {
        let (cli, database) = connect_with_cli(&["--no-pager"]).await;
        let mut app = App::new(cli, database);
        let atomic_batch = "CREATE FUNCTION pg_temp.pgline_copy_guard() RETURNS int LANGUAGE SQL \
             BEGIN ATOMIC SELECT 1; END; COPY (SELECT 1) TO STDOUT";
        assert!(matches!(
            app.run_query(atomic_batch, Mode::OneShot).await,
            Err(AppError::Unsupported(_))
        ));
        let function_exists: bool = app
            .database
            .client
            .query_one(
                "SELECT to_regprocedure('pg_temp.pgline_copy_guard()') IS NOT NULL",
                &[],
            )
            .await
            .unwrap()
            .get(0);
        assert!(
            !function_exists,
            "COPY guard submitted a preceding statement"
        );

        for sql in [
            "COPY pg_catalog.pg_class FROM STDIN",
            "COPY (SELECT 1) TO STDOUT",
        ] {
            assert!(matches!(
                app.run_query(sql, Mode::OneShot).await,
                Err(AppError::Unsupported(_))
            ));
            assert!(app.run_query(sql, Mode::Repl).await.is_ok());
            assert_connection_usable(&app.database.client).await;
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "requires PGLINE_TEST_URL"]
    async fn failed_commit_leaves_transaction_state_unknown() {
        let (cli, database) = connect_with_cli(&["--no-pager"]).await;
        let mut app = App::new(cli, database);
        app.run_query(
            "CREATE TEMP TABLE pgline_deferred_unique(\
                 value int, UNIQUE(value) DEFERRABLE INITIALLY DEFERRED)",
            Mode::Repl,
        )
        .await
        .unwrap();
        app.run_query("BEGIN", Mode::Repl).await.unwrap();
        app.run_query(
            "INSERT INTO pgline_deferred_unique VALUES (1), (1)",
            Mode::Repl,
        )
        .await
        .unwrap();
        app.run_query("COMMIT", Mode::Repl).await.unwrap();
        assert_eq!(app.transaction, TransactionStatus::Unknown);
        assert_connection_usable(&app.database.client).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "requires PGLINE_TEST_URL"]
    async fn parameter_status_survives_a_later_batch_error() {
        let (cli, database) = connect_with_cli(&["--no-pager"]).await;
        let setting = Arc::clone(&database.standard_conforming_strings);
        let mut app = App::new(cli, database);
        app.run_query(
            "SET standard_conforming_strings = off; COMMIT; SELECT missing_column",
            Mode::Repl,
        )
        .await
        .unwrap();
        tokio::time::timeout(Duration::from_secs(1), async {
            while setting.load(Ordering::Relaxed) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("parameter status did not update after the failed batch");
        app.run_query("SET standard_conforming_strings = on", Mode::Repl)
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(1), async {
            while !setting.load(Ordering::Relaxed) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("parameter status did not update after restoring the setting");
    }

    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "requires PGLINE_TEST_URL"]
    async fn notices_do_not_deadlock_streamed_output() {
        let (cli, database) = connect_with_cli(&["--no-pager"]).await;
        let mut app = App::new(cli, database);
        tokio::time::timeout(
            Duration::from_secs(2),
            app.run_query(
                "DO $$ BEGIN RAISE NOTICE 'streamed notice'; END $$; SELECT 1",
                Mode::OneShot,
            ),
        )
        .await
        .expect("streamed output deadlocked while handling a notice")
        .unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "requires PGLINE_TEST_URL"]
    async fn closed_connections_fail_metadata_loading() {
        let (cli, refresh_database) = connect_with_cli(&[]).await;
        let startup_database = crate::connection::connect(&cli).await.unwrap();
        let killer = crate::connection::connect(&cli).await.unwrap();
        for database in [&refresh_database, &startup_database] {
            let pid = backend_pid(&database.client).await;
            assert!(
                killer
                    .client
                    .query_one("SELECT pg_terminate_backend($1)", &[&pid])
                    .await
                    .unwrap()
                    .get::<_, bool>(0)
            );
        }
        tokio::time::sleep(Duration::from_millis(25)).await;

        let mut app = App::new(cli, refresh_database);
        let refresh_error = app
            .refresh_metadata()
            .await
            .expect_err("refresh must propagate a closed connection");
        assert!(matches!(refresh_error, AppError::Postgres(source) if source.is_closed()));

        let startup_error = load_startup_metadata(&startup_database)
            .await
            .expect_err("startup metadata must propagate a closed connection");
        assert!(matches!(startup_error, AppError::Postgres(source) if source.is_closed()));
    }

    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "requires PGLINE_TEST_URL"]
    async fn closed_query_connection_exits_interactive_execution() {
        let (cli, database) = connect_with_cli(&["--no-pager"]).await;
        let pid = backend_pid(&database.client).await;
        let killer = crate::connection::connect(&cli).await.unwrap();
        assert!(
            killer
                .client
                .query_one("SELECT pg_terminate_backend($1)", &[&pid])
                .await
                .unwrap()
                .get::<_, bool>(0)
        );
        tokio::time::sleep(Duration::from_millis(25)).await;

        let mut app = App::new(cli, database);
        let error = app
            .run_query("SELECT 1", Mode::Repl)
            .await
            .expect_err("closed connections must leave the REPL");
        assert!(matches!(error, AppError::Postgres(source) if source.is_closed()));
    }

    #[test]
    fn default_limits_apply_only_to_human_output_at_the_repl() {
        let csv = Layout::new(OutputFormat::Csv, false);
        let expanded_csv = Layout::new(OutputFormat::Csv, true);
        let table = Layout::new(OutputFormat::Table, false);

        assert_eq!(effective_row_limit(Mode::Repl, None), 1000);
        assert_eq!(effective_row_limit(Mode::OneShot, None), 0);
        assert_eq!(effective_row_limit(Mode::OneShot, Some(7)), 7);

        assert_eq!(effective_max_field_width(Mode::Repl, csv, None), 0);
        assert_eq!(
            effective_max_field_width(Mode::Repl, expanded_csv, None),
            500
        );
        assert_eq!(effective_max_field_width(Mode::Repl, table, None), 500);
        assert_eq!(effective_max_field_width(Mode::OneShot, table, None), 0);
        assert_eq!(effective_max_field_width(Mode::Repl, csv, Some(12)), 12);
        assert_eq!(
            effective_max_field_width(Mode::OneShot, table, Some(12)),
            12
        );
    }

    #[test]
    fn formats_short_and_long_durations() {
        assert_eq!(format_duration(Duration::from_millis(12)), "12.000 ms");
        assert_eq!(format_duration(Duration::from_millis(1250)), "1.250 s");
    }
}
