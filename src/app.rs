use std::{
    fs,
    future::Future,
    io::{self, IsTerminal},
    sync::{Arc, atomic::Ordering},
    time::{Duration, Instant},
};

use reedline::Signal;

use crate::{
    cli::{Cli, OutputFormat},
    commands::{self, SpecialCommand},
    connection::{self, CancellationTls, Database},
    copy_preflight::unsupported_copy_error,
    error::{AppError, Result},
    executor,
    metadata::{Metadata, MetadataStore},
    output,
    repl::{self, SqlPrompt, TransactionStatus},
    transaction,
};

pub struct App {
    database: Database,
    format: OutputFormat,
    expanded: bool,
    timing: bool,
    pager: bool,
    row_limit: usize,
    max_field_width: Option<usize>,
    transaction: TransactionStatus,
    last_query: Option<String>,
    metadata: MetadataStore,
    editor_rebuild_requested: bool,
}

impl App {
    pub fn new(cli: &Cli, database: Database) -> Self {
        Self {
            database,
            format: cli.format,
            expanded: cli.expanded,
            timing: cli.timing,
            pager: !cli.no_pager,
            row_limit: cli.row_limit,
            max_field_width: cli.max_field_width,
            transaction: TransactionStatus::Idle,
            last_query: None,
            metadata: MetadataStore::default(),
            editor_rebuild_requested: false,
        }
    }

    pub async fn run(mut self, cli: &Cli) -> Result<()> {
        if let Some(sql) = &cli.execute {
            if let Some(command) = commands::parse(sql) {
                match command {
                    SpecialCommand::Unknown(name) => {
                        return Err(AppError::InvalidCommand(format!(
                            "unknown command: \\{name}"
                        )));
                    }
                    SpecialCommand::Invalid(message) => {
                        return Err(AppError::InvalidCommand(message));
                    }
                    command => self.handle_command(command).await?,
                };
                return Ok(());
            }
            return self.run_query(sql, false).await;
        }
        if let Some(path) = &cli.file {
            let path = path.clone();
            let sql = tokio::task::spawn_blocking(move || fs::read_to_string(path)).await??;
            return self.run_query(&sql, false).await;
        }
        if !io::stdin().is_terminal() {
            let sql = tokio::task::spawn_blocking(|| io::read_to_string(io::stdin())).await??;
            return self.run_query(&sql, false).await;
        }
        self.run_interactive(cli).await
    }

    fn create_editor(&self, cli: &Cli) -> Result<reedline::Reedline> {
        repl::create_editor(
            cli,
            self.metadata.clone(),
            Arc::clone(&self.database.standard_conforming_strings),
        )
    }

    async fn run_interactive(&mut self, cli: &Cli) -> Result<()> {
        output::write_stdout(&format!(
            "Connected to {} as {}. Type \\? for help.\n",
            output::safe_terminal_text(&self.database.info.database),
            output::safe_terminal_text(&self.database.info.user)
        ))?;
        let metadata = load_startup_metadata(&self.database).await?;
        warn_if_metadata_truncated(&metadata);
        self.metadata.replace(metadata);
        let mut editor = self.create_editor(cli)?;
        let mut exit_armed = false;

        loop {
            let info = &self.database.info;
            let prompt = SqlPrompt::new(&info.user, &info.host, &info.database, self.transaction);
            let signal = tokio::task::block_in_place(|| editor.read_line(&prompt))?;
            let (next_exit_armed, should_exit) = exit_guard_transition(
                exit_armed,
                self.transaction,
                matches!(&signal, Signal::CtrlD),
            );
            exit_armed = next_exit_armed;
            if should_exit {
                break;
            }
            match signal {
                Signal::Success(input) => {
                    if let Some(command) = commands::parse(&input) {
                        if let SpecialCommand::Edit(seed) = command {
                            let initial =
                                seed.as_deref().or(self.last_query.as_deref()).unwrap_or("");
                            if let Some(query) =
                                tokio::task::block_in_place(|| commands::edit_query(initial))?
                            {
                                repl::replace_buffer(&mut editor, query);
                            }
                        } else {
                            let is_catalog = matches!(&command, SpecialCommand::Catalog(_));
                            match self.handle_command(command).await {
                                Ok(true) => break,
                                Ok(false) => {
                                    if std::mem::take(&mut self.editor_rebuild_requested) {
                                        editor = self.create_editor(cli)?;
                                    }
                                }
                                Err(AppError::Postgres(error))
                                    if !error.is_closed() && error.as_db_error().is_some() =>
                                {
                                    if is_catalog {
                                        self.transaction =
                                            transaction::after_catalog_operation(self.transaction);
                                    }
                                    eprintln!(
                                        "PostgreSQL error: {}",
                                        output::safe_terminal_text(&error.to_string())
                                    );
                                }
                                Err(AppError::InvalidCommand(message)) => {
                                    eprintln!("{}", output::safe_terminal_text(&message));
                                }
                                Err(error) => return Err(error),
                            }
                        }
                    } else if !input.trim().is_empty() {
                        self.run_query(&input, true).await?;
                    }
                }
                Signal::CtrlC => {
                    output::write_stdout("^C\n")?;
                }
                Signal::CtrlD => {
                    eprintln!("A transaction is active. Press Ctrl-D again to exit, or ROLLBACK;");
                }
                _ => {}
            }
        }
        Ok(())
    }

    async fn handle_command(&mut self, command: SpecialCommand) -> Result<bool> {
        match command {
            SpecialCommand::Help => output::write_stdout(commands::HELP)?,
            SpecialCommand::Quit => {
                if !matches!(self.transaction, TransactionStatus::Idle) {
                    eprintln!(
                        "A transaction is active; run ROLLBACK; before quitting (Ctrl-D twice to force)."
                    );
                } else {
                    return Ok(true);
                }
            }
            SpecialCommand::Edit(seed) => {
                let initial = seed.as_deref().or(self.last_query.as_deref()).unwrap_or("");
                if let Some(query) = tokio::task::block_in_place(|| commands::edit_query(initial))?
                {
                    output::write_stdout(&query)?;
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
            SpecialCommand::Connect(database) => self.reconnect(&database).await?,
            SpecialCommand::Catalog(command) => {
                let catalog = commands::catalog::run(
                    &self.database.client,
                    &command,
                    commands::catalog::CatalogLimits {
                        row_limit: self.row_limit,
                        max_field_width: self.max_field_width.unwrap_or(500),
                    },
                );
                match await_catalog_query(
                    catalog,
                    tokio::signal::ctrl_c(),
                    &self.database.client.cancel_token(),
                    &self.database.tls,
                )
                .await?
                {
                    CatalogQueryOutcome::Completed(rendered) => {
                        tokio::task::block_in_place(|| output::write(&rendered, self.pager))?;
                    }
                    CatalogQueryOutcome::Cancelled {
                        backend_cancelled, ..
                    } => {
                        let was_active = matches!(self.transaction, TransactionStatus::Active);
                        if backend_cancelled {
                            self.transaction =
                                transaction::after_catalog_operation(self.transaction);
                        }
                        if was_active && backend_cancelled {
                            eprintln!("Catalog query cancelled; transaction is now failed.");
                        } else {
                            eprintln!("Catalog query cancelled.");
                        }
                        return Ok(false);
                    }
                }
            }
            SpecialCommand::Invalid(message) => {
                eprintln!("{}", output::safe_terminal_text(&message));
            }
            SpecialCommand::Unknown(name) => {
                eprintln!(
                    "Unknown command: \\{}. Type \\? for help.",
                    output::safe_terminal_text(&name)
                );
            }
        }
        Ok(false)
    }

    async fn refresh_metadata(&mut self) -> Result<()> {
        match await_metadata_load(&self.database).await {
            Ok(executor::CancellableQueryOutcome::Completed(metadata)) => {
                warn_if_metadata_truncated(&metadata);
                self.metadata.replace(metadata);
                output::write_stdout("Completion metadata refreshed.\n")?;
            }
            Ok(executor::CancellableQueryOutcome::Cancelled {
                reason,
                backend_cancelled,
            }) => {
                if backend_cancelled {
                    self.transaction = transaction::after_catalog_operation(self.transaction);
                }
                eprintln!(
                    "{}; previous completion metadata retained.",
                    output::safe_terminal_text(&reason)
                );
            }
            Err(AppError::Postgres(error))
                if !error.is_closed() && error.as_db_error().is_some() =>
            {
                self.transaction = transaction::after_catalog_operation(self.transaction);
                eprintln!(
                    "Completion metadata refresh failed: {}; previous metadata retained.",
                    output::safe_terminal_text(&error.to_string())
                );
            }
            Err(error) => return Err(error),
        }
        Ok(())
    }

    async fn reconnect(&mut self, database: &str) -> Result<()> {
        if self.transaction != TransactionStatus::Idle {
            eprintln!("A transaction is active; run ROLLBACK; before changing connections.");
            return Ok(());
        }

        let new_database = match connection::connect_to_database(&self.database, database).await {
            Ok(database) => database,
            Err(error) => {
                eprintln!(
                    "Connection failed: {}; previous connection retained.",
                    output::safe_terminal_text(&error.to_string())
                );
                return Ok(());
            }
        };
        let metadata = match await_metadata_load(&new_database).await {
            Ok(executor::CancellableQueryOutcome::Completed(metadata)) => metadata,
            Ok(executor::CancellableQueryOutcome::Cancelled { reason, .. }) => {
                eprintln!(
                    "Connection setup failed: {}; previous connection retained.",
                    output::safe_terminal_text(&reason)
                );
                return Ok(());
            }
            Err(error) => {
                eprintln!(
                    "Connection setup failed: {}; previous connection retained.",
                    output::safe_terminal_text(&error.to_string())
                );
                return Ok(());
            }
        };
        warn_if_metadata_truncated(&metadata);
        self.metadata.replace(metadata);
        self.database = new_database;
        self.editor_rebuild_requested = true;
        output::write_stdout(&format!(
            "Connected to {} as {}.\n",
            output::safe_terminal_text(&self.database.info.database),
            output::safe_terminal_text(&self.database.info.user)
        ))?;
        Ok(())
    }

    async fn run_query(&mut self, sql: &str, interactive: bool) -> Result<()> {
        self.last_query = Some(sql.to_owned());
        let standard_conforming_strings = self
            .database
            .standard_conforming_strings
            .load(Ordering::Relaxed);
        if let Some(error) = unsupported_copy_error(sql, standard_conforming_strings) {
            if interactive {
                eprintln!("{}", output::safe_terminal_text(&error.to_string()));
                return Ok(());
            }
            return Err(error);
        }
        let max_field_width =
            effective_max_field_width(self.format, self.expanded, self.max_field_width);
        let query_started = Instant::now();
        let execution = match self.execute_sql(sql, interactive, max_field_width).await {
            Ok(execution) => execution,
            Err(error) => {
                self.transaction =
                    transaction::after_error(self.transaction, sql, 0, standard_conforming_strings);
                if interactive
                    && matches!(
                        &error,
                        AppError::Postgres(source)
                            if !source.is_closed() && source.as_db_error().is_some()
                    )
                {
                    eprintln!("{}", output::safe_terminal_text(&error.to_string()));
                    return Ok(());
                }
                return Err(error);
            }
        };

        self.update_transaction_after_execution(
            sql,
            execution.completed_statements,
            execution.error.is_some(),
            standard_conforming_strings,
        );
        self.present_execution(&execution, query_started.elapsed())?;

        if let Some(error) = execution.error {
            if interactive && !error.is_closed() && error.as_db_error().is_some() {
                eprintln!(
                    "PostgreSQL error: {}",
                    output::safe_terminal_text(&error.to_string())
                );
                Ok(())
            } else {
                Err(AppError::Postgres(error))
            }
        } else {
            Ok(())
        }
    }
    async fn execute_sql(
        &self,
        sql: &str,
        interactive: bool,
        max_field_width: usize,
    ) -> Result<executor::Execution> {
        let stream_machine =
            !self.expanded && matches!(self.format, OutputFormat::Csv | OutputFormat::Tsv);
        let (output_sink, stream_writer) = if interactive && !stream_machine {
            (None, None)
        } else {
            let (sink, writer) = output::stream_writer();
            (Some(sink), Some(writer))
        };
        let execution = executor::execute(
            &self.database.client,
            &self.database.tls,
            sql,
            executor::ExecutionOptions {
                format: self.format,
                expanded: self.expanded,
                row_limit: self.row_limit,
                max_field_width,
            },
            output_sink.as_ref(),
        )
        .await;
        drop(output_sink);
        let writer = match stream_writer {
            Some(writer) => writer.finish().await,
            None => Ok(()),
        };
        writer.and(execution)
    }

    fn update_transaction_after_execution(
        &mut self,
        sql: &str,
        completed_statements: usize,
        failed: bool,
        standard_conforming_strings: bool,
    ) {
        self.transaction = if failed {
            transaction::after_error(
                self.transaction,
                sql,
                completed_statements,
                standard_conforming_strings,
            )
        } else {
            transaction::after_success(self.transaction, sql, standard_conforming_strings)
        };
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
        if matches!(self.format, OutputFormat::Csv | OutputFormat::Tsv) && !self.expanded {
            eprintln!("Time: {}", format_duration(elapsed));
        } else {
            output::write_stdout(&format!("Time: {}\n", format_duration(elapsed)))?;
        }
        Ok(())
    }
}

const METADATA_LOAD_TIMEOUT: Duration = Duration::from_secs(5);

async fn load_startup_metadata(database: &Database) -> Result<Metadata> {
    match await_metadata_load(database).await {
        Ok(executor::CancellableQueryOutcome::Completed(metadata)) => Ok(metadata),
        Ok(executor::CancellableQueryOutcome::Cancelled { reason, .. }) => {
            eprintln!(
                "warning: metadata completion unavailable: {}",
                output::safe_terminal_text(&reason)
            );
            Ok(Metadata::default())
        }
        Err(AppError::Postgres(error)) if !error.is_closed() && error.as_db_error().is_some() => {
            eprintln!(
                "warning: metadata completion unavailable: {}",
                output::safe_terminal_text(&error.to_string())
            );
            Ok(Metadata::default())
        }
        Err(error) => Err(error),
    }
}

async fn await_metadata_load(
    database: &Database,
) -> Result<executor::CancellableQueryOutcome<Metadata>> {
    executor::await_cancellable_query(
        Metadata::load(&database.client),
        tokio::signal::ctrl_c(),
        Some(METADATA_LOAD_TIMEOUT),
        METADATA_LOAD_TIMEOUT,
        &database.client.cancel_token(),
        &database.tls,
        "metadata loading",
    )
    .await
}

fn warn_if_metadata_truncated(metadata: &Metadata) {
    if metadata.truncated {
        eprintln!("warning: completion metadata was truncated; some suggestions are unavailable");
    }
}

fn exit_guard_transition(
    armed: bool,
    transaction: TransactionStatus,
    ctrl_d: bool,
) -> (bool, bool) {
    if !ctrl_d {
        return (false, false);
    }
    if transaction == TransactionStatus::Idle || armed {
        (false, true)
    } else {
        (true, false)
    }
}

type CatalogQueryOutcome<T> = executor::CancellableQueryOutcome<T>;

async fn await_catalog_query<T, Query, Interrupt>(
    query: Query,
    interrupt: Interrupt,
    cancel_token: &tokio_postgres::CancelToken,
    tls: &CancellationTls,
) -> Result<CatalogQueryOutcome<T>>
where
    Query: Future<Output = Result<T>>,
    Interrupt: Future<Output = io::Result<()>>,
{
    executor::await_cancellable_query(
        query,
        interrupt,
        None,
        Duration::from_secs(5),
        cancel_token,
        tls,
        "catalog query",
    )
    .await
}

fn effective_max_field_width(
    format: OutputFormat,
    expanded: bool,
    configured: Option<usize>,
) -> usize {
    configured.unwrap_or({
        if matches!(format, OutputFormat::Csv | OutputFormat::Tsv) && !expanded {
            0
        } else {
            500
        }
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

    #[test]
    fn transaction_exit_confirmation_must_be_consecutive() {
        let (armed, exit) = exit_guard_transition(false, TransactionStatus::Active, true);
        assert!(armed);
        assert!(!exit);
        let (armed, exit) = exit_guard_transition(armed, TransactionStatus::Active, false);
        assert!(!armed);
        assert!(!exit);
        assert_eq!(
            exit_guard_transition(armed, TransactionStatus::Active, true),
            (true, false)
        );
        assert_eq!(
            exit_guard_transition(true, TransactionStatus::Active, true),
            (false, true)
        );
    }

    #[tokio::test]
    #[ignore = "requires PGLINE_TEST_URL"]
    async fn reconnect_switches_database_and_replaces_metadata() {
        let (cli, database) = crate::test_support::connect_with_cli(&[]).await;
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
        let mut app = App::new(&cli, database);
        app.metadata.replace(Metadata {
            relations: vec!["stale_relation".into()],
            ..Metadata::default()
        });

        app.reconnect(&target_database).await.unwrap();

        assert_eq!(app.database.info.database, target_database);
        assert!(app.editor_rebuild_requested);
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
        let (cli, database) = crate::test_support::connect_with_cli(&[]).await;
        let original_database = database.info.database.clone();
        let mut app = App::new(&cli, database);

        app.reconnect("pgline_database_that_does_not_exist")
            .await
            .unwrap();

        assert_eq!(app.database.info.database, original_database);
        assert!(!app.editor_rebuild_requested);
        assert_eq!(
            app.database
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
    async fn refresh_replaces_completion_metadata() {
        let (cli, database) = crate::test_support::connect_with_cli(&[]).await;
        database
            .client
            .batch_execute(
                "BEGIN; CREATE TEMP TABLE pgline_refresh_test \
                 (first_column integer, second_column text)",
            )
            .await
            .unwrap();
        let mut app = App::new(&cli, database);

        assert!(!app.handle_command(SpecialCommand::Refresh).await.unwrap());

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
        let database = crate::test_support::connect().await;
        let observer = crate::test_support::connect().await;
        let pid: i32 = database
            .client
            .query_one("SELECT pg_backend_pid()", &[])
            .await
            .unwrap()
            .get(0);
        let cancel_token = database.client.cancel_token();
        let query = async {
            database.client.query("SELECT pg_sleep(30)", &[]).await?;
            Ok::<(), AppError>(())
        };
        let interrupt = async {
            tokio::time::timeout(Duration::from_secs(2), async {
                loop {
                    let active: bool = observer
                        .client
                        .query_one(
                            "SELECT EXISTS(SELECT 1 FROM pg_stat_activity \
                             WHERE pid = $1 AND state = 'active' \
                               AND query LIKE '%pg_sleep(30)%')",
                            &[&pid],
                        )
                        .await
                        .unwrap()
                        .get(0);
                    if active {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "query did not become active"))
        };

        let outcome = await_catalog_query(query, interrupt, &cancel_token, &database.tls)
            .await
            .unwrap();
        assert!(matches!(
            outcome,
            CatalogQueryOutcome::Cancelled {
                backend_cancelled: true,
                ..
            }
        ));
        let value: i32 = tokio::time::timeout(
            Duration::from_secs(2),
            database.client.query_one("SELECT 1", &[]),
        )
        .await
        .expect("connection remained busy after catalog cancellation")
        .unwrap()
        .get(0);
        assert_eq!(value, 1);
    }

    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "requires PGLINE_TEST_URL"]
    async fn rejected_copy_protocol_forms_leave_connection_usable() {
        let (cli, database) = crate::test_support::connect_with_cli(&["--no-pager"]).await;
        let mut app = App::new(&cli, database);
        let atomic_batch = "CREATE FUNCTION pg_temp.pgline_copy_guard() RETURNS int LANGUAGE SQL \
             BEGIN ATOMIC SELECT 1; END; COPY (SELECT 1) TO STDOUT";
        assert!(matches!(
            app.run_query(atomic_batch, false).await,
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
                app.run_query(sql, false).await,
                Err(AppError::Unsupported(_))
            ));
            assert!(app.run_query(sql, true).await.is_ok());
            let value: i32 = app
                .database
                .client
                .query_one("SELECT 1", &[])
                .await
                .unwrap()
                .get(0);
            assert_eq!(value, 1);
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "requires PGLINE_TEST_URL"]
    async fn failed_commit_leaves_transaction_state_unknown() {
        let (cli, database) = crate::test_support::connect_with_cli(&["--no-pager"]).await;
        let mut app = App::new(&cli, database);
        app.run_query(
            "CREATE TEMP TABLE pgline_deferred_unique(\
                 value int, UNIQUE(value) DEFERRABLE INITIALLY DEFERRED)",
            true,
        )
        .await
        .unwrap();
        app.run_query("BEGIN", true).await.unwrap();
        app.run_query("INSERT INTO pgline_deferred_unique VALUES (1), (1)", true)
            .await
            .unwrap();
        app.run_query("COMMIT", true).await.unwrap();
        assert_eq!(app.transaction, TransactionStatus::Unknown);
        assert_eq!(
            app.database
                .client
                .query_one("SELECT 1", &[])
                .await
                .unwrap()
                .get::<_, i32>(0),
            1
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "requires PGLINE_TEST_URL"]
    async fn parameter_status_survives_a_later_batch_error() {
        let (cli, database) = crate::test_support::connect_with_cli(&["--no-pager"]).await;
        let setting = Arc::clone(&database.standard_conforming_strings);
        let mut app = App::new(&cli, database);
        app.run_query(
            "SET standard_conforming_strings = off; COMMIT; SELECT missing_column",
            true,
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
        app.run_query("SET standard_conforming_strings = on", true)
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
        let _ = tracing_subscriber::fmt()
            .with_ansi(false)
            .without_time()
            .try_init();
        let (cli, database) = crate::test_support::connect_with_cli(&["--no-pager"]).await;
        let mut app = App::new(&cli, database);
        tokio::time::timeout(
            Duration::from_secs(2),
            app.run_query(
                "DO $$ BEGIN RAISE NOTICE 'streamed notice'; END $$; SELECT 1",
                false,
            ),
        )
        .await
        .expect("streamed output deadlocked while handling a notice")
        .unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "requires PGLINE_TEST_URL"]
    async fn closed_connections_fail_metadata_loading() {
        let (cli, refresh_database) = crate::test_support::connect_with_cli(&[]).await;
        let startup_database = crate::connection::connect(&cli).await.unwrap();
        let killer = crate::connection::connect(&cli).await.unwrap();
        for database in [&refresh_database, &startup_database] {
            let pid: i32 = database
                .client
                .query_one("SELECT pg_backend_pid()", &[])
                .await
                .unwrap()
                .get(0);
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

        let mut app = App::new(&cli, refresh_database);
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
        let (cli, database) = crate::test_support::connect_with_cli(&["--no-pager"]).await;
        let pid: i32 = database
            .client
            .query_one("SELECT pg_backend_pid()", &[])
            .await
            .unwrap()
            .get(0);
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

        let mut app = App::new(&cli, database);
        let error = app
            .run_query("SELECT 1", true)
            .await
            .expect_err("closed connections must leave the REPL");
        assert!(matches!(error, AppError::Postgres(source) if source.is_closed()));
    }

    #[test]
    fn machine_output_defaults_follow_expanded_mode() {
        assert_eq!(effective_max_field_width(OutputFormat::Csv, false, None), 0);
        assert_eq!(
            effective_max_field_width(OutputFormat::Csv, true, None),
            500
        );
        assert_eq!(
            effective_max_field_width(OutputFormat::Table, false, None),
            500
        );
        assert_eq!(
            effective_max_field_width(OutputFormat::Csv, false, Some(12)),
            12
        );
        assert_eq!(
            effective_max_field_width(OutputFormat::Csv, true, Some(12)),
            12
        );
    }

    #[test]
    fn formats_short_and_long_durations() {
        assert_eq!(format_duration(Duration::from_millis(12)), "12.000 ms");
        assert_eq!(format_duration(Duration::from_millis(1250)), "1.250 s");
    }
}
