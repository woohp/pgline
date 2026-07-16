use std::{
    future::poll_fn,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use rand::seq::SliceRandom;
use rustls_tokio_postgres::{MakeRustlsConnect, config_platform_verifier};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_postgres::{AsyncMessage, Client, Config, Connection, NoTls, config::LoadBalanceHosts};

#[cfg(test)]
use tokio_postgres::config::SslMode;

mod pgpass;
mod settings;
use pgpass::PasswordFile;
use settings::{base_config, can_use_tls, config_for_target, connection_target_count, uses_no_tls};

use crate::{
    cli::Cli,
    error::{AppError, Result},
    executor, output,
};

#[derive(Clone)]
pub enum CancellationTls {
    Disabled,
    Rustls(MakeRustlsConnect),
}

#[derive(Debug, Clone)]
pub struct ConnectionInfo {
    pub user: String,
    pub database: String,
    pub host: String,
}

pub struct Database {
    pub client: Client,
    pub tls: CancellationTls,
    pub info: ConnectionInfo,
    pub standard_conforming_strings: Arc<AtomicBool>,
    reconnect_config: Config,
    password_prompt_allowed: bool,
}

#[derive(Debug)]
struct ConnectAttemptsError {
    errors: Vec<tokio_postgres::Error>,
    saw_password_error: bool,
}

impl ConnectAttemptsError {
    fn preferred(&self) -> &tokio_postgres::Error {
        self.errors
            .iter()
            .max_by_key(|error| connection_error_priority(error))
            .expect("a failed connection has an error")
    }

    fn into_preferred(self) -> tokio_postgres::Error {
        let index = self
            .errors
            .iter()
            .enumerate()
            .max_by_key(|(_, error)| connection_error_priority(error))
            .map(|(index, _)| index)
            .expect("a failed connection has an error");
        self.errors.into_iter().nth(index).unwrap()
    }
}

impl std::fmt::Display for ConnectAttemptsError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.preferred().fmt(formatter)
    }
}

const STARTUP_QUERY_TIMEOUT: Duration = Duration::from_secs(5);

type Connected = (Client, CancellationTls, Arc<AtomicBool>);

pub async fn connect(cli: &Cli) -> Result<Database> {
    connect_configured(base_config(cli)?, cli.password, !cli.no_password).await
}

pub async fn connect_to_database(current: &Database, database: &str) -> Result<Database> {
    let mut config = current.reconnect_config.clone();
    config.dbname(database);
    connect_configured(config, false, current.password_prompt_allowed).await
}

async fn connect_configured(
    mut config: Config,
    prompt_before_connecting: bool,
    password_prompt_allowed: bool,
) -> Result<Database> {
    if prompt_before_connecting {
        let password = rpassword::prompt_password("Password: ")?;
        config.password(password);
    }

    let rustls = if can_use_tls(&config) {
        let tls_config =
            config_platform_verifier().map_err(|error| AppError::Tls(error.to_string()))?;
        Some(MakeRustlsConnect::new(tls_config))
    } else {
        None
    };
    let (client, tls, standard_conforming_strings) =
        match connect_config_with_interrupt(&config, rustls.as_ref()).await? {
            Ok(connected) => connected,
            Err(error) if should_retry_with_password(password_prompt_allowed, &config, &error) => {
                let password = rpassword::prompt_password("Password: ")?;
                config.password(password);
                connect_config_with_interrupt(&config, rustls.as_ref())
                    .await?
                    .map_err(ConnectAttemptsError::into_preferred)?
            }
            Err(error) => return Err(error.into_preferred().into()),
        };

    // Password-file credentials are added only to per-target attempts. The
    // reusable config therefore preserves all targets and re-evaluates
    // database-specific password-file entries after `\c`.
    let reconnect_config = config.clone();
    let row = query_connection_info(&client, &tls).await?;
    let info = ConnectionInfo {
        user: row.get(0),
        database: row.get(1),
        host: row.get(2),
    };

    Ok(Database {
        client,
        tls,
        info,
        standard_conforming_strings,
        reconnect_config,
        password_prompt_allowed,
    })
}

async fn connect_config_with_interrupt(
    config: &Config,
    rustls: Option<&MakeRustlsConnect>,
) -> Result<std::result::Result<Connected, ConnectAttemptsError>> {
    let password_file = if config.get_password().is_some() {
        PasswordFile::empty()
    } else {
        tokio::select! {
            result = tokio::task::spawn_blocking(PasswordFile::load) => result
                .map_err(|error| AppError::Internal(format!("password-file task failed: {error}")))?,
            interrupt = tokio::signal::ctrl_c() => {
                interrupt?;
                return Err(AppError::Cancellation("connection attempt cancelled".into()));
            }
        }
    };
    tokio::select! {
        result = connect_config_with_password_file(config, rustls, &password_file) => Ok(result),
        interrupt = tokio::signal::ctrl_c() => {
            interrupt?;
            Err(AppError::Cancellation("connection attempt cancelled".into()))
        }
    }
}

async fn query_connection_info(
    client: &Client,
    tls: &CancellationTls,
) -> Result<tokio_postgres::Row> {
    let query = async {
        Ok(client
            .query_one(
                "SELECT current_user, current_database(), COALESCE(inet_server_addr()::text, 'local')",
                &[],
            )
            .await?)
    };
    match executor::await_cancellable_query(
        query,
        tokio::signal::ctrl_c(),
        Some(STARTUP_QUERY_TIMEOUT),
        STARTUP_QUERY_TIMEOUT,
        &client.cancel_token(),
        tls,
        "connection information query",
    )
    .await?
    {
        executor::CancellableQueryOutcome::Completed(row) => Ok(row),
        executor::CancellableQueryOutcome::Cancelled { reason, .. } => {
            Err(AppError::Cancellation(reason))
        }
    }
}

#[cfg(test)]
async fn connect_config(
    config: &Config,
    rustls: Option<&MakeRustlsConnect>,
) -> std::result::Result<Connected, ConnectAttemptsError> {
    let password_file = if config.get_password().is_some() {
        PasswordFile::empty()
    } else {
        PasswordFile::load()
    };
    connect_config_with_password_file(config, rustls, &password_file).await
}

async fn connect_config_with_password_file(
    config: &Config,
    rustls: Option<&MakeRustlsConnect>,
    password_file: &PasswordFile,
) -> std::result::Result<Connected, ConnectAttemptsError> {
    let target_count = connection_target_count(config);
    if target_count > 1 {
        let mut errors = Vec::new();
        let mut saw_password_error = false;
        let mut indices = (0..target_count).collect::<Vec<_>>();
        if config.get_load_balance_hosts() == LoadBalanceHosts::Random {
            indices.shuffle(&mut rand::rng());
        }
        for index in indices {
            let mut attempt = config_for_target(config, index);
            password_file.apply(&mut attempt);
            match connect_single(&attempt, rustls).await {
                Ok(connected) => return Ok(connected),
                Err(error) => {
                    saw_password_error |= is_password_error(&error);
                    errors.push(error);
                }
            }
        }
        return Err(ConnectAttemptsError {
            errors,
            saw_password_error,
        });
    }
    let mut attempt = config.clone();
    password_file.apply(&mut attempt);
    connect_single(&attempt, rustls)
        .await
        .map_err(|error| ConnectAttemptsError {
            saw_password_error: is_password_error(&error),
            errors: vec![error],
        })
}

async fn connect_single(
    config: &Config,
    rustls: Option<&MakeRustlsConnect>,
) -> std::result::Result<Connected, tokio_postgres::Error> {
    if uses_no_tls(config) {
        let (client, mut connection) = config.connect(NoTls).await?;
        let standard_conforming_strings = Arc::new(AtomicBool::new(
            connection.parameter("standard_conforming_strings") != Some("off"),
        ));
        let observed_setting = Arc::clone(&standard_conforming_strings);
        tokio::spawn(async move {
            drive_connection(&mut connection, &observed_setting).await;
        });
        Ok((
            client,
            CancellationTls::Disabled,
            standard_conforming_strings,
        ))
    } else {
        let rustls = rustls.expect("TLS connector is initialized for TLS-capable targets");
        let (client, mut connection) = config.connect(rustls.clone()).await?;
        let standard_conforming_strings = Arc::new(AtomicBool::new(
            connection.parameter("standard_conforming_strings") != Some("off"),
        ));
        let observed_setting = Arc::clone(&standard_conforming_strings);
        tokio::spawn(async move {
            drive_connection(&mut connection, &observed_setting).await;
        });
        Ok((
            client,
            CancellationTls::Rustls(rustls.clone()),
            standard_conforming_strings,
        ))
    }
}

async fn drive_connection<S, T>(
    connection: &mut Connection<S, T>,
    standard_conforming_strings: &AtomicBool,
) where
    S: AsyncRead + AsyncWrite + Unpin,
    T: AsyncRead + AsyncWrite + Unpin,
{
    loop {
        let message = poll_fn(|context| {
            let message = connection.poll_message(context);
            if let Some(value) = connection.parameter("standard_conforming_strings") {
                standard_conforming_strings.store(value != "off", Ordering::Relaxed);
            }
            message
        })
        .await;
        match message {
            Some(Ok(AsyncMessage::Notice(notice))) => {
                let severity = output::safe_terminal_text(notice.severity());
                let message = output::safe_terminal_text(notice.message());
                tracing::info!("{severity}: {message}");
            }
            Some(Ok(AsyncMessage::Notification(_))) => {}
            Some(Err(error)) => {
                let error = output::safe_terminal_text(&error.to_string());
                tracing::error!(%error, "PostgreSQL connection closed");
                break;
            }
            None => break,
            _ => {}
        }
    }
}

fn should_retry_with_password(
    password_prompt_allowed: bool,
    config: &Config,
    error: &ConnectAttemptsError,
) -> bool {
    password_prompt_allowed && config.get_password().is_none() && error.saw_password_error
}

fn connection_error_priority(error: &tokio_postgres::Error) -> u8 {
    let message = error.to_string().to_ascii_lowercase();
    let is_tls_error = ["tls", "ssl", "certificate"]
        .iter()
        .any(|needle| message.contains(needle));
    if is_password_error(error) || is_authentication_error_message(&message) {
        3
    } else if is_tls_error {
        2
    } else if error.as_db_error().is_some() {
        1
    } else {
        0
    }
}

fn is_authentication_error_message(message: &str) -> bool {
    message.contains("authentication error")
}

fn is_password_error(error: &tokio_postgres::Error) -> bool {
    error.as_db_error().is_some_and(|error| {
        matches!(
            error.code(),
            &tokio_postgres::error::SqlState::INVALID_PASSWORD
                | &tokio_postgres::error::SqlState::INVALID_AUTHORIZATION_SPECIFICATION
        )
    }) || std::iter::successors(Some(error as &dyn std::error::Error), |error| {
        error.source()
    })
    .any(|error| error.to_string().to_ascii_lowercase().contains("password"))
}

#[cfg(test)]
mod tests {
    use super::settings::{supply_tls_hosts_for_hostaddrs, with_network_overrides};
    use super::*;

    #[tokio::test]
    #[ignore = "requires PGLINE_TEST_URL and PostgreSQL on 127.0.0.1"]
    async fn hostaddr_only_connections_select_the_right_tls_connector() {
        let url = std::env::var("PGLINE_TEST_URL").expect("PGLINE_TEST_URL is required");
        let original: Config = url.parse().unwrap();
        let mut config = Config::new();
        if let Some(user) = original.get_user() {
            config.user(user);
        }
        if let Some(password) = original.get_password() {
            config.password(password);
        }
        if let Some(database) = original.get_dbname() {
            config.dbname(database);
        }
        config.hostaddr("127.0.0.1".parse().unwrap());
        config.ssl_mode(SslMode::Disable);
        let tls = MakeRustlsConnect::new(config_platform_verifier().unwrap());
        let (client, selected_tls, _) = connect_config(&config, Some(&tls)).await.unwrap();
        assert!(matches!(selected_tls, CancellationTls::Disabled));
        assert_eq!(
            client
                .query_one("SELECT 1", &[])
                .await
                .unwrap()
                .get::<_, i32>(0),
            1
        );

        config.ssl_mode(SslMode::Require);
        supply_tls_hosts_for_hostaddrs(&mut config);
        if let Err(error) = connect_config(&config, Some(&tls)).await {
            assert!(!error.to_string().contains("invalid dns name"));
        }
    }

    #[tokio::test]
    #[ignore = "requires PGLINE_TEST_URL with password-authenticated TCP PostgreSQL"]
    async fn password_file_credentials_authenticate() {
        let url = std::env::var("PGLINE_TEST_URL").expect("PGLINE_TEST_URL is required");
        let original: Config = url.parse().unwrap();
        let Some(tokio_postgres::config::Host::Tcp(host)) = original.get_hosts().first() else {
            return;
        };
        let Some(password) = original.get_password() else {
            return;
        };
        let mut config = Config::new();
        config.host(host);
        config.port(original.get_ports().first().copied().unwrap_or(5432));
        config.user(original.get_user().unwrap_or("postgres"));
        if let Some(database) = original.get_dbname() {
            config.dbname(database);
        }
        config.ssl_mode(SslMode::Disable);

        let password_file = PasswordFile::matching_all(password);
        let (client, _, _) = connect_config_with_password_file(&config, None, &password_file)
            .await
            .unwrap();
        assert!(config.get_password().is_none());
        assert_eq!(
            client
                .query_one("SELECT 1", &[])
                .await
                .unwrap()
                .get::<_, i32>(0),
            1
        );
    }

    #[tokio::test]
    #[ignore = "requires PGLINE_TEST_URL with password-authenticated TCP PostgreSQL"]
    async fn multi_host_errors_retain_earlier_password_failures() {
        let url = std::env::var("PGLINE_TEST_URL").expect("PGLINE_TEST_URL is required");
        let original: Config = url.parse().unwrap();
        if !original
            .get_hosts()
            .iter()
            .any(|host| matches!(host, tokio_postgres::config::Host::Tcp(_)))
        {
            return;
        }
        let mut config = Config::new();
        config.user(original.get_user().unwrap_or("postgres"));
        if let Some(database) = original.get_dbname() {
            config.dbname(database);
        }
        config.hostaddr("127.0.0.1".parse().unwrap());
        config.port(original.get_ports().first().copied().unwrap_or(5432));
        config.hostaddr("127.0.0.1".parse().unwrap());
        config.port(1);
        config.connect_timeout(std::time::Duration::from_millis(500));
        config.ssl_mode(SslMode::Disable);

        let error = match connect_config(&config, None).await {
            Ok(_) => panic!("multi-host connection unexpectedly succeeded"),
            Err(error) => error,
        };
        assert!(error.saw_password_error);
        assert!(!is_password_error(error.errors.last().unwrap()));
        assert!(is_password_error(error.preferred()));

        config.password("definitely-wrong-password");
        let error = match connect_config(&config, None).await {
            Ok(_) => panic!("multi-host connection unexpectedly succeeded"),
            Err(error) => error,
        };
        assert!(is_password_error(error.preferred()));
    }

    #[cfg(unix)]
    #[tokio::test]
    #[ignore = "requires PGLINE_TEST_URL with a Unix socket host"]
    async fn mixed_hosts_fall_back_from_tcp_to_unix_socket() {
        let url = std::env::var("PGLINE_TEST_URL").expect("PGLINE_TEST_URL is required");
        let original: Config = url.parse().unwrap();
        let socket = original
            .get_hosts()
            .iter()
            .find_map(|host| match host {
                tokio_postgres::config::Host::Unix(path) => Some(path.clone()),
                _ => None,
            })
            .expect("PGLINE_TEST_URL must contain a Unix socket host");
        let port = original.get_ports().first().copied();
        let mut config = with_network_overrides(original, Some("203.0.113.1"), port);
        config.host_path(socket);
        config.connect_timeout(std::time::Duration::from_millis(100));
        config.ssl_mode(SslMode::Require);

        let database = connect_configured(config, false, false).await.unwrap();
        assert!(matches!(database.tls, CancellationTls::Disabled));
        assert_eq!(connection_target_count(&database.reconnect_config), 2);
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

    #[test]
    fn recognizes_protocol_authentication_errors_for_priority() {
        assert!(is_authentication_error_message(
            "error performing SCRAM authentication: authentication error"
        ));
    }
}
