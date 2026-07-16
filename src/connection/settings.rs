use std::{env, str::FromStr, time::Duration};

use tokio_postgres::{Config, config::SslMode};

use crate::{
    cli::Cli,
    error::{AppError, Result},
};

const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

#[cfg(test)]
fn has_mixed_host_types(config: &Config) -> bool {
    #[cfg(unix)]
    {
        let has_unix = config
            .get_hosts()
            .iter()
            .any(|host| matches!(host, tokio_postgres::config::Host::Unix(_)));
        let has_tcp = config
            .get_hosts()
            .iter()
            .any(|host| matches!(host, tokio_postgres::config::Host::Tcp(_)));
        has_unix && has_tcp
    }
    #[cfg(not(unix))]
    false
}

pub(super) fn connection_target_count(config: &Config) -> usize {
    config
        .get_hosts()
        .len()
        .max(config.get_hostaddrs().len())
        .max(1)
}

pub(super) fn config_for_target(original: &Config, index: usize) -> Config {
    let host = match original.get_hosts() {
        [] => None,
        [host] => Some(host),
        hosts => hosts.get(index),
    };
    let address = match original.get_hostaddrs() {
        [] => None,
        [address] => Some(*address),
        addresses => addresses.get(index).copied(),
    };
    let port = match original.get_ports() {
        [] => None,
        [port] => Some(*port),
        ports => ports.get(index).copied(),
    };
    let mut config = copy_connection_settings(original);
    match host {
        Some(tokio_postgres::config::Host::Tcp(host)) => {
            config.host(host);
        }
        #[cfg(unix)]
        Some(tokio_postgres::config::Host::Unix(path)) => {
            config.host_path(path);
            config.ssl_mode(SslMode::Disable);
        }
        None => {
            config.host(
                address
                    .expect("network target has a host or address")
                    .to_string(),
            );
        }
    }
    #[cfg(unix)]
    let unix_socket = matches!(host, Some(tokio_postgres::config::Host::Unix(_)));
    #[cfg(not(unix))]
    let unix_socket = false;
    if !unix_socket && let Some(address) = address {
        config.hostaddr(address);
    }
    if let Some(port) = port {
        config.port(port);
    }
    config
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ConnectionInput {
    DatabaseName,
    Uri,
    Keywords,
}

pub(super) fn base_config(cli: &Cli) -> Result<Config> {
    let connection_input = cli
        .connection
        .as_deref()
        .map_or(ConnectionInput::DatabaseName, classify_connection_input);
    let is_dsn = connection_input != ConnectionInput::DatabaseName;
    let dsn_has_sslmode = is_dsn
        && cli
            .connection
            .as_deref()
            .is_some_and(|value| dsn_has_parameter(value, "sslmode"));
    let mut config = if is_dsn {
        let dsn = normalize_dsn_ssl_mode(cli.connection.as_deref().unwrap())?;
        Config::from_str(&dsn).map_err(|error| AppError::Connection(error.to_string()))?
    } else {
        Config::new()
    };

    apply_environment_defaults(&mut config, dsn_has_sslmode, &PgEnvironment::read())?;
    if !is_dsn && let Some(database) = cli.connection.as_deref() {
        config.dbname(database);
    }
    if config.get_connect_timeout().is_none() {
        config.connect_timeout(DEFAULT_CONNECT_TIMEOUT);
    }

    if cli.host.is_some() || cli.port.is_some() {
        config = with_network_overrides(config, cli.host.as_deref(), cli.port);
    }
    if let Some(user) = &cli.user {
        config.user(user);
    }
    if let Some(database) = &cli.database {
        config.dbname(database);
    }

    apply_host_defaults(&mut config);
    supply_tls_hosts_for_hostaddrs(&mut config);
    validate_network_config(&config)?;
    Ok(config)
}

fn classify_connection_input(value: &str) -> ConnectionInput {
    let trimmed = value.trim_start();
    let scheme_end = trimmed.find(':').unwrap_or(0);
    if scheme_end != 0
        && matches!(
            &trimmed[..scheme_end].to_ascii_lowercase()[..],
            "postgres" | "postgresql"
        )
        && trimmed[scheme_end..].starts_with("://")
    {
        return ConnectionInput::Uri;
    }

    let key_end = trimmed
        .find(|character: char| character == '=' || character.is_whitespace())
        .unwrap_or(trimmed.len());
    let key = &trimmed[..key_end];
    let has_equals = trimmed[key_end..].trim_start().starts_with('=');
    if has_equals && is_connection_keyword(key) {
        ConnectionInput::Keywords
    } else {
        ConnectionInput::DatabaseName
    }
}

fn is_connection_keyword(value: &str) -> bool {
    matches!(
        value.to_ascii_lowercase().as_str(),
        "application_name"
            | "channel_binding"
            | "connect_timeout"
            | "dbname"
            | "host"
            | "hostaddr"
            | "keepalives"
            | "keepalives_idle"
            | "keepalives_interval"
            | "keepalives_retries"
            | "load_balance_hosts"
            | "options"
            | "password"
            | "port"
            | "sslmode"
            | "sslnegotiation"
            | "target_session_attrs"
            | "tcp_user_timeout"
            | "user"
    )
}

pub(super) fn supply_tls_hosts_for_hostaddrs(config: &mut Config) {
    if config.get_ssl_mode() != SslMode::Disable && config.get_hosts().is_empty() {
        for address in config.get_hostaddrs().to_vec() {
            config.host(address.to_string());
        }
    }
}

fn validate_network_config(config: &Config) -> Result<()> {
    let host_count = config.get_hosts().len();
    let address_count = config.get_hostaddrs().len();
    if host_count != 0 && address_count != 0 && host_count != address_count {
        return Err(AppError::Connection(format!(
            "number of hosts ({host_count}) differs from number of host addresses ({address_count})"
        )));
    }
    let target_count = host_count.max(address_count);
    let port_count = config.get_ports().len();
    if port_count > 1 && port_count != target_count {
        return Err(AppError::Connection(format!(
            "number of ports ({port_count}) differs from number of connection targets ({target_count})"
        )));
    }
    Ok(())
}

fn apply_host_defaults(config: &mut Config) {
    if config.get_hosts().is_empty() && config.get_hostaddrs().is_empty() {
        #[cfg(unix)]
        {
            config.host_path("/var/run/postgresql");
            config.host_path("/tmp");
        }
        #[cfg(not(unix))]
        config.host("localhost");
    }

    #[cfg(unix)]
    if !config.get_hosts().is_empty()
        && config
            .get_hosts()
            .iter()
            .all(|host| matches!(host, tokio_postgres::config::Host::Unix(_)))
    {
        // libpq ignores sslmode for Unix-domain sockets. rustls cannot verify a
        // filesystem path as a hostname, so always use the native socket directly.
        config.ssl_mode(SslMode::Disable);
    }
}

fn apply_environment_defaults(
    config: &mut Config,
    ssl_mode_is_set: bool,
    environment: &PgEnvironment,
) -> Result<()> {
    if config.get_hosts().is_empty()
        && let Some(host) = &environment.host
    {
        for host in host.split(',') {
            config.host(host);
        }
    }
    if config.get_ports().is_empty()
        && let Some(ports) = &environment.port
    {
        for port in ports.split(',') {
            let parsed = port
                .parse()
                .map_err(|_| AppError::Connection(format!("invalid PGPORT: {port}")))?;
            config.port(parsed);
        }
    }
    if config.get_user().is_none()
        && let Some(user) = &environment.user
    {
        config.user(user);
    }
    if config.get_password().is_none()
        && let Some(password) = &environment.password
    {
        config.password(password);
    }
    if config.get_dbname().is_none()
        && let Some(database) = &environment.database
    {
        config.dbname(database);
    }
    if !ssl_mode_is_set && let Some(mode) = &environment.ssl_mode {
        config.ssl_mode(parse_ssl_mode(mode)?);
    }
    Ok(())
}

#[derive(Default)]
struct PgEnvironment {
    host: Option<String>,
    port: Option<String>,
    user: Option<String>,
    password: Option<String>,
    database: Option<String>,
    ssl_mode: Option<String>,
}

impl PgEnvironment {
    fn read() -> Self {
        Self {
            host: env::var("PGHOST").ok(),
            port: env::var("PGPORT").ok(),
            user: env::var("PGUSER").ok(),
            password: env::var("PGPASSWORD").ok(),
            database: env::var("PGDATABASE").ok(),
            ssl_mode: env::var("PGSSLMODE").ok(),
        }
    }
}

fn copy_connection_settings(original: &Config) -> Config {
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
    if let Some(options) = original.get_options() {
        config.options(options);
    }
    if let Some(name) = original.get_application_name() {
        config.application_name(name);
    }
    config
        .ssl_mode(original.get_ssl_mode())
        .ssl_negotiation(original.get_ssl_negotiation())
        .keepalives(original.get_keepalives())
        .target_session_attrs(original.get_target_session_attrs())
        .channel_binding(original.get_channel_binding())
        .load_balance_hosts(original.get_load_balance_hosts());
    if let Some(timeout) = original.get_connect_timeout() {
        config.connect_timeout(*timeout);
    }
    if let Some(timeout) = original.get_tcp_user_timeout() {
        config.tcp_user_timeout(*timeout);
    }
    #[cfg(not(target_arch = "wasm32"))]
    {
        config.keepalives_idle(original.get_keepalives_idle());
        if let Some(interval) = original.get_keepalives_interval() {
            config.keepalives_interval(interval);
        }
        if let Some(retries) = original.get_keepalives_retries() {
            config.keepalives_retries(retries);
        }
    }
    config
}

pub(super) fn with_network_overrides(
    original: Config,
    host: Option<&str>,
    port: Option<u16>,
) -> Config {
    let mut config = copy_connection_settings(&original);
    if let Some(host) = host {
        config.host(host);
    } else {
        for item in original.get_hosts() {
            match item {
                tokio_postgres::config::Host::Tcp(host) => {
                    config.host(host);
                }
                #[cfg(unix)]
                tokio_postgres::config::Host::Unix(path) => {
                    config.host_path(path);
                }
            }
        }
        for address in original.get_hostaddrs() {
            config.hostaddr(*address);
        }
    }
    if let Some(port) = port {
        config.port(port);
    } else if host.is_some() {
        if let Some(port) = original.get_ports().first() {
            config.port(*port);
        }
    } else {
        for port in original.get_ports() {
            config.port(*port);
        }
    }
    config
}

pub(super) fn uses_no_tls(config: &Config) -> bool {
    config.get_ssl_mode() == SslMode::Disable
}

pub(super) fn can_use_tls(config: &Config) -> bool {
    !uses_no_tls(config)
        && config
            .get_hosts()
            .iter()
            .any(|host| matches!(host, tokio_postgres::config::Host::Tcp(_)))
}

fn dsn_has_parameter(dsn: &str, wanted: &str) -> bool {
    !dsn_parameter_values(dsn, wanted).is_empty()
}

fn normalize_dsn_ssl_mode(dsn: &str) -> Result<String> {
    // tokio-postgres rejects libpq's verification mode names even though this
    // client intentionally maps both to its hostname-verifying `Require` mode.
    let mut normalized = dsn.to_owned();
    let mut parameters = dsn_parameter_values(dsn, "sslmode");
    parameters.sort_by_key(|(range, _)| std::cmp::Reverse(range.start));
    for (range, value) in parameters {
        let canonical = match parse_ssl_mode(&value)? {
            SslMode::Disable => "disable",
            SslMode::Prefer => "prefer",
            SslMode::Require => "require",
            _ => unreachable!("tokio-postgres added an SSL mode"),
        };
        normalized.replace_range(range, canonical);
    }
    Ok(normalized)
}

fn dsn_parameter_values(dsn: &str, wanted: &str) -> Vec<(std::ops::Range<usize>, String)> {
    if dsn.contains("://") {
        let Some(question) = dsn.find('?') else {
            return Vec::new();
        };
        let query_start = question + 1;
        let query_end = dsn[query_start..]
            .find('#')
            .map_or(dsn.len(), |offset| query_start + offset);
        let mut parameters = Vec::new();
        let mut segment_start = query_start;
        while segment_start <= query_end {
            let segment_end = dsn[segment_start..query_end]
                .find('&')
                .map_or(query_end, |offset| segment_start + offset);
            let segment = &dsn[segment_start..segment_end];
            if let Some(equals) = segment.find('=') {
                let key = &segment[..equals];
                if percent_decode_ascii(key).eq_ignore_ascii_case(wanted) {
                    let value_start = segment_start + equals + 1;
                    parameters.push((
                        value_start..segment_end,
                        percent_decode_ascii(&dsn[value_start..segment_end]),
                    ));
                }
            }
            if segment_end == query_end {
                break;
            }
            segment_start = segment_end + 1;
        }
        return parameters;
    }

    let bytes = dsn.as_bytes();
    let mut parameters = Vec::new();
    let mut index = 0;
    while index < bytes.len() {
        while index < bytes.len() && bytes[index].is_ascii_whitespace() {
            index += 1;
        }
        let key_start = index;
        while index < bytes.len() && !bytes[index].is_ascii_whitespace() && bytes[index] != b'=' {
            index += 1;
        }
        let key = &dsn[key_start..index];
        while index < bytes.len() && bytes[index].is_ascii_whitespace() {
            index += 1;
        }
        if index >= bytes.len() || bytes[index] != b'=' {
            while index < bytes.len() && !bytes[index].is_ascii_whitespace() {
                index += 1;
            }
            continue;
        }
        index += 1;
        while index < bytes.len() && bytes[index].is_ascii_whitespace() {
            index += 1;
        }
        let value_start = index;
        let mut value = Vec::new();
        if index < bytes.len() && bytes[index] == b'\'' {
            index += 1;
            let mut closed = false;
            while index < bytes.len() {
                match bytes[index] {
                    b'\\' if index + 1 < bytes.len() => {
                        value.push(bytes[index + 1]);
                        index += 2;
                    }
                    b'\'' => {
                        index += 1;
                        closed = true;
                        break;
                    }
                    byte => {
                        value.push(byte);
                        index += 1;
                    }
                }
            }
            if !closed {
                continue;
            }
        } else {
            while index < bytes.len() && !bytes[index].is_ascii_whitespace() {
                if bytes[index] == b'\\' && index + 1 < bytes.len() {
                    value.push(bytes[index + 1]);
                    index += 2;
                } else {
                    value.push(bytes[index]);
                    index += 1;
                }
            }
        }
        if key.eq_ignore_ascii_case(wanted) {
            parameters.push((
                value_start..index,
                String::from_utf8_lossy(&value).into_owned(),
            ));
        }
    }
    parameters
}

fn percent_decode_ascii(value: &str) -> String {
    let bytes = value.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%'
            && index + 2 < bytes.len()
            && let (Some(high), Some(low)) =
                (hex_value(bytes[index + 1]), hex_value(bytes[index + 2]))
        {
            decoded.push(high * 16 + low);
            index += 3;
        } else {
            decoded.push(bytes[index]);
            index += 1;
        }
    }
    String::from_utf8_lossy(&decoded).into_owned()
}

fn hex_value(value: u8) -> Option<u8> {
    match value {
        b'0'..=b'9' => Some(value - b'0'),
        b'a'..=b'f' => Some(value - b'a' + 10),
        b'A'..=b'F' => Some(value - b'A' + 10),
        _ => None,
    }
}

fn parse_ssl_mode(value: &str) -> Result<SslMode> {
    match value {
        "disable" => Ok(SslMode::Disable),
        "prefer" => Ok(SslMode::Prefer),
        "require" | "verify-ca" | "verify-full" => Ok(SslMode::Require),
        _ => Err(AppError::Connection(format!(
            "unsupported sslmode: {value}"
        ))),
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;
    use std::str::FromStr;

    #[test]
    fn classifies_ambiguous_connection_arguments_as_database_names() {
        for database in ["release=2026", "archive://2026"] {
            let cli = Cli::try_parse_from(["pgline", database]).unwrap();
            let config = base_config(&cli).unwrap();
            assert_eq!(config.get_dbname(), Some(database));
        }

        for dsn in ["host=db.example dbname=app", "postgresql://db.example/app"] {
            assert_ne!(
                classify_connection_input(dsn),
                ConnectionInput::DatabaseName
            );
        }
    }

    #[test]
    fn applies_a_default_connection_timeout_without_overriding_an_explicit_one() {
        let cli = Cli::try_parse_from(["pgline", "app"]).unwrap();
        assert_eq!(
            base_config(&cli).unwrap().get_connect_timeout(),
            Some(&DEFAULT_CONNECT_TIMEOUT)
        );

        let cli = Cli::try_parse_from(["pgline", "host=db.example connect_timeout=37"]).unwrap();
        assert_eq!(
            base_config(&cli).unwrap().get_connect_timeout(),
            Some(&Duration::from_secs(37))
        );
    }

    #[test]
    fn parses_supported_ssl_modes() {
        assert_eq!(parse_ssl_mode("disable").unwrap(), SslMode::Disable);
        assert_eq!(parse_ssl_mode("verify-full").unwrap(), SslMode::Require);
        assert!(parse_ssl_mode("allow").is_err());
        assert!(parse_ssl_mode("surprise").is_err());
    }

    #[test]
    fn normalizes_required_ssl_modes_in_connection_strings() {
        let uri = normalize_dsn_ssl_mode(
            "postgresql://host/db?application_name=test&sslmode=verify-full",
        )
        .unwrap();
        assert_eq!(
            Config::from_str(&uri).unwrap().get_ssl_mode(),
            SslMode::Require
        );
        assert!(uri.contains("application_name=test"));

        let keyword = normalize_dsn_ssl_mode("host=host dbname=db sslmode = 'verify-ca'").unwrap();
        assert_eq!(
            Config::from_str(&keyword).unwrap().get_ssl_mode(),
            SslMode::Require
        );
    }

    #[test]
    fn rejects_allow_in_connection_strings() {
        assert!(normalize_dsn_ssl_mode("postgresql://host/db?sslmode=allow").is_err());
        assert!(normalize_dsn_ssl_mode("host=host sslmode=allow").is_err());
    }

    #[test]
    fn supplies_a_default_local_host_without_tls() {
        let mut config = Config::new();
        apply_host_defaults(&mut config);
        assert!(!config.get_hosts().is_empty());
        #[cfg(unix)]
        assert_eq!(config.get_ssl_mode(), SslMode::Disable);
    }

    #[test]
    fn preserves_an_explicit_host() {
        let mut config = Config::new();
        config.host("db.example");
        apply_host_defaults(&mut config);
        assert_eq!(config.get_hosts().len(), 1);
    }

    #[test]
    fn dsn_values_win_while_missing_values_use_environment() {
        let mut config: Config = "host=dsn.example user=dsn_user".parse().unwrap();
        let environment = PgEnvironment {
            host: Some("env.example".into()),
            port: Some("5433".into()),
            user: Some("env_user".into()),
            password: Some("secret".into()),
            database: Some("env_database".into()),
            ssl_mode: Some("disable".into()),
        };
        apply_environment_defaults(&mut config, false, &environment).unwrap();
        assert!(
            matches!(&config.get_hosts()[0], tokio_postgres::config::Host::Tcp(host) if host == "dsn.example")
        );
        assert_eq!(config.get_user(), Some("dsn_user"));
        assert_eq!(config.get_ports(), &[5433]);
        assert_eq!(config.get_password(), Some("secret".as_bytes()));
        assert_eq!(config.get_dbname(), Some("env_database"));
        assert_eq!(config.get_ssl_mode(), SslMode::Disable);
    }

    #[test]
    fn explicit_network_options_replace_dsn_values() {
        let config: Config = "host=one,two port=1111,2222 user=ada".parse().unwrap();
        let config = with_network_overrides(config, Some("override"), Some(5439));
        assert_eq!(config.get_hosts().len(), 1);
        assert!(
            matches!(&config.get_hosts()[0], tokio_postgres::config::Host::Tcp(host) if host == "override")
        );
        assert_eq!(config.get_ports(), &[5439]);
        assert_eq!(config.get_user(), Some("ada"));

        let config: Config = "host=one,two port=1111,2222".parse().unwrap();
        let config = with_network_overrides(config, Some("override"), None);
        assert_eq!(config.get_hosts().len(), 1);
        assert_eq!(config.get_ports(), &[1111]);
    }

    #[test]
    fn detects_explicit_sslmode_parameters_without_matching_values() {
        assert!(dsn_has_parameter(
            "postgresql://localhost/db?%73slmode=disable",
            "sslmode"
        ));
        assert!(!dsn_has_parameter(
            "postgresql://localhost/db?application_name=sslmode%3Ddisable",
            "sslmode"
        ));
        assert!(dsn_has_parameter(
            "host=localhost sslmode='require' application_name=x",
            "sslmode"
        ));
        assert!(!dsn_has_parameter(
            "host=localhost application_name='contains sslmode=disable'",
            "sslmode"
        ));
    }

    #[test]
    fn explicit_sslmode_presence_controls_environment_precedence() {
        let environment = PgEnvironment {
            ssl_mode: Some("disable".into()),
            ..PgEnvironment::default()
        };

        let dsn = "postgresql://localhost/db?%73slmode=prefer";
        let mut config: Config = dsn.parse().unwrap();
        apply_environment_defaults(&mut config, dsn_has_parameter(dsn, "sslmode"), &environment)
            .unwrap();
        assert_eq!(config.get_ssl_mode(), SslMode::Prefer);

        let dsn = "host=localhost application_name='sslmode=prefer'";
        let mut config: Config = dsn.parse().unwrap();
        apply_environment_defaults(&mut config, dsn_has_parameter(dsn, "sslmode"), &environment)
            .unwrap();
        assert_eq!(config.get_ssl_mode(), SslMode::Disable);
    }

    #[cfg(unix)]
    #[test]
    fn environment_and_cli_hosts_preserve_unix_sockets() {
        let mut config = Config::new();
        let environment = PgEnvironment {
            host: Some("/tmp,db.example".into()),
            ..PgEnvironment::default()
        };
        apply_environment_defaults(&mut config, false, &environment).unwrap();
        assert!(matches!(
            &config.get_hosts()[0],
            tokio_postgres::config::Host::Unix(path) if path == std::path::Path::new("/tmp")
        ));
        assert!(matches!(
            &config.get_hosts()[1],
            tokio_postgres::config::Host::Tcp(host) if host == "db.example"
        ));

        let config = with_network_overrides(config, Some("/var/run/postgresql"), None);
        assert!(matches!(
            &config.get_hosts()[0],
            tokio_postgres::config::Host::Unix(path) if path == std::path::Path::new("/var/run/postgresql")
        ));
    }

    #[test]
    fn hostname_inherits_independently_of_hostaddr() {
        let mut config: Config = "hostaddr=127.0.0.1".parse().unwrap();
        let environment = PgEnvironment {
            host: Some("db.example".into()),
            ..PgEnvironment::default()
        };
        apply_environment_defaults(&mut config, false, &environment).unwrap();
        assert_eq!(config.get_hosts().len(), 1);
        assert_eq!(config.get_hostaddrs().len(), 1);
    }

    #[cfg(unix)]
    #[test]
    fn preserves_non_utf8_socket_paths_when_splitting_targets() {
        use std::{ffi::OsString, os::unix::ffi::OsStringExt, path::PathBuf};

        let path = PathBuf::from(OsString::from_vec(b"/tmp/pgline-\xff".to_vec()));
        let mut config = Config::new();
        config.host_path(&path);
        config.host("db.example");
        let attempt = config_for_target(&config, 0);
        assert!(matches!(
            &attempt.get_hosts()[0],
            tokio_postgres::config::Host::Unix(actual) if actual == &path
        ));
    }

    #[cfg(unix)]
    #[test]
    fn mixed_hosts_get_connector_specific_attempts() {
        let mut config = Config::new();
        config.host("/tmp");
        config.host("db.example");
        config.ssl_mode(SslMode::Require);
        assert!(has_mixed_host_types(&config));

        let socket = config_for_target(&config, 0);
        assert!(uses_no_tls(&socket));
        let tcp = config_for_target(&config, 1);
        assert!(!uses_no_tls(&tcp));
        assert_eq!(tcp.get_ssl_mode(), SslMode::Require);
    }

    #[test]
    fn hostaddr_only_tls_uses_the_ip_as_verification_host() {
        for address in ["127.0.0.1", "::1"] {
            let mut config = Config::new();
            config.hostaddr(address.parse().unwrap());
            config.ssl_mode(SslMode::Require);
            supply_tls_hosts_for_hostaddrs(&mut config);
            assert_eq!(config.get_hosts().len(), 1);
            assert!(matches!(
                &config.get_hosts()[0],
                tokio_postgres::config::Host::Tcp(host) if host == address
            ));
        }

        let mut config = Config::new();
        config.hostaddr("127.0.0.1".parse().unwrap());
        config.ssl_mode(SslMode::Disable);
        supply_tls_hosts_for_hostaddrs(&mut config);
        assert!(config.get_hosts().is_empty());
        assert!(uses_no_tls(&config));
    }

    #[test]
    fn splits_hostaddr_only_targets_into_individual_attempts() {
        let mut config = Config::new();
        config.hostaddr("127.0.0.1".parse().unwrap());
        config.hostaddr("127.0.0.2".parse().unwrap());
        config.port(5432);
        config.port(5433);
        config.ssl_mode(SslMode::Disable);

        assert_eq!(connection_target_count(&config), 2);
        let second = config_for_target(&config, 1);
        assert_eq!(second.get_hosts().len(), 1);
        assert_eq!(
            second.get_hostaddrs(),
            &["127.0.0.2".parse::<std::net::IpAddr>().unwrap()]
        );
        assert_eq!(second.get_ports(), &[5433]);
    }

    #[test]
    fn rejects_mismatched_network_target_lists() {
        let mut config = Config::new();
        config.host("one");
        config.host("two");
        config.hostaddr("127.0.0.1".parse().unwrap());
        assert!(validate_network_config(&config).is_err());

        let mut config = Config::new();
        config.host("one");
        config.host("two");
        config.port(1111);
        config.port(2222);
        config.port(3333);
        assert!(validate_network_config(&config).is_err());
    }
    #[test]
    fn plaintext_targets_do_not_require_a_tls_connector() {
        let mut config = Config::new();
        config.host("db.example");
        config.ssl_mode(SslMode::Disable);
        assert!(!can_use_tls(&config));

        config.ssl_mode(SslMode::Require);
        assert!(can_use_tls(&config));

        #[cfg(unix)]
        {
            let mut socket = Config::new();
            socket.host_path("/tmp");
            socket.ssl_mode(SslMode::Require);
            apply_host_defaults(&mut socket);
            assert!(!can_use_tls(&socket));
        }
    }

    #[test]
    fn unix_sockets_always_disable_tls() {
        let mut config = Config::new();
        #[cfg(unix)]
        config.host_path("/tmp");
        #[cfg(not(unix))]
        config.host("localhost");
        config.ssl_mode(SslMode::Require);
        apply_host_defaults(&mut config);
        #[cfg(unix)]
        assert_eq!(config.get_ssl_mode(), SslMode::Disable);
    }
}
