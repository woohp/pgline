use std::{
    env,
    fs::File,
    io::{self, Read},
    path::{Path, PathBuf},
};

use tokio_postgres::{Config, config::Host};

use crate::output;

const MAX_PASSWORD_FILE_BYTES: u64 = 1024 * 1024;

#[derive(Debug)]
struct Entry {
    host: Vec<u8>,
    port: Vec<u8>,
    database: Vec<u8>,
    user: Vec<u8>,
    password: Vec<u8>,
}

pub(super) struct PasswordFile {
    entries: Vec<Entry>,
}

impl PasswordFile {
    pub(super) fn empty() -> Self {
        Self {
            entries: Vec::new(),
        }
    }

    pub(super) fn load() -> Self {
        let Some(path) = password_file_path() else {
            return Self::empty();
        };
        let entries = match read_password_file(&path) {
            Ok(Some(entries)) => entries,
            Ok(None) => Vec::new(),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Vec::new(),
            Err(error) => {
                eprintln!(
                    "warning: could not read password file {}: {}",
                    output::safe_terminal_text(&path.display().to_string()),
                    output::safe_terminal_text(&error.to_string())
                );
                Vec::new()
            }
        };
        Self { entries }
    }

    #[cfg(test)]
    pub(super) fn matching_all(password: &[u8]) -> Self {
        Self {
            entries: vec![Entry {
                host: b"*".to_vec(),
                port: b"*".to_vec(),
                database: b"*".to_vec(),
                user: b"*".to_vec(),
                password: password.to_vec(),
            }],
        }
    }

    pub(super) fn apply(&self, config: &mut Config) {
        if config.get_password().is_some() {
            return;
        }
        let parameters = ConnectionParameters::from_config(config);
        if let Some(entry) = self.entries.iter().find(|entry| entry.matches(&parameters)) {
            config.password(&entry.password);
        }
    }
}

fn password_file_path() -> Option<PathBuf> {
    if let Some(path) = env::var_os("PGPASSFILE") {
        return Some(path.into());
    }
    #[cfg(windows)]
    {
        env::var_os("APPDATA")
            .map(PathBuf::from)
            .map(|path| path.join("postgresql").join("pgpass.conf"))
    }
    #[cfg(not(windows))]
    {
        env::var_os("HOME")
            .map(PathBuf::from)
            .map(|path| path.join(".pgpass"))
    }
}

fn read_password_file(path: &Path) -> io::Result<Option<Vec<Entry>>> {
    let mut file = open_password_file(path)?;
    if !password_file_is_secure(&file)? {
        eprintln!(
            "warning: password file {} has group or world access; ignoring it",
            output::safe_terminal_text(&path.display().to_string())
        );
        return Ok(None);
    }
    if file.metadata()?.len() > MAX_PASSWORD_FILE_BYTES {
        return Err(password_file_too_large());
    }
    let mut contents = Vec::new();
    file.by_ref()
        .take(MAX_PASSWORD_FILE_BYTES + 1)
        .read_to_end(&mut contents)?;
    if contents.len() as u64 > MAX_PASSWORD_FILE_BYTES {
        return Err(password_file_too_large());
    }
    Ok(Some(parse_entries(&contents)))
}

fn password_file_too_large() -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        format!("password file exceeds {MAX_PASSWORD_FILE_BYTES} bytes"),
    )
}

#[cfg(unix)]
fn open_password_file(path: &Path) -> io::Result<File> {
    use std::os::unix::fs::OpenOptionsExt;

    std::fs::OpenOptions::new()
        .read(true)
        // Avoid blocking startup if PGPASSFILE names a FIFO or device. The
        // descriptor is validated as a regular file before it is read.
        .custom_flags(libc::O_NONBLOCK)
        .open(path)
}

#[cfg(not(unix))]
fn open_password_file(path: &Path) -> io::Result<File> {
    File::open(path)
}

#[cfg(unix)]
fn password_file_is_secure(file: &File) -> io::Result<bool> {
    use std::os::unix::fs::MetadataExt;

    let metadata = file.metadata()?;
    Ok(metadata.is_file() && metadata.mode() & 0o077 == 0)
}

#[cfg(not(unix))]
fn password_file_is_secure(file: &File) -> io::Result<bool> {
    Ok(file.metadata()?.is_file())
}

fn parse_entries(contents: &[u8]) -> Vec<Entry> {
    contents
        .split(|byte| *byte == b'\n')
        .filter_map(parse_entry)
        .collect()
}

fn parse_entry(line: &[u8]) -> Option<Entry> {
    let line = line.strip_suffix(b"\r").unwrap_or(line);
    if line.is_empty() || line.first() == Some(&b'#') {
        return None;
    }
    let fields = split_fields(line)?;
    Some(Entry {
        host: fields[0].clone(),
        port: fields[1].clone(),
        database: fields[2].clone(),
        user: fields[3].clone(),
        password: fields[4].clone(),
    })
}

fn split_fields(line: &[u8]) -> Option<[Vec<u8>; 5]> {
    let mut fields = Vec::new();
    let mut field = Vec::new();
    let mut escaped = false;
    for &byte in line {
        if escaped {
            field.push(byte);
            escaped = false;
        } else if byte == b'\\' {
            escaped = true;
        } else if byte == b':' {
            fields.push(std::mem::take(&mut field));
        } else {
            field.push(byte);
        }
    }
    if escaped {
        field.push(b'\\');
    }
    fields.push(field);
    fields.try_into().ok()
}

struct ConnectionParameters {
    host: Vec<u8>,
    port: Vec<u8>,
    database: Vec<u8>,
    user: Vec<u8>,
}

impl ConnectionParameters {
    fn from_config(config: &Config) -> Self {
        let host = match config.get_hosts().first() {
            Some(Host::Tcp(host)) => host.as_bytes().to_vec(),
            #[cfg(unix)]
            Some(Host::Unix(path)) if is_default_socket_directory(path) => b"localhost".to_vec(),
            #[cfg(unix)]
            Some(Host::Unix(path)) => path.as_os_str().as_encoded_bytes().to_vec(),
            None => config.get_hostaddrs().first().map_or_else(
                || b"localhost".to_vec(),
                |address| address.to_string().into_bytes(),
            ),
        };
        let user = config
            .get_user()
            .map(str::to_owned)
            .or_else(|| whoami::username().ok())
            .unwrap_or_default();
        let database = config
            .get_dbname()
            .map(str::to_owned)
            .unwrap_or_else(|| user.clone());
        Self {
            host,
            port: config
                .get_ports()
                .first()
                .copied()
                .unwrap_or(5432)
                .to_string()
                .into_bytes(),
            database: database.into_bytes(),
            user: user.into_bytes(),
        }
    }
}

#[cfg(unix)]
fn is_default_socket_directory(path: &Path) -> bool {
    path == Path::new("/var/run/postgresql") || path == Path::new("/tmp")
}

impl Entry {
    fn matches(&self, parameters: &ConnectionParameters) -> bool {
        field_matches(&self.host, &parameters.host)
            && field_matches(&self.port, &parameters.port)
            && field_matches(&self.database, &parameters.database)
            && field_matches(&self.user, &parameters.user)
    }
}

fn field_matches(pattern: &[u8], value: &[u8]) -> bool {
    pattern == b"*" || pattern == value
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_escapes_comments_and_first_match_order() {
        let entries = parse_entries(
            b"# comment\nlocalhost:5432:app:user:first\\:password\n*:*:*:*:fallback\n",
        );
        let parameters = ConnectionParameters {
            host: b"localhost".to_vec(),
            port: b"5432".to_vec(),
            database: b"app".to_vec(),
            user: b"user".to_vec(),
        };
        let matching = entries
            .iter()
            .find(|entry| entry.matches(&parameters))
            .unwrap();
        assert_eq!(matching.password, b"first:password");
    }

    #[test]
    fn rejects_malformed_lines() {
        assert!(parse_entry(b"too:few:fields").is_none());
        assert!(parse_entry(b"too:many:fields:are:not:accepted").is_none());
    }

    #[test]
    fn applies_the_first_matching_password_to_a_target() {
        let password_file = PasswordFile {
            entries: parse_entries(
                b"other:*:*:*:wrong\ndb.example:5544:app:alice:secret\n*:*:*:*:fallback\n",
            ),
        };
        let mut config = Config::new();
        config.host("db.example");
        config.port(5544);
        config.dbname("app");
        config.user("alice");

        password_file.apply(&mut config);

        assert_eq!(config.get_password(), Some(b"secret".as_slice()));
    }

    #[test]
    fn derives_hostaddr_and_default_database_matching_parameters() {
        let mut config = Config::new();
        config.hostaddr("192.0.2.10".parse().unwrap());
        config.user("alice");

        let parameters = ConnectionParameters::from_config(&config);

        assert_eq!(parameters.host, b"192.0.2.10");
        assert_eq!(parameters.port, b"5432");
        assert_eq!(parameters.database, b"alice");
        assert_eq!(parameters.user, b"alice");
    }

    #[cfg(unix)]
    #[test]
    fn derives_socket_matching_parameters() {
        for path in ["/var/run/postgresql", "/tmp"] {
            let mut config = Config::new();
            config.host_path(path);
            assert_eq!(
                ConnectionParameters::from_config(&config).host,
                b"localhost"
            );
        }

        let mut config = Config::new();
        config.host_path("/custom/postgresql");
        assert_eq!(
            ConnectionParameters::from_config(&config).host,
            b"/custom/postgresql"
        );
    }

    #[test]
    fn selects_different_passwords_for_connection_targets() {
        let password_file = PasswordFile {
            entries: parse_entries(
                b"first.example:*:*:*:first-password\nsecond.example:*:*:*:second-password\n",
            ),
        };
        let mut first = Config::new();
        first.host("first.example");
        let mut second = Config::new();
        second.host("second.example");

        password_file.apply(&mut first);
        password_file.apply(&mut second);

        assert_eq!(first.get_password(), Some(b"first-password".as_slice()));
        assert_eq!(second.get_password(), Some(b"second-password".as_slice()));
    }

    #[test]
    fn explicit_password_takes_precedence() {
        let password_file = PasswordFile {
            entries: parse_entries(b"*:*:*:*:from-file\n"),
        };
        let mut config = Config::new();
        config.password("explicit");

        password_file.apply(&mut config);

        assert_eq!(config.get_password(), Some(b"explicit".as_slice()));
    }

    #[cfg(unix)]
    #[test]
    fn accepts_password_files_private_to_the_owner() {
        use std::os::unix::fs::PermissionsExt;

        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("pgpass");
        std::fs::write(&path, b"*:*:*:*:secret\n").unwrap();
        for mode in [0o400, 0o600] {
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(mode)).unwrap();
            assert_eq!(read_password_file(&path).unwrap().unwrap().len(), 1);
        }
    }

    #[cfg(unix)]
    #[test]
    fn rejects_oversized_password_files() {
        use std::os::unix::fs::PermissionsExt;

        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("pgpass");
        let file = File::create(&path).unwrap();
        file.set_len(MAX_PASSWORD_FILE_BYTES + 1).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();

        assert_eq!(
            read_password_file(&path).unwrap_err().kind(),
            io::ErrorKind::InvalidData
        );
    }

    #[cfg(unix)]
    #[test]
    fn rejects_password_files_accessible_to_other_users() {
        use std::os::unix::fs::PermissionsExt;

        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("pgpass");
        std::fs::write(&path, b"*:*:*:*:secret\n").unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();
        assert!(read_password_file(&path).unwrap().is_none());
    }
}
