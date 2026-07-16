# pgline

A PostgreSQL REPL

## Install

```sh
cargo build --release
```

## Use

```sh
pgline my_database
pgline 'postgresql://user@localhost/database'
pgline -h localhost -U user -d database
```

Run a query and exit:

```sh
pgline my_database -c 'select 1'
pgline my_database -f query.sql
```

## Commands

```text
\?             help
\q             quit
\e             edit query
\c DATABASE    change database
\refresh       refresh completions
\d             relations
\dt            tables
\dv            views
\df            functions
\dn            schemas
\l             databases
\du            roles
\x             expanded output
\timing        timing
\pager         pager
```

## Development

```sh
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test
```
