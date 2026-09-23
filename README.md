# pgline

A PostgreSQL REPL

## Install

### Arch Linux

Install the latest release as a pacman-managed package:

```sh
curl -fL https://github.com/woohp/pgline/releases/latest/download/pgline-x86_64.pkg.tar.zst -o /tmp/pgline.pkg.tar.zst && sudo pacman -U /tmp/pgline.pkg.tar.zst
```

Alternatively, install the prebuilt Linux binary directly:

```sh
curl -fsSL https://github.com/woohp/pgline/releases/latest/download/pgline-x86_64-unknown-linux-gnu.tar.gz | sudo tar -xz -C /usr/local/bin pgline
```

Ansible equivalent:

```yaml
- name: Install pgline
  become: true
  ansible.builtin.unarchive:
    src: https://github.com/woohp/pgline/releases/latest/download/pgline-x86_64-unknown-linux-gnu.tar.gz
    dest: /usr/local/bin
    remote_src: true
    include:
      - pgline
    mode: "0755"
```

### From source

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

Output to a terminal that is longer than the screen goes through
`$PGLINE_PAGER`, then `$PAGER`, then `less`; shorter output is printed as is.
Output to a pipe or file is never paged. The pager opens while rows are still
arriving, and quitting it cancels the query. A CSV-aware pager works well with
`--format csv`:

```sh
PGLINE_PAGER=csvlens pgline my_database --format csv -c 'select * from events'
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
