# mvSQLite

mvSQLite is a distributed, MVCC SQLite running on [FoundationDB](https://github.com/apple/foundationdb), modified by V-Sekai (https://github.com/V-Sekai). It offers full feature-set from SQLite, time travel, lock-free scalable reads and writes, and more. You can use `LD_PRELOAD` or a patched `libsqlite3.so` to integrate mvSQLite into your existing apps.

## Quick Links

- [Documentation](https://github.com/losfair/mvsqlite/wiki/)
- [Releases](https://github.com/losfair/mvsqlite/releases)
- [Quick Reference](https://github.com/V-Sekai/mvsqlite/wiki/Quick-Reference)

## Getting Started

1. Install FoundationDB:

```bash
wget https://github.com/apple/foundationdb/releases/download/7.1.15/foundationdb-clients_7.1.15-1_amd64.deb
sudo dpkg -i foundationdb-clients_7.1.15-1_amd64.deb
wget https://github.com/apple/foundationdb/releases/download/7.1.15/foundationdb-server_7.1.15-1_amd64.deb
sudo dpkg -i foundationdb-server_7.1.15-1_amd64.deb
```

2. mvSQLite's SQLite VFS talks to FoundationDB directly - there's no server process to run. Set the environment variables that tell it where to find the cluster and which namespace to use, then build `libsqlite3` and the `sqlite3` CLI. Detailed instructions can be found in the [wiki](https://github.com/V-Sekai/mvsqlite/wiki).

```bash
export MVSQLITE_FDB_CLUSTER=/etc/foundationdb/fdb.cluster
export MVSQLITE_METADATA_PREFIX=mvsqlite
export MVSQLITE_RAW_DATA_PREFIX=m
export MVSQLITE_AUTO_CREATE_NAMESPACE=1
```

3. To run `sqlite3`. Build `libsqlite3` and the `sqlite3` CLI: (note that a custom build is only needed here because the `sqlite3` binary shipped on most systems are statically linked to `libsqlite3` and `LD_PRELOAD` don't work)

```bash
cargo build --release -p mvsqlite
cd mvsqlite-sqlite3
make build-patched-sqlite3
./sqlite3
```

## Contributing

mvSQLite can be built with the standard Rust toolchain. More details are available in the [wiki](https://github.com/V-Sekai/mvsqlite/wiki).
