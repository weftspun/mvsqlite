# mvsqlite_pgtypes

A standard SQLite loadable extension bundling SQLite's own "standard"
`ext/misc` extensions, closing the biggest gaps between SQLite's type
affinity system and PostgreSQL/CockroachDB's typed columns.

(`v-sekai/cockroach` was checked separately and found to be a CI/build-only
fork of upstream CockroachDB with no behavioral changes — there's no
fork-specific type system to match, so this targets CockroachDB's own types,
which are the standard PostgreSQL set.)

## What's included

| Source | Functions | Matches |
|---|---|---|
| `uuid.c` (SQLite `ext/misc`, public domain) | `uuid()`, `uuid_str(x)`, `uuid_blob(x)` | PG/CRDB `UUID` type |
| `decimal.c` (SQLite `ext/misc`, public domain) | `decimal(x)`, `decimal_add/sub/mul/cmp(...)`, `decimal_sum()` aggregate | PG/CRDB `DECIMAL`/`NUMERIC` — arbitrary precision, no float rounding error |
| `series.c` (SQLite `ext/misc`, public domain) | `generate_series(start, stop, step)` | PG `generate_series()` |
| `ipaddr/extension.c` ([sqlean](https://github.com/nalgeon/sqlean), MIT, © 2021 Vincent Bernat) | `iphost(x)`, `ipmasklen(x)`, `ipnetwork(x)`, `ipcontains(net, x)`, `ipfamily(x)` | PG/CRDB `INET`/`CIDR` type |

`uuid.c`/`decimal.c`/`series.c` are vendored verbatim from
https://sqlite.org/src/dir/ext/misc — the reference implementation most
projects already reach for. `ipaddr/extension.c` is vendored from sqlean,
the maintained, well-known extension for IP address handling in SQLite
(`sqlite3-ipaddr.c`, sqlean's own thin wrapper, isn't vendored — it only adds
an unrelated `sqlean_version()` function; `ipaddr_init()` is called directly
instead).

The only modification from upstream: `uuid.c`/`decimal.c`/`series.c`'s
`SQLITE_EXTENSION_INIT1` (which *defines* the `sqlite3_api` pointer) was
changed to `SQLITE_EXTENSION_INIT3` (which `extern`-declares it instead),
since linking multiple files that each define the same global into one
shared library is a duplicate-symbol error. `pgtypes_ext.c` owns the one
real definition and calls each file's `..._init` in turn. This is SQLite's
own documented pattern for combining multiple `ext/misc` extensions into a
single loadable library. (`ipaddr/extension.c` already used `INIT3` upstream
— no change needed there.)

## Build

```bash
make -C mvsqlite-pgtypes-ext build
```

Produces `libmvsqlite_pgtypes.so` (or `.dylib` on macOS).

## Usage

```sql
.load ./libmvsqlite_pgtypes

SELECT uuid();
-- '3d3ea0e9-1f4a-447d-a8bb-fb18f0684462'

SELECT decimal_add('1.10', '2.25');
-- '3.35'  (exact, unlike float arithmetic)

SELECT value FROM generate_series(1, 5);
-- 1, 2, 3, 4, 5

SELECT ipcontains('10.0.0.0/8', '10.1.2.3');
-- 1
```
