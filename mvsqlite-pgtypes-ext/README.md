# mvsqlite_pgtypes

A standard SQLite loadable extension bundling SQLite's own "standard"
`ext/misc` extensions, closing the biggest gaps between SQLite's type
affinity system and PostgreSQL/CockroachDB's typed columns.

(`v-sekai/cockroach` was checked separately and found to be a CI/build-only
fork of upstream CockroachDB with no behavioral changes — there's no
fork-specific type system to match, so this targets CockroachDB's own types,
which are the standard PostgreSQL set.)

## What's included

Vendored verbatim (public domain) from https://sqlite.org/src/dir/ext/misc,
the reference implementation most projects already reach for:

| File | Functions | Matches |
|---|---|---|
| `uuid.c` | `uuid()`, `uuid_str(x)`, `uuid_blob(x)` | PG/CRDB `UUID` type |
| `decimal.c` | `decimal(x)`, `decimal_add/sub/mul/cmp(...)`, `decimal_sum()` aggregate | PG/CRDB `DECIMAL`/`NUMERIC` — arbitrary precision, no float rounding error |
| `series.c` | `generate_series(start, stop, step)` | PG `generate_series()` |

The only modification from upstream: each file's `SQLITE_EXTENSION_INIT1`
(which *defines* the `sqlite3_api` pointer) was changed to `SQLITE_EXTENSION_INIT3`
(which `extern`-declares it instead), since linking multiple files that each
define the same global into one shared library is a duplicate-symbol error.
`pgtypes_ext.c` owns the one real definition and calls each file's `..._init`
in turn. This is SQLite's own documented pattern for combining multiple
`ext/misc` extensions into a single loadable library.

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
```
