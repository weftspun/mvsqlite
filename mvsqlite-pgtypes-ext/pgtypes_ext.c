/*
 * mvsqlite_pgtypes: a standard SQLite loadable extension bundling SQLite's own
 * "standard" ext/misc extensions that close the biggest gaps between SQLite's
 * type affinity system and PostgreSQL/CockroachDB's typed columns:
 *
 *   - uuid.c    -> uuid(), uuid_str(x), uuid_blob(x)      (PG/CRDB UUID type)
 *   - decimal.c -> decimal(x), decimal_add/sub/mul/cmp,   (PG/CRDB DECIMAL/NUMERIC)
 *                  decimal_sum() aggregate, arbitrary precision, no float error
 *   - series.c  -> generate_series(start, stop, step)     (PG generate_series())
 *
 * These are vendored verbatim from https://sqlite.org/src/dir/ext/misc (public
 * domain) rather than reimplemented - they're the reference implementation
 * every project reaching for Postgres-like SQLite types already uses.
 *
 * v-sekai/cockroach (checked separately) is a CI/build-only fork of upstream
 * CockroachDB with no behavioral changes, so there is no fork-specific type
 * system to match - CockroachDB's own types are the standard PostgreSQL set,
 * which is what this extension targets.
 *
 * Load with: SELECT load_extension('libmvsqlite_pgtypes');  -- or `.load` in the CLI
 */

#include "sqlite3ext.h"
SQLITE_EXTENSION_INIT1

int sqlite3_uuid_init(sqlite3 *db, char **pzErrMsg, const sqlite3_api_routines *pApi);
int sqlite3_decimal_init(sqlite3 *db, char **pzErrMsg, const sqlite3_api_routines *pApi);
int sqlite3_series_init(sqlite3 *db, char **pzErrMsg, const sqlite3_api_routines *pApi);

#ifdef _WIN32
__declspec(dllexport)
#endif
int sqlite3_mvsqlitepgtypes_init(
    sqlite3 *db,
    char **pzErrMsg,
    const sqlite3_api_routines *pApi
) {
    SQLITE_EXTENSION_INIT2(pApi);

    int rc = sqlite3_uuid_init(db, pzErrMsg, pApi);
    if (rc != SQLITE_OK) {
        return rc;
    }

    rc = sqlite3_decimal_init(db, pzErrMsg, pApi);
    if (rc != SQLITE_OK) {
        return rc;
    }

    return sqlite3_series_init(db, pzErrMsg, pApi);
}
