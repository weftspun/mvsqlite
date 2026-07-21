/*
 * mvsqlite_shard: a standard SQLite loadable extension exposing a single,
 * canonical shard-routing function.
 *
 * Namespace-scoped serialization primitives in mvstore (last-write-version,
 * the namespace lock, the commit-token for large transactions) mean a single
 * mvSQLite namespace has a throughput ceiling no amount of tuning inside it
 * can remove. Linear scalability with concurrent writer count comes from
 * splitting writers across multiple independent namespaces instead, each
 * running on disjoint FDB key ranges.
 *
 * That only helps if every caller picks the same namespace for the same key
 * every time. This extension gives every client language a single,
 * dependency-free, load-once implementation of that routing decision, so it
 * isn't reimplemented (and potentially inconsistently) per app.
 *
 * Load with: SELECT load_extension('libmvsqlite_shard');  -- or `.load` in the CLI
 */

#include "sqlite3ext.h"
SQLITE_EXTENSION_INIT1

#include <stdint.h>
#include <stdio.h>

static uint64_t fnv1a64(const unsigned char *data, int len) {
    uint64_t hash = 14695981039346656037ULL;
    for (int i = 0; i < len; i++) {
        hash ^= (uint64_t)data[i];
        hash *= 1099511628211ULL;
    }
    return hash;
}

/* mvsqlite_shard_index(key, num_shards) -> INTEGER in [0, num_shards) */
static void mvsqlite_shard_index_func(
    sqlite3_context *ctx,
    int argc,
    sqlite3_value **argv
) {
    (void)argc;
    sqlite3_int64 num_shards = sqlite3_value_int64(argv[1]);
    if (num_shards <= 0) {
        sqlite3_result_error(ctx, "num_shards must be positive", -1);
        return;
    }

    /* Hash the key's text representation, so the same logical key routes to
       the same shard whether it was passed as an INTEGER or as TEXT. */
    const unsigned char *key_bytes = sqlite3_value_text(argv[0]);
    if (key_bytes == NULL) {
        sqlite3_result_error(ctx, "key must not be NULL", -1);
        return;
    }
    int key_len = sqlite3_value_bytes(argv[0]);

    uint64_t hash = fnv1a64(key_bytes, key_len);
    sqlite3_result_int64(ctx, (sqlite3_int64)(hash % (uint64_t)num_shards));
}

/* mvsqlite_shard_namespace(base, key, num_shards) -> TEXT, e.g. "world_0007" */
static void mvsqlite_shard_namespace_func(
    sqlite3_context *ctx,
    int argc,
    sqlite3_value **argv
) {
    (void)argc;
    const unsigned char *base = sqlite3_value_text(argv[0]);
    if (base == NULL) {
        sqlite3_result_error(ctx, "base must not be NULL", -1);
        return;
    }

    sqlite3_int64 num_shards = sqlite3_value_int64(argv[2]);
    if (num_shards <= 0) {
        sqlite3_result_error(ctx, "num_shards must be positive", -1);
        return;
    }

    const unsigned char *key_bytes = sqlite3_value_text(argv[1]);
    if (key_bytes == NULL) {
        sqlite3_result_error(ctx, "key must not be NULL", -1);
        return;
    }
    int key_len = sqlite3_value_bytes(argv[1]);

    uint64_t hash = fnv1a64(key_bytes, key_len);
    sqlite3_int64 shard = (sqlite3_int64)(hash % (uint64_t)num_shards);

    char buf[512];
    int n = snprintf(buf, sizeof(buf), "%s_%04lld", (const char *)base, (long long)shard);
    if (n < 0 || n >= (int)sizeof(buf)) {
        sqlite3_result_error(ctx, "base namespace name too long", -1);
        return;
    }
    sqlite3_result_text(ctx, buf, n, SQLITE_TRANSIENT);
}

/*
 * Deny ATTACH on any connection this extension is loaded into. ATTACH is the
 * one thing that would let a connection see more than one namespace/shard at
 * once and join across them - reintroducing the cross-shard coordination cost
 * sharding exists to avoid. Loading this extension is meant to be the ONLY way
 * game-sim code talks to mvSQLite, so it also closes that escape hatch.
 */
static int deny_attach_authorizer(
    void *pArg,
    int action_code,
    const char *p1,
    const char *p2,
    const char *p3,
    const char *p4
) {
    (void)pArg;
    (void)p1;
    (void)p2;
    (void)p3;
    (void)p4;
    if (action_code == SQLITE_ATTACH) {
        return SQLITE_DENY;
    }
    return SQLITE_OK;
}

#ifdef _WIN32
__declspec(dllexport)
#endif
int sqlite3_mvsqliteshard_init(
    sqlite3 *db,
    char **pzErrMsg,
    const sqlite3_api_routines *pApi
) {
    SQLITE_EXTENSION_INIT2(pApi);
    (void)pzErrMsg;

    int rc = sqlite3_create_function(
        db, "mvsqlite_shard_index", 2,
        SQLITE_UTF8 | SQLITE_DETERMINISTIC, 0,
        mvsqlite_shard_index_func, 0, 0
    );
    if (rc != SQLITE_OK) {
        return rc;
    }

    rc = sqlite3_create_function(
        db, "mvsqlite_shard_namespace", 3,
        SQLITE_UTF8 | SQLITE_DETERMINISTIC, 0,
        mvsqlite_shard_namespace_func, 0, 0
    );
    if (rc != SQLITE_OK) {
        return rc;
    }

    return sqlite3_set_authorizer(db, deny_attach_authorizer, 0);
}
