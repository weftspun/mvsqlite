# mvsqlite_shard

A standard SQLite loadable extension providing one canonical shard-routing
function, for splitting writers across multiple mvSQLite namespaces to get
linear write throughput scaling instead of bottlenecking on one namespace's
serialization primitives (last-write-version, namespace lock, commit-token).

## Build

```bash
make -C mvsqlite-shard-ext build
```

Produces `libmvsqlite_shard.so` (or `.dylib` on macOS).

## Usage

```sql
.load ./libmvsqlite_shard

-- Which shard index (0..num_shards) does this key route to?
SELECT mvsqlite_shard_index('player-1234', 16);

-- Convenience: build the full namespace name directly.
SELECT mvsqlite_shard_namespace('world', 'player-1234', 16);
-- -> 'world_0007'
```

Route by whatever key makes writes to the same shard land together and
different shards stay independent for your workload — player ID for
flat sharding, or a spatial cell ID for interest-management-aligned sharding.
The function is deterministic (same key + shard count always routes the same
way), so every client language calling it gets identical routing without
reimplementing the hash.

This extension only computes *which* namespace to use — opening the
corresponding mvSQLite connection for that namespace is still the caller's
responsibility, the same as choosing any other connection target.

## Enforcement

Loading this extension into a connection also denies `ATTACH DATABASE` on
that connection (via `sqlite3_set_authorizer`). ATTACH is the one thing that
would let a connection see more than one namespace/shard at once and join
across them, silently reintroducing the cross-shard coordination cost
sharding exists to avoid — so a connection that loads this extension can
never do that, regardless of what the application code tries.

That's client-side enforcement (it protects a connection that loads this
extension). For server-side enforcement — actually preventing any
non-sharded namespace from being created in the first place, so it can't be
bypassed by a client that never loads this extension — start mvstore with
`--require-sharded-namespace-names`. This rejects creation of any namespace
whose key doesn't end in `_NNNN` (four digits, matching
`mvsqlite_shard_namespace()`'s output), at both the explicit
`/api/create_namespace` call and namespace auto-creation on first access.
