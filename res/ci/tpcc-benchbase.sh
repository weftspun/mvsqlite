#!/bin/bash
# Builds a BenchBase TPC-C benchmark that talks to mvsqlite directly, with no
# LD_PRELOAD involved.
#
# BenchBase's SQLite support goes through sqlite-jdbc, which statically
# compiles a full SQLite amalgamation into its own native library rather than
# dynamically linking a system libsqlite3.so - so LD_PRELOAD (mvsqlite's
# usual interposition mechanism, see mvsqlite-preload/) has nothing to
# intercept there. Instead, this script rebuilds sqlite-jdbc's native library
# from source, substituting mvsqlite's own patched SQLite amalgamation
# (mvsqlite-preload/sqlite-3410000.patch) for the stock one sqlite-jdbc
# would otherwise download - so the resulting libsqlitejdbc.so has mvsqlite
# baked in directly and depends on libfdb_c.so instead of libsqlite3.so.
#
# Must be run from the repo root, inside a container with: a Rust toolchain,
# clang, libsqlite3-dev, the FDB client .deb installed, and a JDK 23 + Maven
# (see Dockerfile.benchbase). Requires MVSQLITE_FDB_CLUSTER and friends to be
# set for the final run step.
#
# Usage: res/ci/tpcc-benchbase.sh [prepare|run] [benchbase.jar args...]
#   prepare - build the patched sqlite-jdbc driver and BenchBase, install
#             both into ./target/tpcc-benchbase/. Safe to skip on reruns.
#   run     - execute ./target/tpcc-benchbase/benchbase-sqlite/benchbase.jar
#             with the given args (e.g. -b tpcc -c config/sqlite/sample_tpcc_config.xml
#             --create=true --load=true --execute=true).

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
WORK="$REPO_ROOT/target/tpcc-benchbase"
AMAL_DIR="$WORK/sqlite-amalgamation"
SQLITE_JDBC_DIR="$REPO_ROOT/third_party/sqlite-jdbc"
BENCHBASE_DIR="$REPO_ROOT/third_party/benchbase"

prepare() {
  mkdir -p "$AMAL_DIR"
  cd "$AMAL_DIR"

  # Same amalgamation version and patch mvsqlite-preload's own CI build uses
  # (see .github/workflows/ci.yml's "sqlite3.c" step) - keeps this in sync
  # with the one already-verified LD_PRELOAD build instead of drifting.
  if [ ! -f sqlite3.c ]; then
    curl -fL -o sqlite.zip https://www.sqlite.org/2023/sqlite-amalgamation-3410000.zip
    unzip -qo sqlite.zip
    cp sqlite-amalgamation-3410000/sqlite3.c sqlite-amalgamation-3410000/sqlite3.h .
    patch -p1 ./sqlite3.c < "$REPO_ROOT/mvsqlite-preload/sqlite-3410000.patch"
  fi

  cargo build --release --manifest-path "$REPO_ROOT/Cargo.toml" -p mvsqlite

  # Combine the patched amalgamation with mvsqlite's shim/preload glue into
  # one archive. -DMV_STATIC_PATCH switches shim.c/preload.c to call the
  # renamed real_sqlite3_open_v2/real_sqlite3_step functions directly
  # (defined right inside the patched sqlite3.c) instead of the LD_PRELOAD
  # build's dlsym(RTLD_NEXT, ...) lookup - there's no separate "real" sqlite3
  # library to find here, mvsqlite *is* the only sqlite3 in this process.
  cc -O2 -fPIC -c -DMV_STATIC_PATCH \
    -DSQLITE_ENABLE_FTS3 -DSQLITE_ENABLE_FTS4 -DSQLITE_ENABLE_FTS5 \
    -DSQLITE_ENABLE_RTREE -DSQLITE_ENABLE_DBSTAT_VTAB \
    -DSQLITE_ENABLE_MATH_FUNCTIONS -DSQLITE_ENABLE_COLUMN_METADATA \
    -o sqlite3.o sqlite3.c
  cc -O2 -fPIC -c -DMV_STATIC_PATCH -I"$REPO_ROOT/mvsqlite-preload" \
    -o preload.o "$REPO_ROOT/mvsqlite-preload/preload.c"
  cc -O2 -fPIC -c -DMV_STATIC_PATCH -I"$REPO_ROOT/mvsqlite-preload" \
    -o shim.o "$REPO_ROOT/mvsqlite-preload/shim.c"
  ar rcs libmvsqlite_combined.a sqlite3.o preload.o shim.o

  cd "$SQLITE_JDBC_DIR"
  make native \
    SQLITE_SOURCE="$AMAL_DIR" \
    SQLITE_OBJ="$AMAL_DIR/libmvsqlite_combined.a" \
    Linux-x86_64_LINKFLAGS="-shared -static-libgcc -pthread -lm -L$REPO_ROOT/target/release -lmvsqlite -lfdb_c -ldl"
  mvn -q package -DskipTests

  local jdbc_jar
  jdbc_jar="$(ls target/sqlite-jdbc-*.jar)"
  mvn -q install:install-file \
    -Dfile="$jdbc_jar" \
    -DgroupId=org.xerial -DartifactId=sqlite-jdbc -Dversion=3.47.1.0 -Dpackaging=jar

  cd "$BENCHBASE_DIR"
  mvn -q clean package -P sqlite -DskipTests

  mkdir -p "$WORK"
  rm -rf "$WORK/benchbase-sqlite"
  tar xzf target/benchbase-sqlite.tgz -C "$WORK"
}

run() {
  shift || true
  cd "$WORK/benchbase-sqlite"
  exec java -jar benchbase.jar "$@"
}

case "${1:-}" in
  prepare) prepare ;;
  run) run "$@" ;;
  *)
    echo "usage: $0 [prepare|run] [benchbase.jar args...]" >&2
    exit 1
    ;;
esac
