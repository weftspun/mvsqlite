# mvsqlite for Windows

```bat
REM for windows
REM scoop install llvm-mingw@20220323
```

```bat
cmd
scoop uninstall llvm-mingw
scoop install llvm openssl-mingw
cargo build --release -p mvsqlite
cd mvsqlite-sqlite3
set CC="%userprofile%/scoop/apps/mingw/current/bin/x86_64-w64-mingw32-gcc.exe"
mingw32-make.exe build-patched-sqlite3
```

## Check fdb status

```bash
'C:\Program Files\foundationdb\bin\fdbcli.exe'
status
# Migrate the database from in-memory to ssd
configure perpetual_storage_wiggle=1 storage_migration_type=gradual
configure single ssd
# Check the status
status
```

## Start sqlite client

mvSQLite's SQLite VFS talks to FoundationDB directly - there's no server process to run, just environment variables pointing at the cluster file and namespace prefixes.

```cmd
set RUST_LOG=info
set MVSQLITE_FDB_CLUSTER=C:/ProgramData/foundationdb/fdb.cluster
set MVSQLITE_METADATA_PREFIX=mvsqlite
set MVSQLITE_RAW_DATA_PREFIX=m
set MVSQLITE_AUTO_CREATE_NAMESPACE=1
sqlite3.exe mvsqlite
.tables
```

## Installing foundationdb on Linux

```bash
# on Linux
wget https://github.com/apple/foundationdb/releases/download/7.1.15/foundationdb-clients_7.1.15-1_amd64.deb
sudo dpkg -i foundationdb-clients_7.1.15-1_amd64.deb
wget https://github.com/apple/foundationdb/releases/download/7.1.15/foundationdb-server_7.1.15-1_amd64.deb
sudo dpkg -i foundationdb-server_7.1.15-1_amd64.deb
```

## Installing foundationdb on Windows

```bash
cmd
REM install https://github.com/apple/foundationdb/releases/download/7.1.25/foundationdb-7.1.25-x64.msi
REM Copy fdb_c_types.h
```

## Copy fdb_c_types.h to `C:/Program Files/foundationdb/include/foundationdb/fdb_c_types.h`

```C
/*
 * fdb_c_types.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2022 Apple Inc. and the FoundationDB project authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef FDB_C_TYPES_H
#define FDB_C_TYPES_H
#pragma once

#ifndef DLLEXPORT
#define DLLEXPORT
#endif

#ifdef __cplusplus
extern "C" {
#endif

/* Pointers to these opaque types represent objects in the FDB API */
typedef struct FDB_future FDBFuture;
typedef struct FDB_result FDBResult;
typedef struct FDB_cluster FDBCluster;
typedef struct FDB_database FDBDatabase;
typedef struct FDB_tenant FDBTenant;
typedef struct FDB_transaction FDBTransaction;

typedef int fdb_error_t;
typedef int fdb_bool_t;

#ifdef __cplusplus
}
#endif
#endif
```

## Create a mvsqlite database

Set `MVSQLITE_AUTO_CREATE_NAMESPACE=1` (see above) and the namespace is created automatically the first time it's opened - no separate admin API call needed.

## Debugging library loading

```powershell
scoop install Dependencies 
```
