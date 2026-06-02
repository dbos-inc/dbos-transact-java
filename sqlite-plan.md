# Plan: SQLite Support

## Goals

Full production-grade support for SQLite as a database backend alongside PostgreSQL and CockroachDB. Feature parity except where noted in the out-of-scope section.

This plan is informed by the existing SQLite implementations in `~/py-transact` and `~/go-transact`, which have already solved most of the design questions.

---

## Schema Namespacing

PostgreSQL uses `"schema"."table_name"`. SQLite has no schema concept.

**Approach: no prefix.** Both the Python and Go implementations use bare table names for SQLite — `workflow_status`, not `dbos_workflow_status`. For SQLite, `DbContext.tableRef("workflow_status")` returns `workflow_status`; for PostgreSQL/CockroachDB it returns `"dbos"."workflow_status"` as today.

The format string `"%1$s".workflow_status` becomes `workflow_status` for SQLite.

---

## Out of Scope

- **TxStepFactory** (`SQLiteJdbcStepFactory`, `SQLiteJdbiStepFactory`, `SQLiteJooqStepFactory`): Deferred. Core workflow/queue/step functionality is in scope; transactional steps with JDBI/jOOQ are not.
- **Cross-dialect data migration**: SQLite and PostgreSQL instances are not interchangeable. Switching databases means starting a new DBOS instance; in-flight workflows do not transfer. A startup validation check should reject a SQLite database previously initialized by PostgreSQL (and vice versa), failing fast rather than silently corrupting state.

---

## Steps

### Step 1: Relax Validation + Add `tableRef()` to `DbContext`

No `DbDialect` enum is needed in the runtime layer. SQLite detection uses the JDBC URL (starts with `jdbc:sqlite:`) — no connection required.

**`SystemDatabase`**: rename `validatePostgresDataSource()` to `validateDataSource()` and accept SQLite. All other PostgreSQL-specific methods (`createDatabaseIfNotExists()`, `extractDbAndPostgresUrl()`, `createDataSource()`) branch on whether the URL is a SQLite URL.

**`DbContext`**: add a `tableQualifier` string field computed once at construction:
- PostgreSQL/CockroachDB: `"schema".` (e.g. `"dbos".`)
- SQLite: `""` (empty)

Add `tableRef(String tableName)` method that returns `tableQualifier + tableName`. All DAO SQL moves from inline `"%s".workflow_status` patterns to `ctx.tableRef("workflow_status")`.

### Step 3: SQLite-Specific Migrations

Write a parallel set of SQLite migrations (35 total). Key rewrites from the PostgreSQL originals:

| PostgreSQL | SQLite replacement |
|---|---|
| `CREATE EXTENSION IF NOT EXISTS "uuid-ossp"` | Remove entirely |
| `gen_random_uuid()` as DEFAULT | Remove DEFAULT; app supplies `UUID.randomUUID().toString()` |
| `EXTRACT(epoch FROM now()) * 1000.0` as DEFAULT | Remove DEFAULT; app supplies `System.currentTimeMillis()` |
| `"schema".table_name` | `table_name` (no schema prefix) |
| `CREATE SCHEMA IF NOT EXISTS "schema"` | No-op |
| `NOTIFY` triggers | Remove entirely |
| `NUMERIC(38,0)` | `TEXT` |
| `CREATE INDEX CONCURRENTLY` | `CREATE INDEX` |
| `information_schema.schemata` queries | `sqlite_master` queries |
| `INT4`, `BIGINT` | `INTEGER` |

`getMigrations()` in `MigrationManager` takes a new `DbDialect` param and routes to the right list.

### Step 4: Migration Locking

Drop the `pg_try_advisory_lock()` path for SQLite entirely. IMMEDIATE transactions are the locking mechanism — SQLite acquires a write lock at the start of every transaction, so concurrent migration attempts serialize cleanly without advisory locks or Java-level locks.

The existing advisory lock path stays for PostgreSQL/CockroachDB unchanged.

### Step 5: UUID and Timestamp Generation

Most DAOs already supply UUIDs and timestamps explicitly from Java. Only two INSERT statements rely on DB-side defaults and need updating:

- **`QueuesDAO.upsertQueue()`**: omits `queue_id` (relies on `DEFAULT gen_random_uuid()`) and `created_at` (relies on timestamp DEFAULT). Add both columns to the INSERT and supply `UUID.randomUUID().toString()` and `System.currentTimeMillis()`.
- **`ApplicationVersionDAO.createApplicationVersion()`**: omits `version_timestamp` and `created_at` (both rely on timestamp DEFAULTs). Add both columns to the INSERT and supply `System.currentTimeMillis()` for each.

The SQLite migrations omit these DEFAULT expressions entirely. The PostgreSQL migrations can keep them as fallbacks, but the DAO changes make both databases consistent.

### Step 6: `SystemDatabase` Plumbing

Several PostgreSQL-specific methods need SQLite branches:

- **`createDataSource()`**: New SQLite-specific HikariCP config:
  - No username/password
  - Pool size 8 (matching go-transact: `MaxOpenConns=8`)
  - `connectionInitSql` to apply PRAGMAs on each connection (see below)
  - Remove PostgreSQL-specific properties (`tcpKeepAlive`, `reWriteBatchedInserts`)
- **`createDatabaseIfNotExists()`**: Skip — SQLite creates the file automatically.
- **`ensureDbosSchema()`**: Skip — no schemas.
- **`shouldMigrate()`**: Replace `information_schema` queries with `sqlite_master` queries.
- **`extractDbAndPostgresUrl()`**: Skip for SQLite URLs.
- **Notification source**: SQLite always gets `NullNotificationSource` (polling). Expose `dbPollingInterval` as `notificationPollingInterval` in `DBOSConfig` (default `Duration.ofSeconds(1)`, following Python's `notification_listener_polling_interval_sec`). Remove the test-only `speedUpPollingForTest()` workaround — tests set the config field directly instead.

**PRAGMAs applied to every SQLite connection** (via `connectionInitSql` or connection event listener):

```sql
PRAGMA journal_mode = WAL;       -- concurrent reads + one writer
PRAGMA synchronous = NORMAL;     -- 3-5x faster than FULL, safe with WAL
PRAGMA busy_timeout = 5000;      -- wait up to 5s on write lock contention
PRAGMA foreign_keys = ON;        -- off by default in SQLite
```

**IMMEDIATE transactions**: SQLite connections must use IMMEDIATE transaction mode so that every transaction acquires the write lock upfront, preventing `SQLITE_BUSY` mid-transaction. In HikariCP this can be set via `transactionIsolation` or by wrapping the connection. (go-transact uses `_txlock=immediate` as a DSN parameter; py-transact sets `isolation_level="IMMEDIATE"` on each connection.)

### Step 7: Build Configuration

In `transact/build.gradle.kts`, move SQLite from `testImplementation` to `implementation`:

```kotlin
implementation(libs.sqlite.jdbc)  // was testImplementation
```

### Step 8: Test Infrastructure

Add a SQLite test path alongside the existing `PgContainer`. Both py-transact and go-transact use **temp files per test** (not in-memory), which avoids the shared-cache complexity of `jdbc:sqlite::memory:` with a connection pool.

```java
// SqliteTestDb.java
public class SqliteTestDb {
    // Creates a temp file per test class via Files.createTempFile()
    // Runs migrations on setup
    // Deletes the file on teardown
}
```

Add `DBOS_TEST_BACKEND=sqlite` env var (matching go-transact's convention) to route test database setup. Update `PgContainer` to check this flag.

---

## Phasing

1. **Phase 1** — Dialect detection + `DbDialect` enum + relax validation (no behavior change)
2. **Phase 2** — `DbContext.tableRef()` + update all DAOs + SQLite migrations (core work)
3. **Phase 3** — `MigrationManager` SQLite support + `SystemDatabase` plumbing
4. **Phase 4** — Test infrastructure + promote SQLite JDBC to `implementation`

Phases 1, 3, and 4 can proceed in parallel. Phase 2 is the largest chunk and blocks Phase 3.

---

## Reference Implementations

| Decision | Python (`py-transact`) | Go (`go-transact`) | Java (this plan) |
|---|---|---|---|
| Schema handling | `None` (no prefix) | `""` (no prefix) | `tableQualifier=""` in `DbContext` |
| TX isolation | `IMMEDIATE` | `_txlock=immediate` | IMMEDIATE mode |
| Pool size | SQLAlchemy default | MaxOpenConns=8 | 8 |
| UUID generation | `hex(randomblob(16))` DEFAULT | App: `uuid.New().String()` | App: `UUID.randomUUID()` |
| Timestamps | `unixepoch('subsec')*1000` DEFAULT | App: `time.Now().UnixMilli()` | App: `System.currentTimeMillis()` |
| Journal mode | Not set | WAL | WAL |
| Synchronous | Not set | NORMAL | NORMAL |
| Busy timeout | Not set | 5000ms | 5000ms |
| Foreign keys | ON | ON | ON |
| Migration locking | IMMEDIATE TX | IMMEDIATE TX | IMMEDIATE TX |
| Test setup | Temp file | Temp file (`t.TempDir()`) | Temp file |
