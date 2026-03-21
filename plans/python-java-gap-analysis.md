# Python vs Java System Database — Gap Analysis

Features present in Python (`/home/harry/py-transact/dbos/_sys_db.py`) that are missing from the Java database layer (`transact/src/main/java/dev/dbos/transact/database/`).

---

### 1. Stream write/read/close API

Python has a complete runtime stream abstraction:
- `write_stream_from_step()` / `write_stream_from_workflow()` — append values with atomic offset assignment
- `close_stream()` — writes a sentinel value
- `read_stream()` — fetches value at a specific (workflow, key, offset)
- `get_all_stream_entries()` — returns all values ordered by offset
- Stream entries before `start_step` are copied during workflow fork

Java has a `streams` table and can read stream rows during export/import, but has no runtime write, close, or read API.

---

### 2. `DELAYED` workflow status and `transition_delayed_workflows()`

Python supports a `DELAYED` state for workflows enqueued with a `delay_until_epoch_ms` timestamp. A background process calls `transition_delayed_workflows()` to move `DELAYED` → `ENQUEUED` when the delay expires. Java has no equivalent status or transition mechanism — delayed execution is not supported.

---

### 3. Application version management

Python manages deployment versions as first-class entities:
- `create_application_version()` — idempotent insert
- `update_application_version_timestamp()` — update version ordering
- `list_application_versions()` — ordered by timestamp DESC
- `get_latest_application_version()` — returns most recent

Java has `application_version` as a field on workflow rows but no dedicated version registry or version lifecycle API.

---

### 4. Schedule CRUD in the database layer

Python has a full schedule management API in `_sys_db.py`:
- `create_schedule()`, `get_schedule()`, `list_schedules()`
- `pause_schedule()`, `resume_schedule()`
- `update_last_fired_at()`
- `delete_schedule()`

Java has a `SchedulerService` but schedule persistence is not part of the `SystemDatabase` facade — it is unclear whether the schedule CRUD operations reach the same level of completeness.

---

### 5. Workflow aggregates

Python exposes `get_workflow_aggregates()`, which issues group-by count queries over `workflow_status` (by status, name, queue_name, executor_id, app_version). Java has `getMetrics()` which counts workflow and step completions by name, but no general aggregation query.

---

### 6. Two-phase recv/getEvent design

Python explicitly separates the receive operation into `recv_setup` (register listener, check DB, record durable sleep) and `recv_consume` (transactional consumption, record result). This makes the phase boundary explicit and easier to reason about.

Java folds both phases into a single `recv` method with internal control flow, which achieves the same result but is harder to follow.

---

### 7. Retry strategy — unbounded vs bounded

Python's `@db_retry` decorator retries indefinitely with exponential backoff and random jitter (initial 1s, max 60s, multiplied by random 0.5–1.0). The design prioritises correctness over availability.

Java caps retries at 20 attempts. Under prolonged database unavailability Java will eventually throw, while Python will keep retrying.

---

## Priority

| Priority | Gap |
|----------|-----|
| High | Stream write/read/close API |
| High | `DELAYED` status and `transition_delayed_workflows()` |
| Medium | Application version registry |
| Low | Workflow aggregates |
| Low | Schedule CRUD consolidation |
| Low | Unbounded retry (Java caps at 20) |
| Low | Two-phase recv design |
