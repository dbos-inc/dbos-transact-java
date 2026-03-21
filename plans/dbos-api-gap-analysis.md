# Python vs Java DBOS API — Gap Analysis

Features present in Python (`/home/harry/py-transact/dbos/_dbos.py`) that are missing from Java (`transact/src/main/java/dev/dbos/transact/DBOS.java`).

Async variants are excluded — Java is synchronous by design.

---

### 1. Schedule management

Python exposes a full schedule lifecycle on the DBOS class. Java has no equivalent public API.

| Python method | Purpose |
|---------------|---------|
| `create_schedule(schedule_name, workflow_fn, schedule, context, automatic_backfill, cron_timezone)` | Create a cron schedule |
| `list_schedules(status, workflow_name, schedule_name_prefix)` | List schedules with filters |
| `get_schedule(name)` | Get a schedule by name |
| `delete_schedule(name)` | Delete a schedule |
| `pause_schedule(name)` | Pause a schedule |
| `resume_schedule(name)` | Resume a paused schedule |
| `apply_schedules(schedules)` | Atomically create or replace a set of schedules |
| `backfill_schedule(schedule_name, start, end)` | Enqueue all missed executions in a time window, returns list of handles |
| `trigger_schedule(schedule_name)` | Immediately enqueue a scheduled workflow, returns handle |

---

### 2. Streams

Python exposes stream read/write/close on the DBOS class. Java has no public stream API.

| Python method | Purpose |
|---------------|---------|
| `write_stream(key, value, serialization_type)` | Append a value to a named stream within a workflow |
| `close_stream(key)` | Write a close sentinel to a stream |
| `read_stream(workflow_id, key)` | Read a stream as a generator, blocking until values arrive or stream closes |

---

### 3. Application version management

Python exposes version registry operations on the DBOS class. Java has no equivalent.

| Python method | Purpose |
|---------------|---------|
| `list_application_versions()` | List all registered application versions ordered by timestamp |
| `get_latest_application_version()` | Get the most recent application version |
| `set_latest_application_version(version_name)` | Promote a version to latest |

---

### 4. Bulk workflow operations

Python has list-accepting variants of cancel, delete, and resume. Java only accepts a single workflow ID per call.

| Python method | Java equivalent |
|---------------|----------------|
| `cancel_workflows(workflow_ids: List[str])` | `cancelWorkflow(String)` — single only |
| `delete_workflows(workflow_ids: List[str], delete_children)` | `deleteWorkflow(String, boolean)` — single only |
| `resume_workflows(workflow_ids: List[str], queue_name)` | `resumeWorkflow(String)` — single only |

---

### 5. `get_all_events(workflow_id)`

Python has `get_all_events(workflow_id) -> Dict[str, Any]` which returns every event key/value for a workflow in one call. Java only has `getEvent(workflowId, key, timeout)` for fetching a single known key.

---

### 6. `wait_first(handles)`

Python has `wait_first(handles: List[WorkflowHandle]) -> WorkflowHandle` which blocks until the first of a list of workflows completes and returns its handle. Java has no equivalent — callers must poll each handle individually.

---

### 7. `queue_name` parameter on `resumeWorkflow` and `forkWorkflow`

Python's `resume_workflow` and `fork_workflow` both accept an optional `queue_name` (and `fork_workflow` also accepts `queue_partition_key`) to control which queue the resumed/forked workflow is enqueued on. Java's `resumeWorkflow` and `forkWorkflow` have no such parameters.

---

### 8. `list_queued_workflows`

Python has a dedicated `list_queued_workflows(...)` method with the same filter set as `list_workflows`. Java has a single `listWorkflows(ListWorkflowsInput)` — it is unclear whether `ListWorkflowsInput` exposes a `queuesOnly` flag to achieve the same result.

---

### 9. `set_authentication(authenticated_user, authenticated_roles)`

Python has `set_authentication(user, roles)` to inject auth context into the current execution context (used in middleware/request handlers). Java has no equivalent public method on `DBOS`.

---

### 10. Transaction support as a distinct concept

Python has `@dbos.transaction(isolation_level)` as a first-class decorator that wraps a function in a database transaction and exposes `DBOS.sql_session` (a SQLAlchemy session) inside. Java's `runStep` is the only analogous entry point and does not expose a database session or accept an isolation level.

---

## Summary

| Priority | Gap |
|----------|-----|
| High | Streams (`write_stream`, `close_stream`, `read_stream`) |
| High | Schedule management (9 methods) |
| Medium | Bulk workflow operations (`cancel_workflows`, `delete_workflows`, `resume_workflows`) |
| Medium | `queue_name` on `resumeWorkflow` and `forkWorkflow` |
| Medium | Application version management (3 methods) |
| Medium | Transaction support (`@transaction`, `sql_session`) |
| Low | `get_all_events(workflow_id)` |
| Low | `wait_first(handles)` |
| Low | `list_queued_workflows` convenience method |
| Low | `set_authentication` |
