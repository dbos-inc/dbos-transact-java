# Python vs Java Conductor — Gap Analysis

Compared:
- Python: `/home/harry/py-transact/dbos/_conductor/` (28 message types)
- Java: `transact/src/main/java/dev/dbos/transact/conductor/` (17 message types)

---

## Missing message types

### 1. Schedule management (6 message types)

Python handles full schedule lifecycle over the conductor protocol:

| Message | Purpose |
|---------|---------|
| `LIST_SCHEDULES` | List cron schedules with filters (status, workflow_name, name prefix, load_context) |
| `GET_SCHEDULE` | Fetch a single schedule by name |
| `PAUSE_SCHEDULE` | Pause a schedule |
| `RESUME_SCHEDULE` | Resume a paused schedule |
| `BACKFILL_SCHEDULE` | Run backfill for a date range, returns list of workflow IDs created |
| `TRIGGER_SCHEDULE` | Manually trigger a schedule, returns workflow ID |

Java has a `SchedulerService` but none of these are implemented as conductor protocol messages.

---

### 2. Application version management (2 message types)

| Message | Purpose |
|---------|---------|
| `LIST_APPLICATION_VERSIONS` | List registered versions ordered by timestamp |
| `SET_LATEST_APPLICATION_VERSION` | Update the latest version timestamp |

Java tracks `application_version` on workflow rows but has no conductor API for version registry management.

---

### 3. Workflow data retrieval (3 message types)

| Message | Purpose |
|---------|---------|
| `GET_WORKFLOW_EVENTS` | Retrieve all events (key/value pairs) for a workflow |
| `GET_WORKFLOW_NOTIFICATIONS` | Retrieve all notifications for a workflow |
| `GET_WORKFLOW_STREAMS` | Retrieve all stream entries for a workflow |

Java exposes these tables through export but has no dedicated conductor messages for querying them individually.

---

### 4. `GET_WORKFLOW_AGGREGATES` (1 message type)

Python supports group-by aggregate counts over `workflow_status` with configurable group-by flags (status, name, queue_name, executor_id, application_version) and filters (status, time range, name, app_version, executor_id, queue_name, workflow_id_prefix). Java has no equivalent.

---

## Protocol-level gaps in existing message types

### 5. Bulk operations on CANCEL, DELETE, RESUME

Python's `CancelRequest`, `DeleteRequest`, and `ResumeRequest` each accept either a single `workflow_id` or a list `workflow_ids`. Java accepts only a single `workflow_id` per request, requiring multiple round-trips for bulk operations.

---

### 6. `queue_name` in `ResumeRequest`

Python's `ResumeRequest` has an optional `queue_name` field, allowing the workflow to be re-queued to a different queue on resume. Java's `ResumeRequest` has no such field.

---

### 7. `queue_name` and `queue_partition_key` in `ForkWorkflowRequest`

Python's `ForkWorkflowRequest` supports `queue_name` and `queue_partition_key` so the forked workflow can be enqueued onto a specific queue. Java's `ForkWorkflowRequest` has neither field.

---

### 8. `executor_metadata` in `ExecutorInfoResponse`

Python's `ExecutorInfoResponse` includes an optional `executor_metadata` field (populated from `dbos._conductor_executor_metadata`). Java's response has no metadata extensibility point.

---

### 9. `load_output` flag in `ListStepsRequest`

Python's `ListStepsRequest` has a `load_output` boolean to control whether step output is included in the response. Java always returns step output.

---

### 10. `workflow_id_prefix` filter in `ListWorkflowsRequest`

Python supports filtering workflows by a UUID prefix. Java's `ListWorkflowsRequest` has no equivalent filter.

---

## Summary table

| Priority | Gap |
|----------|-----|
| High | Schedule management (LIST, GET, PAUSE, RESUME, BACKFILL, TRIGGER) |
| High | Bulk operations for CANCEL, DELETE, RESUME |
| Medium | Application version management (LIST, SET_LATEST) |
| Medium | `GET_WORKFLOW_EVENTS`, `GET_WORKFLOW_NOTIFICATIONS`, `GET_WORKFLOW_STREAMS` |
| Medium | `GET_WORKFLOW_AGGREGATES` |
| Medium | `queue_name` in ForkWorkflowRequest |
| Low | `queue_name` in ResumeRequest |
| Low | `executor_metadata` in ExecutorInfoResponse |
| Low | `load_output` flag in ListStepsRequest |
| Low | `workflow_id_prefix` filter in ListWorkflowsRequest |
