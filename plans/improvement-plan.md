# Java DBOS Improvement Plan

Synthesised from gap analysis across public API (`DBOS.java`), conductor protocol, and system database (`SystemDatabase` / DAOs).

---

## Area 1: Streams

The `streams` table exists and is populated/read during export/import, but the runtime API is entirely missing.

**Public API gaps:**
- `writeStream(String key, Object value)` — append a value to a named stream within a workflow
- `closeStream(String key)` — write a close sentinel
- `readStream(String workflowId, String key)` — blocking read, returning values as they arrive

**System database gaps:**
- `writeStreamFromStep()` / `writeStreamFromWorkflow()` — atomic offset assignment via `COALESCE(MAX(offset), -1) + 1`
- `closeStream()` — insert sentinel
- `readStream(workflowId, key, offset)` — fetch by offset
- Stream entries before `startStep` copied during `forkWorkflow`

**Conductor gaps:**
- `GET_WORKFLOW_STREAMS` message type — retrieve all stream entries for a workflow

---

## Area 2: Schedule management

Java has a `SchedulerService` internally but exposes no schedule lifecycle through the public API or conductor protocol.

**Public API gaps:**
- `createSchedule(name, workflowFn, cron, context, automaticBackfill, cronTimezone)`
- `listSchedules(status, workflowName, namePrefix)`
- `getSchedule(name)`
- `deleteSchedule(name)`
- `pauseSchedule(name)`
- `resumeSchedule(name)`
- `applySchedules(List<ScheduleInput>)` — atomically create or replace a set of schedules
- `backfillSchedule(name, start, end)` — enqueue all missed executions in a time window
- `triggerSchedule(name)` — immediately enqueue a scheduled workflow

**System database gaps:**
- Consolidate schedule CRUD into `SystemDatabase` facade (`createSchedule`, `getSchedule`, `listSchedules`, `pauseSchedule`, `resumeSchedule`, `updateLastFiredAt`, `deleteSchedule`)

**Conductor gaps:**
- `LIST_SCHEDULES`, `GET_SCHEDULE`, `PAUSE_SCHEDULE`, `RESUME_SCHEDULE`, `BACKFILL_SCHEDULE`, `TRIGGER_SCHEDULE` message types

---

## Area 3: Application version management

Java stores `application_version` as a field on workflow rows but has no registry for managing versions as first-class entities.

**Public API gaps:**
- `listApplicationVersions()`
- `getLatestApplicationVersion()`
- `setLatestApplicationVersion(String versionName)`

**System database gaps:**
- `createApplicationVersion()` — idempotent insert
- `updateApplicationVersionTimestamp()` — update ordering
- `listApplicationVersions()` — ordered by timestamp DESC
- `getLatestApplicationVersion()`

**Conductor gaps:**
- `LIST_APPLICATION_VERSIONS`, `SET_LATEST_APPLICATION_VERSION` message types

---

## Area 4: Delayed workflow execution

Python supports a `DELAYED` state allowing workflows to be enqueued with a future start time.

**System database gaps:**
- `DELAYED` workflow status
- `delay_until_epoch_ms` column on `workflow_status`
- `transitionDelayedWorkflows()` — background process to move `DELAYED` → `ENQUEUED` when delay expires

**Public API gaps:**
- `StartWorkflowOptions` / `EnqueueOptions` should support a `delayUntil` / `delayMs` parameter

---

## Area 5: Bulk workflow operations

Every mutating workflow operation in Java accepts a single workflow ID. Python accepts both single IDs and lists, enabling efficient bulk operations without multiple round-trips.

**Public API gaps:**
- `cancelWorkflows(List<String> workflowIds)`
- `deleteWorkflows(List<String> workflowIds, boolean deleteChildren)`
- `resumeWorkflows(List<String> workflowIds)`

**Conductor gaps:**
- `CancelRequest`, `DeleteRequest`, `ResumeRequest` — add `workflow_ids` list field alongside existing `workflow_id`

---

## Area 6: Queue targeting on resume and fork

Python allows a workflow to be resumed or forked onto a specific queue. Java ignores queue placement on these operations.

**Public API gaps:**
- `resumeWorkflow(String workflowId, String queueName)`
- `forkWorkflow(String workflowId, int startStep, ForkOptions options)` — `ForkOptions` should include `queueName` and `queuePartitionKey`

**Conductor gaps:**
- `ResumeRequest` — add `queue_name` field
- `ForkWorkflowRequest` — add `queue_name` and `queue_partition_key` fields

---

## Area 7: Workflow data retrieval via conductor

Python exposes dedicated conductor messages to retrieve per-workflow internal data. Java only surfaces this data through the bulk export mechanism.

No schema changes are needed — `workflow_events`, `notifications`, and `streams` tables already exist.

**System database gaps:**
- `getAllEvents(workflowId)` — expose the existing private `listWorkflowEvents` logic as a public method; returns a map of key → deserialized value
- `getAllNotifications(workflowId)` — new method; queries `notifications` by `destination_uuid`, orders by `created_at_epoch_ms`, deserializes message, converts the null-topic sentinel (`_dbos_null_topic`) to `null`; returns list of `{topic, message, created_at_epoch_ms, consumed}`
- `getAllStreamEntries(workflowId)` — expose private `listWorkflowStreams` but with additional processing: order by `(key, offset)`, skip the close sentinel (`_dbos_stream_closed_sentinel`), group into a map of key → ordered list of values

**Conductor gaps:**
- `GET_WORKFLOW_EVENTS` — retrieve all event key/value pairs for a workflow
- `GET_WORKFLOW_NOTIFICATIONS` — retrieve all notifications for a workflow
- `GET_WORKFLOW_STREAMS` — retrieve all stream entries for a workflow (also blocked by Area 1)

---

## Area 8: Workflow aggregates

Python has a flexible group-by aggregation query. Java's `getMetrics()` only counts completions by name.

No schema changes are needed — all grouping and filtering columns already exist on `workflow_status`.

**System database gaps:**
- `getWorkflowAggregates(...)` — new method; dynamically builds `SELECT [group cols], COUNT(*) FROM workflow_status WHERE [...] GROUP BY [group cols]`; requires at least one group-by flag to be set
  - **Group-by flags (boolean):** `groupByStatus`, `groupByName`, `groupByQueueName`, `groupByExecutorId`, `groupByApplicationVersion`
  - **Filter parameters (all optional):** `status: List<String>` (IN), `startTime`, `endTime` (epoch ms range on `created_at`), `name: List<String>` (IN), `appVersion: List<String>` (IN), `executorId: List<String>` (IN), `queueName: List<String>` (IN), `workflowIdPrefix: List<String>` (OR'd LIKE prefix matches on `workflow_uuid`)
  - **Return type:** list of `WorkflowAggregateRow` — a new type with a `Map<String, String> group` (column name → value) and a `long count`; mirrors Python's `WorkflowAggregateRow(group={...}, count=N)`

**Conductor gaps:**
- `GET_WORKFLOW_AGGREGATES` message type

---

## Area 9: Workflow querying improvements

Several filter and control parameters available in Python are absent from Java's `listWorkflows`.

**Public API / conductor gaps:**
- `workflow_id_prefix` filter on `listWorkflows` / `ListWorkflowsRequest`
- `listQueuedWorkflows(ListWorkflowsInput)` — convenience method equivalent to `listWorkflows` with `queuesOnly=true`
- `load_output` flag on `listWorkflowSteps` / `ListStepsRequest` — skip deserialising step output when not needed

---

## Area 10: Minor API completeness

Smaller gaps that round out the public API.

- **`getAllEvents(String workflowId)`** — return all event key/value pairs for a workflow in one call; Java only supports `getEvent` for a single known key
- **`waitFirst(List<WorkflowHandle<?,?>>)`** — block until the first of a list of workflow handles completes
- **`executor_metadata` in `ExecutorInfoResponse`** — extensibility field for conductor to receive arbitrary executor metadata

---

## Priority summary

| Priority | Area |
|----------|------|
| High | Area 1 — Streams |
| High | Area 2 — Schedule management |
| High | Area 4 — Delayed workflow execution |
| High | Area 5 — Bulk workflow operations |
| Medium | Area 3 — Application version management |
| Medium | Area 6 — Queue targeting on resume/fork |
| Medium | Area 7 — Workflow data retrieval via conductor |
| Medium | Area 8 — Workflow aggregates |
| Low | Area 9 — Workflow querying improvements |
| Low | Area 10 — Minor API completeness |
