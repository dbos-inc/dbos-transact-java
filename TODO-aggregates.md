# Aggregates PR — Remaining Work

## Blocker: `completed_at` column on `workflow_status`

The migration PR must land first. Once it does, these items become unblocked.

### Write-side SQL (currently broken — revert or gate on migration)

Three places in `WorkflowDAO.java` reference `completed_at` on `workflow_status` but the column doesn't exist yet:

1. **`updateWorkflowOutcome`** (`~line 327`)  
   Currently sets `completed_at = ?` in the UPDATE. Either:
   - Land this as-is once migration PR merges, OR
   - Revert to the old 4-param form now and add it back after migration

2. **`cancelWorkflows`** (`~line 992`)  
   Currently sets `completed_at = (EXTRACT(EPOCH FROM now()) * 1000)::bigint`.  
   Same choice as above.

3. **`importWorkflow`** (`~line 1564`)  
   Currently inserts `completed_at` (param 29, derived from `updatedAt` for terminal states).  
   **This is why ALL current SystemDatabaseTest failures happen** — `importWorkflow` is called in almost every test and errors immediately with "column completed_at does not exist".

**Immediate fix to unblock tests:** Revert the three write-side changes above. They will be re-added once the migration column exists.

---

## Tests to add/fix once migration lands

### `testGetWorkflowAggregatesCompletedFilters`
Already written in `SystemDatabaseTest.java` but will fail until `completed_at` column exists and is populated.  
Options: `@Disabled("requires completed_at migration")`, or move into a separate `@Tag` group.

### `testGetWorkflowAggregatesMaxTotalLatencyMs`
Not yet written. Needs `completed_at` populated by `updateWorkflowOutcome` (which sets it after a real workflow run).  
Will require running actual DBOS workflows (like `MetricsTest` does), or using `importWorkflow` once that populates `completed_at`.

---

## Things already done (DO NOT re-do)

- `WorkflowAggregateRow` — 5-field record (`group, count, minCreatedAt, maxQueueWaitMs, maxTotalLatencyMs`)
- `GetWorkflowAggregatesInput` — select flags, time bucket, completed/dequeued filters
- `GetWorkflowAggregatesRequest` — new body fields + backward-compat `toInput()` (no select flags → default `selectCount=true`)
- `GetWorkflowAggregatesResponse` — `WorkflowAggregateOutput` with nullable metric fields
- `StepAggregateRow`, `GetStepAggregatesInput` — new records
- `GetStepAggregatesRequest`, `GetStepAggregatesResponse` — new protocol classes
- `MessageType.GET_STEP_AGGREGATES` added
- `WorkflowDAO.getWorkflowAggregates` — rewritten: select flags, time bucketing, all new filters, new metrics
- `WorkflowDAO.getStepAggregates` — new method (queries `operation_outputs`)
- `SystemDatabase.getStepAggregates` — new delegation method
- `Conductor.handleGetStepAggregates` — new handler + wired into switch
- `SystemDatabaseTest` — 12 new tests (step aggregates fully covered; workflow aggregate new features covered except `completed_at`-dependent ones)
- `ConductorTest` — 3 new tests (`canGetWorkflowAggregatesWithSelectFields`, `canGetStepAggregates`, `canGetStepAggregatesThrows`)

---

## Quick test status once write-side reverted

| Test class | Expected |
|---|---|
| `SystemDatabaseTest.*Aggregate*` | Pass (except completed_at test — disable it) |
| `SystemDatabaseTest.*StepAgg*` | Pass |
| `ConductorTest.*Aggregate*` | Pass |
| `ConductorTest.*StepAgg*` | Pass |
| `MetricsTest` | Pass |
