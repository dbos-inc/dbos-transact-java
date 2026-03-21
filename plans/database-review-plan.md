# Database Layer Code Review — Action Plan

Reviewed: `transact/src/main/java/dev/dbos/transact/database`

---

## Bugs

### 1. Unquoted schema in `getWorkflowSerialization`
**File:** `WorkflowDAO.java:349`
**Priority:** High

Every SQL statement in the codebase qualifies the schema as `"%s"` (double-quoted identifier). This one method uses `%s` without quotes:

```java
"SELECT serialization FROM %s.workflow_status WHERE workflow_uuid = ?"
    .formatted(this.schema);
```

Any schema name with uppercase letters, spaces, or other characters valid inside a quoted identifier will produce a SQL syntax error here. Fix: add quotes.

---

### 2. Unsafe cast on deserialized sleep output
**File:** `StepsDAO.java:311`
**Priority:** Medium

```java
endTime = ((Number) deserialized).longValue();
```

`SerializationUtil.deserializeValue` can return `null` if the stored output is null or corrupt. A null `deserialized` will throw a `NullPointerException` on the cast. The preceding check on line 305 only guards against `recordedOutput.output()` being null (the raw serialized string), not against the deserialized value itself being null. Add an explicit null check on `deserialized` before the cast.

---

## Performance

### 3. Row-by-row inserts in `importWorkflow`
**File:** `SystemDatabase.java:815–1009`
**Priority:** Medium

`importWorkflow` prepares one statement per table (good) but then executes a separate `stmt.executeUpdate()` per row — steps, events, event history, and streams each loop independently. For a large export with hundreds of steps this generates hundreds of round-trips. Use `addBatch()` / `executeBatch()` to send all rows for each table in one round-trip.

---

## Code Quality

### 4. Test-only methods in production classes
**Files:** `SystemDatabase.java:155`, `WorkflowDAO.java:43`, `NotificationsDAO.java:42`
**Priority:** Low

`speedUpPollingForTest()` is package-private on production classes and mutates polling intervals at runtime. If accidentally called in production it would spike database load. Options:
- Inject polling intervals via constructor so tests pass short values directly.
- Move override logic to test subclasses.

---

### 5. Hard-coded polling intervals
**Files:** `WorkflowDAO.java`, `NotificationsDAO.java`
**Priority:** Low

`dbPollingIntervalEventMs = 10000` and `getResultPollingIntervalMs = 1000` are hard-coded constants with no external configuration path. Expose via constructor or config object so they can be tuned without recompilation.

---

### 6. Oversized methods
**Priority:** Low

Three methods are large enough to make the logic hard to follow and test in isolation:

| Method | File | Approx. lines |
|--------|------|---------------|
| `recv()` | `NotificationsDAO.java` | ~160 |
| `listWorkflows()` | `WorkflowDAO.java` | ~161 |
| `importWorkflow()` | `SystemDatabase.java` | ~195 |

Each mixes query construction, transaction management, result mapping, and business logic. Extract sub-methods for each concern.

---

### 7. `SystemDatabase.java` size and responsibilities
**File:** `SystemDatabase.java` (~1017 lines)
**Priority:** Low

The class handles facade delegation, SQL construction, serialisation/deserialisation, retry logic, and export/import. SQL building and serialisation helpers could be extracted to reduce cognitive load and make the retry/coordination logic easier to follow.

---

## Design

### 8. Inconsistent schema quoting
**Files:** `WorkflowDAO.java:349` and throughout
**Priority:** Medium (overlaps with bug #1)

`WorkflowDAO:349` uses `%s` (unquoted). All other SQL across `WorkflowDAO`, `StepsDAO`, `QueuesDAO`, `NotificationsDAO`, and `SystemDatabase` uses `"%s"` (double-quoted). The inconsistency makes it easy to introduce new unquoted references. Establish a helper or convention to enforce consistent quoting.

---

### 9. Undocumented transaction isolation level choices
**Files:** `WorkflowDAO.java:63`, `QueuesDAO.java:50`
**Priority:** Low

`WorkflowDAO` uses `READ_COMMITTED` for workflow init; `QueuesDAO` uses `REPEATABLE_READ` for dequeue. The rationale is not documented. Add comments explaining why each level was chosen (e.g. why `REPEATABLE_READ` + `SKIP LOCKED` is required for correct queue concurrency control).

---

## Summary Table

| # | Issue | File | Priority |
|---|-------|------|----------|
| 1 | ~~Unquoted schema in `getWorkflowSerialization`~~ ✓ Fixed | `WorkflowDAO:349` | **High** |
| 2 | ~~Unsafe cast on deserialized sleep output~~ ✓ Fixed | `StepsDAO:311` | Medium |
| 3 | ~~Row-by-row inserts in `importWorkflow`~~ ✓ Fixed | `SystemDatabase:815–1009` | Medium |
| 4 | Inconsistent schema quoting | `WorkflowDAO:349` + others | Medium |
| 5 | Test methods in production classes | `SystemDatabase:155`, `WorkflowDAO:43`, `NotificationsDAO:42` | Low |
| 6 | Hard-coded polling intervals | `WorkflowDAO`, `NotificationsDAO` | Low |
| 7 | Oversized methods | `NotificationsDAO`, `WorkflowDAO`, `SystemDatabase` | Low |
| 8 | `SystemDatabase` size/responsibilities | `SystemDatabase.java` | Low |
| 9 | Undocumented isolation level choices | `WorkflowDAO:63`, `QueuesDAO:50` | Low |
