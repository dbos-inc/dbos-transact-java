package dev.dbos.transact.workflow;

/**
 * Rows a rename moved, by table. {@code workflows} counts both the in-flight rows moved with the
 * queues, schedules and versions and the terminal ones moved afterwards in batches.
 */
public record ApplicationRowCounts(
    long queues, long schedules, long versions, long workflows, long steps) {}
