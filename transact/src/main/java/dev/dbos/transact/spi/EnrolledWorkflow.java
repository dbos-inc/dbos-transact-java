package dev.dbos.transact.spi;

/** A workflow enrolled in auto-upgrade, as {@code workflow_status} names it. */
public record EnrolledWorkflow(String className, String name) {}
