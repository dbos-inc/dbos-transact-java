package dev.dbos.transact.internal;

import dev.dbos.transact.conductor.ConductorSystemDatabase;
import dev.dbos.transact.database.SystemDatabase;
import dev.dbos.transact.suspend.RecordedRecv;
import dev.dbos.transact.suspend.SuspendEngine;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;

import org.jspecify.annotations.Nullable;

/** Suspend mode's view of a DBOS instance: its system database, and the recv it falls back to. */
public final class DBOSSuspendEngine implements SuspendEngine {
  private final SystemDatabase systemDatabase;
  private final @Nullable String appName;

  public DBOSSuspendEngine(SystemDatabase systemDatabase, @Nullable String appName) {
    this.systemDatabase = Objects.requireNonNull(systemDatabase);
    this.appName = appName;
  }

  @Override
  public ConductorSystemDatabase systemDatabase() {
    return systemDatabase.forConductor();
  }

  @Override
  public @Nullable String appName() {
    return appName;
  }

  @Override
  public Optional<RecordedRecv> recordedRecv(String workflowId, int stepId, String ownerXid) {
    return systemDatabase.recordedRecv(workflowId, stepId, ownerXid);
  }

  @Override
  public Instant recvDeadline(
      String workflowId, int timeoutStepId, Duration timeout, String ownerXid) {
    return systemDatabase.recvDeadline(workflowId, timeoutStepId, timeout, ownerXid);
  }

  @Override
  public Instant sleepEnd(String workflowId, int functionId, Duration duration, String ownerXid) {
    return systemDatabase.sleepEnd(workflowId, functionId, duration, ownerXid);
  }

  @Override
  public @Nullable Object recv(
      String workflowId,
      int stepId,
      int timeoutStepId,
      @Nullable String topic,
      Duration timeout,
      String ownerXid) {
    return systemDatabase.recv(workflowId, stepId, timeoutStepId, topic, timeout, ownerXid);
  }

  @Override
  public void checkOwner(Connection connection, String workflowId, String ownerXid)
      throws SQLException {
    systemDatabase.checkOwner(connection, workflowId, ownerXid);
  }

  @Override
  public String clockEpochMsSql() {
    return SystemDatabase.CLOCK_TIMESTAMP_EPOCH_MS;
  }

  @Override
  public boolean isCockroach() {
    return systemDatabase.isCockroach();
  }
}
